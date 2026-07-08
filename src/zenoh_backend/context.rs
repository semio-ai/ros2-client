//! Zenoh [`Context`] — one `zenoh::Session` per ROS 2 context (E3).
//!
//! Mirrors the `rmw_zenoh` model where a ROS 2 context maps to a single Zenoh
//! session shared by all of its entities (see
//! `docs/decisions/0009-zenoh-router-and-config.md`). This is the seed of E3:
//! it opens the session and holds the domain id (used as the key-expression
//! prefix). Node/pub/sub/service creation is added by E4–E6.

use std::sync::{
  atomic::{AtomicU64, Ordering},
  Arc,
};

use zenoh::{pubsub::Subscriber, sample::SampleKind, Config, Session, Wait};

use super::{
  graph_cache::GraphCache,
  keyexpr,
  node::{Node, NodeOptions},
};
use crate::names::NodeName;

/// Builder for configuring a [`Context`] on the Zenoh backend.
pub struct ContextOptions {
  domain_id: u16,
  config: Option<Config>,
}

impl ContextOptions {
  /// New options with defaults (domain id 0, default Zenoh peer config).
  pub fn new() -> Self {
    Self {
      domain_id: 0,
      config: None,
    }
  }

  /// Set the ROS domain id. On the Zenoh backend this is not a transport
  /// setting; it becomes the leading component of every key expression, which
  /// is what isolates domains (see `docs/zenoh_study/research/rmw_zenoh.md`
  /// §2).
  pub fn domain_id(mut self, domain_id: u16) -> Self {
    self.domain_id = domain_id;
    self
  }

  /// Provide an explicit Zenoh [`Config`] (e.g. loaded from a JSON5 file).
  /// If unset, a default peer configuration is used.
  pub fn zenoh_config(mut self, config: Config) -> Self {
    self.config = Some(config);
    self
  }
}

impl Default for ContextOptions {
  fn default() -> Self {
    Self::new()
  }
}

/// A ROS 2 [`Context`] backed by a single Zenoh session.
///
/// Cloning is cheap (a shared handle), matching the DDS backend's `Context`.
#[derive(Clone)]
pub struct Context {
  inner: Arc<ContextInner>,
}

struct ContextInner {
  session: Session,
  domain_id: u16,
  next_node_id: AtomicU64,
  graph_cache: Arc<GraphCache>,
  // Kept alive to keep the graph cache updating; dropped => subscriber undeclared.
  _liveliness_subscriber: Subscriber<()>,
}

impl Context {
  /// Open a new context with default settings (domain id 0, default peer
  /// config). Requires a reachable Zenoh router in the default configuration
  /// (see ADR-0009); opening still succeeds without one and connects later.
  pub fn new() -> zenoh::Result<Context> {
    Self::with_options(ContextOptions::new())
  }

  /// Open a new context with the given options.
  ///
  /// When no explicit [`ContextOptions::zenoh_config`] is given, the session
  /// config is taken from the environment (see [`config_from_env`]): a JSON5
  /// file named by `ZENOH_SESSION_CONFIG_URI`, or the built-in peer default,
  /// with `ZENOH_CONFIG_OVERRIDE` applied on top. This matches `rmw_zenoh`.
  pub fn with_options(opt: ContextOptions) -> zenoh::Result<Context> {
    let config = match opt.config {
      Some(config) => config,
      None => config_from_env()?,
    };
    let session = zenoh::open(config).wait()?;

    // Build the ROS graph cache from liveliness tokens: subscribe over the whole
    // domain admin space with history so existing entities are delivered too.
    let graph_cache = Arc::new(GraphCache::default());
    let cache_for_cb = graph_cache.clone();
    let liveliness_subscriber = session
      .liveliness()
      .declare_subscriber(keyexpr::graph_cache_keyexpr(opt.domain_id))
      .history(true)
      .callback(move |sample| {
        let key = sample.key_expr().as_str();
        match sample.kind() {
          SampleKind::Put => cache_for_cb.apply_put(key),
          SampleKind::Delete => cache_for_cb.apply_delete(key),
        }
      })
      .wait()?;

    Ok(Context {
      inner: Arc::new(ContextInner {
        session,
        domain_id: opt.domain_id,
        next_node_id: AtomicU64::new(0),
        graph_cache,
        _liveliness_subscriber: liveliness_subscriber,
      }),
    })
  }

  /// Number of publishers currently discovered on `topic` (fully-qualified).
  pub fn publisher_count(&self, topic: &str) -> usize {
    self.inner.graph_cache.publisher_count(topic)
  }

  /// Number of subscriptions currently discovered on `topic`.
  pub fn subscription_count(&self, topic: &str) -> usize {
    self.inner.graph_cache.subscription_count(topic)
  }

  /// Fully-qualified names of all currently discovered nodes.
  pub fn node_names(&self) -> Vec<String> {
    self.inner.graph_cache.node_names()
  }

  /// Create a new ROS 2 [`Node`] on this context's session.
  pub fn new_node(&self, name: NodeName, options: NodeOptions) -> Node {
    let node_id = self.inner.next_node_id.fetch_add(1, Ordering::Relaxed);
    Node::new(self.clone(), name, node_id, options)
  }

  /// The ROS domain id (key-expression prefix) for this context.
  pub fn domain_id(&self) -> u16 {
    self.inner.domain_id
  }

  /// The underlying Zenoh session, shared by all entities in this context.
  /// Used by pub/sub, discovery, and services (E4–E6).
  #[allow(dead_code)] // consumed by later work items
  pub(crate) fn session(&self) -> &Session {
    &self.inner.session
  }
}

/// Build a Zenoh [`Config`] from the environment, mirroring `rmw_zenoh`:
///
/// * `ZENOH_SESSION_CONFIG_URI` — if set (and non-empty), a path to a JSON5
///   Zenoh config file loaded as the base config. Otherwise [`default_config`]
///   is the base.
/// * `ZENOH_CONFIG_OVERRIDE` — if set, a `;`-separated list of `key=value`
///   JSON5 assignments applied on top of the base (e.g.
///   `mode="client";connect/endpoints=["tcp/localhost:7447"]`).
///
/// A malformed override, or a config file that fails to load, is returned as an
/// error rather than silently ignored.
pub(crate) fn config_from_env() -> zenoh::Result<Config> {
  let mut config = match std::env::var("ZENOH_SESSION_CONFIG_URI") {
    Ok(uri) if !uri.trim().is_empty() => {
      Config::from_file(uri.trim()).map_err(|e| -> zenoh::Error {
        format!("failed to load Zenoh config from ZENOH_SESSION_CONFIG_URI={uri:?}: {e}").into()
      })?
    }
    _ => default_config(),
  };
  if let Ok(overrides) = std::env::var("ZENOH_CONFIG_OVERRIDE") {
    apply_config_overrides(&mut config, &overrides)?;
  }
  Ok(config)
}

/// Apply a `;`-separated list of `key=value` JSON5 assignments to `config`.
/// Empty entries are ignored; an entry without `=` is an error.
fn apply_config_overrides(config: &mut Config, overrides: &str) -> zenoh::Result<()> {
  for entry in overrides
    .split(';')
    .map(str::trim)
    .filter(|s| !s.is_empty())
  {
    let (key, value) = entry.split_once('=').ok_or_else(|| -> zenoh::Error {
      format!("ZENOH_CONFIG_OVERRIDE entry is not `key=value`: {entry:?}").into()
    })?;
    config
      .insert_json5(key.trim(), value.trim())
      .map_err(|e| -> zenoh::Error {
        format!("ZENOH_CONFIG_OVERRIDE failed for key {:?}: {e}", key.trim()).into()
      })?;
  }
  Ok(())
}

/// Default Zenoh configuration for a ROS 2 peer.
///
/// Peer mode, listening on an IPv4 loopback port and with multicast scouting
/// disabled — the same shape as `rmw_zenoh`'s default session config, which
/// also keeps the crate working in IPv6-less / restricted-network environments
/// (Zenoh's own default listens on `tcp/[::]:0`, which fails where IPv6 is
/// unavailable, e.g. many CI runners).
///
/// For a full `rmw_zenoh`-style deployment (connect to `tcp/localhost:7447`,
/// gossip scouting, timestamping) point `ZENOH_SESSION_CONFIG_URI` at the
/// desired JSON5 file or supply `ZENOH_CONFIG_OVERRIDE` (see
/// [`config_from_env`]).
fn default_config() -> Config {
  let mut config = Config::default();
  // These keys are stable Zenoh config paths; a failure here is a programming
  // error, so surface it loudly rather than silently opening a wrong config.
  config
    .insert_json5("mode", "\"peer\"")
    .expect("valid zenoh config key: mode");
  config
    .insert_json5("listen/endpoints", "[\"tcp/127.0.0.1:0\"]")
    .expect("valid zenoh config key: listen/endpoints");
  config
    .insert_json5("scouting/multicast/enabled", "false")
    .expect("valid zenoh config key: scouting/multicast/enabled");
  config
}

#[cfg(test)]
mod tests {
  use std::time::{Duration, Instant};

  use super::*;
  use crate::{MessageTypeName, Name, NodeName, NodeOptions, Publisher, QosProfile};

  #[test]
  fn config_overrides_apply_and_validate() {
    // A well-formed override list applies cleanly...
    let mut config = default_config();
    apply_config_overrides(
      &mut config,
      "mode=\"client\"; connect/endpoints=[\"tcp/localhost:7447\"]",
    )
    .expect("valid overrides should apply");
    assert_eq!(config.mode(), &Some(zenoh::config::WhatAmI::Client));

    // ...empty entries are ignored...
    apply_config_overrides(&mut default_config(), " ; ; ").expect("empty entries ignored");

    // ...and an entry without `=` is rejected.
    assert!(apply_config_overrides(&mut default_config(), "no_equals_here").is_err());
  }

  #[test]
  fn opens_a_session_and_keeps_domain_id() {
    let ctx = Context::with_options(ContextOptions::new().domain_id(7))
      .expect("opening a Zenoh session should succeed (peer mode, no router needed to open)");
    assert_eq!(ctx.domain_id(), 7);
    // Cloning shares the same session handle.
    let ctx2 = ctx.clone();
    assert_eq!(ctx2.domain_id(), 7);
  }

  fn make_config(listen_port: u16, connect_port: Option<u16>) -> Config {
    let mut c = Config::default();
    c.insert_json5("mode", "\"peer\"").unwrap();
    c.insert_json5("scouting/multicast/enabled", "false")
      .unwrap();
    c.insert_json5(
      "listen/endpoints",
      &format!("[\"tcp/127.0.0.1:{listen_port}\"]"),
    )
    .unwrap();
    if let Some(p) = connect_port {
      c.insert_json5("connect/endpoints", &format!("[\"tcp/127.0.0.1:{p}\"]"))
        .unwrap();
    }
    c
  }

  #[test]
  fn graph_cache_discovers_remote_publisher_and_node() {
    let a_port = 17517;
    let b_port = 17518;
    let ctx_a =
      Context::with_options(ContextOptions::new().zenoh_config(make_config(a_port, None))).unwrap();
    let ctx_b =
      Context::with_options(ContextOptions::new().zenoh_config(make_config(b_port, Some(a_port))))
        .unwrap();

    let node_a = ctx_a.new_node(NodeName::new("/", "talker").unwrap(), NodeOptions::new());
    let topic = node_a.create_topic(
      &Name::new("/", "chatter").unwrap(),
      MessageTypeName::new("std_msgs", "String"),
      &QosProfile::publisher_default(),
    );
    let _publisher: Publisher<String> = node_a.create_publisher(&topic, None).unwrap();

    let deadline = Instant::now() + Duration::from_secs(15);
    while Instant::now() < deadline && ctx_b.publisher_count("/chatter") == 0 {
      std::thread::sleep(Duration::from_millis(100));
    }

    assert_eq!(ctx_b.publisher_count("/chatter"), 1);
    assert!(ctx_b.node_names().contains(&"/talker".to_string()));
  }
}
