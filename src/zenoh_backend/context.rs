//! Zenoh [`Context`] — one `zenoh::Session` per ROS 2 context (E3).
//!
//! Mirrors the `rmw_zenoh` model where a ROS 2 context maps to a single Zenoh
//! session shared by all of its entities (see
//! `docs/decisions/0009-zenoh-router-and-config.md`). This is the seed of E3:
//! it opens the session and holds the domain id (used as the key-expression
//! prefix). Node/pub/sub/service creation is added by E4–E6.

use std::sync::Arc;

use zenoh::{Config, Session, Wait};

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
}

impl Context {
  /// Open a new context with default settings (domain id 0, default peer
  /// config). Requires a reachable Zenoh router in the default configuration
  /// (see ADR-0009); opening still succeeds without one and connects later.
  pub fn new() -> zenoh::Result<Context> {
    Self::with_options(ContextOptions::new())
  }

  /// Open a new context with the given options.
  pub fn with_options(opt: ContextOptions) -> zenoh::Result<Context> {
    let config = opt.config.unwrap_or_else(default_config);
    let session = zenoh::open(config).wait()?;
    Ok(Context {
      inner: Arc::new(ContextInner {
        session,
        domain_id: opt.domain_id,
      }),
    })
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

/// Default Zenoh configuration for a ROS 2 peer.
///
/// Peer mode, listening on an IPv4 loopback port and with multicast scouting
/// disabled — the same shape as `rmw_zenoh`'s default session config, which
/// also keeps the crate working in IPv6-less / restricted-network environments
/// (Zenoh's own default listens on `tcp/[::]:0`, which fails where IPv6 is
/// unavailable, e.g. many CI runners).
///
/// TODO(E3): load the full `rmw_zenoh` JSON5 profile (connect
/// `tcp/localhost:7447`, gossip on, timestamping on) and honour
/// `ZENOH_SESSION_CONFIG_URI` / `ZENOH_CONFIG_OVERRIDE`.
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
  use super::*;

  #[test]
  fn opens_a_session_and_keeps_domain_id() {
    let ctx = Context::with_options(ContextOptions::new().domain_id(7))
      .expect("opening a Zenoh session should succeed (peer mode, no router needed to open)");
    assert_eq!(ctx.domain_id(), 7);
    // Cloning shares the same session handle.
    let ctx2 = ctx.clone();
    assert_eq!(ctx2.domain_id(), 7);
  }
}
