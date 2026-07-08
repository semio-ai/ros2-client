//! Zenoh `Node` and `Topic` (E4).
//!
//! A minimal node surface over a shared [`Context`] session: it resolves ROS
//! names to key expressions and creates [`Publisher`]/[`Subscription`]
//! entities. Discovery (liveliness tokens, graph), services, and actions land
//! in E5/E6.

use std::sync::atomic::{AtomicU64, Ordering};

use serde::{de::DeserializeOwned, Serialize};
use zenoh::{liveliness::LivelinessToken, Wait};

use super::{
  context::Context,
  gid, keyexpr,
  pubsub::{Publisher, Subscription},
  qos_encoding, type_hash,
};
use crate::{
  names::{MessageTypeName, Name, NodeName},
  qos::QosProfile,
};

/// Options for creating a [`Node`] on the Zenoh backend.
///
/// Currently a placeholder (parity with the DDS `NodeOptions`);
/// rosout/parameter toggles arrive with E8/E9.
#[derive(Clone, Debug, Default)]
pub struct NodeOptions {}

impl NodeOptions {
  /// Default node options.
  pub fn new() -> Self {
    Self {}
  }
}

/// A handle to a ROS 2 topic: its fully-qualified name, DDS-form type name, and
/// QoS. Produced by [`Node::create_topic`].
#[derive(Clone, Debug)]
pub struct Topic {
  fully_qualified_name: String,
  dds_type_name: String,
  qos: QosProfile,
}

impl Topic {
  /// The fully-qualified ROS topic name (e.g. `/chatter`).
  pub fn name(&self) -> &str {
    &self.fully_qualified_name
  }

  /// The DDS-form type name (e.g. `std_msgs::msg::dds_::String_`).
  pub fn type_name(&self) -> &str {
    &self.dds_type_name
  }

  /// The topic's QoS profile.
  pub fn qos(&self) -> &QosProfile {
    &self.qos
  }
}

/// A ROS 2 node backed by a shared Zenoh session.
pub struct Node {
  context: Context,
  node_name: NodeName,
  node_id: u64,
  zid: String,
  next_entity_id: AtomicU64,
  // Kept alive so the node stays discoverable; dropped => token undeclared.
  _node_token: Option<LivelinessToken>,
}

impl Node {
  pub(crate) fn new(
    context: Context,
    node_name: NodeName,
    node_id: u64,
    _options: NodeOptions,
  ) -> Self {
    let zid = context.session().zid().to_string();
    // Declare the node (NN) liveliness token so peers discover this node.
    let ids = keyexpr::EntityIds {
      session_id: &zid,
      node_id,
      entity_id: node_id,
      enclave: "",
      namespace: node_name.namespace(),
      node_name: node_name.base_name(),
    };
    let node_key = keyexpr::node_liveliness_keyexpr(context.domain_id(), &ids);
    let node_token = declare_liveliness(&context, node_key);
    Self {
      context,
      node_name,
      node_id,
      zid,
      next_entity_id: AtomicU64::new(0),
      _node_token: node_token,
    }
  }

  /// The node's name.
  pub fn name(&self) -> &NodeName {
    &self.node_name
  }

  /// The node's fully-qualified name (e.g. `/robot1/talker`).
  pub fn fully_qualified_name(&self) -> String {
    self.node_name.fully_qualified_name()
  }

  /// Create a [`Topic`] handle. `name` is resolved against the node namespace
  /// if relative.
  pub fn create_topic(&self, name: &Name, type_name: MessageTypeName, qos: &QosProfile) -> Topic {
    Topic {
      fully_qualified_name: resolve_fqn(name, &self.node_name),
      dds_type_name: type_name.dds_msg_type(),
      qos: qos.clone(),
    }
  }

  /// Create a publisher for `topic`. `qos` overrides the topic QoS if given.
  pub fn create_publisher<M: Serialize>(
    &self,
    topic: &Topic,
    qos: Option<QosProfile>,
  ) -> zenoh::Result<Publisher<M>> {
    let qos = qos.unwrap_or_else(|| topic.qos.clone());
    let domain = self.context.domain_id();
    let sender_hash = type_hash::sender_hash(&topic.dds_type_name);
    // A publisher `put`s on a concrete key (real hash if known, else placeholder).
    let key = keyexpr::topic_keyexpr(
      domain,
      &topic.fully_qualified_name,
      &topic.dds_type_name,
      sender_hash,
    );
    let zenoh_publisher = self.context.session().declare_publisher(key).wait()?;

    let entity_id = self.next_entity_id.fetch_add(1, Ordering::Relaxed);
    let liveliness_key = self.entity_liveliness_key(
      entity_id,
      keyexpr::EntityKind::Publisher,
      topic,
      sender_hash,
      &qos,
    );
    let source_gid = gid::gid_from_liveliness_key(&liveliness_key);
    let token = declare_liveliness(&self.context, liveliness_key);

    Ok(Publisher::new(zenoh_publisher, source_gid, token))
  }

  /// Create a subscription for `topic`. `qos` overrides the topic QoS if given.
  pub fn create_subscription<M: DeserializeOwned>(
    &self,
    topic: &Topic,
    qos: Option<QosProfile>,
  ) -> zenoh::Result<Subscription<M>> {
    let qos = qos.unwrap_or_else(|| topic.qos.clone());
    let domain = self.context.domain_id();
    // A subscription listens with a wildcard type-hash to receive from any
    // publisher regardless of its hash (liberal receive, ADR-0007).
    let key = keyexpr::topic_keyexpr(
      domain,
      &topic.fully_qualified_name,
      &topic.dds_type_name,
      type_hash::WILDCARD,
    );
    let zenoh_subscriber = self.context.session().declare_subscriber(key).wait()?;

    // The liveliness token, unlike the data key, is concrete (real-or-placeholder
    // hash) — it describes this entity for discovery.
    let entity_id = self.next_entity_id.fetch_add(1, Ordering::Relaxed);
    let sub_hash = type_hash::sender_hash(&topic.dds_type_name);
    let liveliness_key = self.entity_liveliness_key(
      entity_id,
      keyexpr::EntityKind::Subscription,
      topic,
      sub_hash,
      &qos,
    );
    let token = declare_liveliness(&self.context, liveliness_key);

    Ok(Subscription::new(zenoh_subscriber, token))
  }

  /// Build the liveliness key for an entity of this node, including the compact
  /// QoS encoding.
  fn entity_liveliness_key(
    &self,
    entity_id: u64,
    kind: keyexpr::EntityKind,
    topic: &Topic,
    hash: &str,
    qos: &QosProfile,
  ) -> String {
    let ids = keyexpr::EntityIds {
      session_id: &self.zid,
      node_id: self.node_id,
      entity_id,
      enclave: "",
      namespace: self.node_name.namespace(),
      node_name: self.node_name.base_name(),
    };
    keyexpr::entity_liveliness_keyexpr(
      self.context.domain_id(),
      &ids,
      kind,
      &topic.fully_qualified_name,
      &topic.dds_type_name,
      hash,
      &qos_encoding::encode_qos(qos),
    )
  }
}

/// Declare a liveliness token on `key`, best-effort: a failure logs a warning
/// and returns `None` (the entity still works; it just isn't discoverable).
fn declare_liveliness(context: &Context, key: String) -> Option<LivelinessToken> {
  match context
    .session()
    .liveliness()
    .declare_token(key.clone())
    .wait()
  {
    Ok(token) => Some(token),
    Err(e) => {
      log::warn!("failed to declare liveliness token for {key}: {e}");
      None
    }
  }
}

/// Resolve a topic [`Name`] to its fully-qualified form against a node name.
/// Absolute names are used as-is; relative names are prefixed with the node
/// namespace.
fn resolve_fqn(name: &Name, node: &NodeName) -> String {
  if name.is_absolute() {
    format!("{name}")
  } else {
    let ns = node.namespace();
    if ns == "/" {
      format!("/{name}")
    } else {
      format!("{ns}/{name}")
    }
  }
}

// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
  use std::time::{Duration, Instant};

  use zenoh::{Config, Wait};

  use super::{
    keyexpr::{graph_cache_keyexpr, parse_liveliness_key, EntityKind},
    *,
  };
  use crate::{Context, ContextOptions};

  #[test]
  fn fqn_resolution() {
    let node = NodeName::new("/robot1", "talker").unwrap();
    let rel = Name::new("", "chatter").unwrap();
    assert_eq!(resolve_fqn(&rel, &node), "/robot1/chatter");

    let root_node = NodeName::new("/", "talker").unwrap();
    assert_eq!(resolve_fqn(&rel, &root_node), "/chatter");

    let abs = Name::new("/", "chatter").unwrap();
    assert_eq!(resolve_fqn(&abs, &node), "/chatter");
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

  // A publisher created on one context is discovered by another via its
  // liveliness tokens (node NN + publisher MP), parsed back correctly.
  #[test]
  fn discovers_entities_via_liveliness() {
    let a_port = 17515;
    let b_port = 17516;

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

    let graph_key = graph_cache_keyexpr(ctx_b.domain_id());
    let deadline = Instant::now() + Duration::from_secs(15);
    let mut saw_node = false;
    let mut saw_publisher = false;

    while Instant::now() < deadline && !(saw_node && saw_publisher) {
      let replies = ctx_b.session().liveliness().get(&graph_key).wait().unwrap();
      while let Ok(reply) = replies.recv() {
        if let Ok(sample) = reply.result() {
          if let Some(e) = parse_liveliness_key(sample.key_expr().as_str()) {
            match e.kind {
              EntityKind::Node if e.node_name == "talker" => saw_node = true,
              EntityKind::Publisher
                if e.topic_name.as_deref() == Some("/chatter") && e.node_name == "talker" =>
              {
                saw_publisher = true
              }
              _ => {}
            }
          }
        }
      }
      if saw_node && saw_publisher {
        break;
      }
      std::thread::sleep(Duration::from_millis(100));
    }

    assert!(saw_node, "did not discover the talker node token");
    assert!(
      saw_publisher,
      "did not discover the /chatter publisher token"
    );
  }
}
