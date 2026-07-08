//! Zenoh `Node` and `Topic` (E4).
//!
//! A minimal node surface over a shared [`Context`] session: it resolves ROS
//! names to key expressions and creates [`Publisher`]/[`Subscription`]
//! entities. Discovery (liveliness tokens, graph), services, and actions land
//! in E5/E6.

use std::sync::atomic::{AtomicU64, Ordering};

use serde::{de::DeserializeOwned, Serialize};
use zenoh::Wait;

use super::{
  context::Context,
  gid, keyexpr,
  pubsub::{Publisher, Subscription},
  type_hash,
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
}

impl Node {
  pub(crate) fn new(
    context: Context,
    node_name: NodeName,
    node_id: u64,
    _options: NodeOptions,
  ) -> Self {
    let zid = context.session().zid().to_string();
    Self {
      context,
      node_name,
      node_id,
      zid,
      next_entity_id: AtomicU64::new(0),
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
    let _qos = qos.unwrap_or_else(|| topic.qos.clone());
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
    let source_gid = self.entity_gid(
      entity_id,
      keyexpr::EntityKind::Publisher,
      topic,
      sender_hash,
    );
    Ok(Publisher::new(zenoh_publisher, source_gid))
  }

  /// Create a subscription for `topic`. `qos` overrides the topic QoS if given.
  pub fn create_subscription<M: DeserializeOwned>(
    &self,
    topic: &Topic,
    qos: Option<QosProfile>,
  ) -> zenoh::Result<Subscription<M>> {
    let _qos = qos.unwrap_or_else(|| topic.qos.clone());
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
    let _entity_id = self.next_entity_id.fetch_add(1, Ordering::Relaxed);
    Ok(Subscription::new(zenoh_subscriber))
  }

  /// Compute the 16-byte GID of an entity from its liveliness key. The compact
  /// QoS component is empty for now; E5 fills it in once QoS encoding lands.
  fn entity_gid(
    &self,
    entity_id: u64,
    kind: keyexpr::EntityKind,
    topic: &Topic,
    hash: &str,
  ) -> [u8; 16] {
    let ids = keyexpr::EntityIds {
      session_id: &self.zid,
      node_id: self.node_id,
      entity_id,
      enclave: "",
      namespace: self.node_name.namespace(),
      node_name: self.node_name.base_name(),
    };
    let liveliness_key = keyexpr::entity_liveliness_keyexpr(
      self.context.domain_id(),
      &ids,
      kind,
      &topic.fully_qualified_name,
      &topic.dds_type_name,
      hash,
      "",
    );
    gid::gid_from_liveliness_key(&liveliness_key)
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
  use super::*;

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
}
