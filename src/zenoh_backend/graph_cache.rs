//! In-memory ROS 2 graph cache built from liveliness tokens (E5).
//!
//! A [`Context`](super::context::Context) declares a liveliness subscriber over
//! `@ros2_lv/<domain>/**`; each PUT/DELETE sample is fed to [`GraphCache`],
//! which parses the token key ([`parse_liveliness_key`]) and maintains the set
//! of live entities. Queries (entity counts, node names) read this set — the
//! Zenoh analogue of the DDS `ros_discovery_info` graph.
//!
//! The parsing/query logic is backend-neutral (no `zenoh` crate), so it is
//! unit-tested on any build.

use std::{collections::HashMap, sync::Mutex};

use super::keyexpr::{parse_liveliness_key, EntityKind, ParsedEntity};

/// A thread-safe cache of discovered ROS 2 entities, keyed by their liveliness
/// token key expression.
#[derive(Default)]
pub struct GraphCache {
  entities: Mutex<HashMap<String, ParsedEntity>>,
}

impl GraphCache {
  /// Record an entity from a liveliness-token PUT. Non-token keys are ignored.
  pub fn apply_put(&self, key: &str) {
    if let Some(entity) = parse_liveliness_key(key) {
      self.entities.lock().unwrap().insert(key.to_owned(), entity);
    }
  }

  /// Remove an entity on a liveliness-token DELETE.
  pub fn apply_delete(&self, key: &str) {
    self.entities.lock().unwrap().remove(key);
  }

  fn count_kind_on_topic(&self, kind: EntityKind, topic: &str) -> usize {
    self
      .entities
      .lock()
      .unwrap()
      .values()
      .filter(|e| e.kind == kind && e.topic_name.as_deref() == Some(topic))
      .count()
  }

  /// Number of discovered publishers on a topic (by fully-qualified name).
  pub fn publisher_count(&self, topic: &str) -> usize {
    self.count_kind_on_topic(EntityKind::Publisher, topic)
  }

  /// Number of discovered subscriptions on a topic.
  pub fn subscription_count(&self, topic: &str) -> usize {
    self.count_kind_on_topic(EntityKind::Subscription, topic)
  }

  /// Number of discovered service servers on a service name.
  pub fn service_server_count(&self, service: &str) -> usize {
    self.count_kind_on_topic(EntityKind::ServiceServer, service)
  }

  /// Fully-qualified names of all discovered nodes, sorted and de-duplicated.
  pub fn node_names(&self) -> Vec<String> {
    let guard = self.entities.lock().unwrap();
    let mut names: Vec<String> = guard
      .values()
      .filter(|e| e.kind == EntityKind::Node)
      .map(|e| join_fqn(&e.namespace, &e.node_name))
      .collect();
    names.sort();
    names.dedup();
    names
  }

  /// Total number of cached entities (all kinds). Mainly for diagnostics/tests.
  pub fn len(&self) -> usize {
    self.entities.lock().unwrap().len()
  }

  /// Whether the cache is empty.
  pub fn is_empty(&self) -> bool {
    self.entities.lock().unwrap().is_empty()
  }
}

fn join_fqn(namespace: &str, name: &str) -> String {
  if namespace.is_empty() || namespace == "/" {
    format!("/{name}")
  } else {
    format!("{namespace}/{name}")
  }
}

// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
  use super::*;
  use crate::zenoh_backend::keyexpr::{
    entity_liveliness_keyexpr, node_liveliness_keyexpr, EntityIds,
  };

  fn ids(entity_id: u64) -> EntityIds<'static> {
    EntityIds {
      session_id: "aac3178e146ba6f1fc6e6a4085e77f21",
      node_id: 0,
      entity_id,
      enclave: "",
      namespace: "",
      node_name: "talker",
    }
  }

  #[test]
  fn tracks_entities_and_counts() {
    let cache = GraphCache::default();
    let node_key = node_liveliness_keyexpr(0, &ids(0));
    let pub_key = entity_liveliness_keyexpr(
      0,
      &ids(1),
      EntityKind::Publisher,
      "/chatter",
      "std_msgs::msg::dds_::String_",
      "RIHS01_x",
      "::,7:,:,:,,",
    );

    cache.apply_put(&node_key);
    cache.apply_put(&pub_key);
    // A non-token key is ignored.
    cache.apply_put("0/chatter/std_msgs::msg::dds_::String_/hash");

    assert_eq!(cache.publisher_count("/chatter"), 1);
    assert_eq!(cache.subscription_count("/chatter"), 0);
    assert_eq!(cache.node_names(), vec!["/talker".to_string()]);
    assert_eq!(cache.len(), 2);

    // DELETE removes the publisher.
    cache.apply_delete(&pub_key);
    assert_eq!(cache.publisher_count("/chatter"), 0);
    assert_eq!(cache.len(), 1);
  }
}
