//! REP-2016 type hashes (`RIHS01_…`) for the Zenoh key-expression scheme.
//!
//! `rmw_zenoh` embeds a type hash in every topic/service key and liveliness
//! token; two entities match only if name **and** type **and** hash agree.
//! `ros2-client` does not yet compute REP-2016 hashes from IDL, so — per
//! `docs/decisions/0007-type-hash-interop-strategy.md` — we use a layered
//! strategy:
//!
//! * **Receivers** (subscriptions, queryables) build their key with
//!   [`WILDCARD`] in the hash slot, so they receive from any publisher/client
//!   regardless of hash.
//! * **Senders** look up a correct hash from the [`known_type_hash`] table for
//!   the common interop types; unknown types fall back to a
//!   wildcard/placeholder and send-direction interop with C++ peers may not
//!   match until full IDL hashing lands.
//!
//! The table values are taken from observed `rmw_zenoh` traffic / the design
//! examples and are covered by a test so drift is caught. They are additionally
//! cross-checked against the from-scratch REP-2016 computation in
//! [`super::type_description`] (which reproduces both hashes byte-exactly from
//! their field descriptions), so the table entries are now *verified* rather
//! than merely observed. Computing hashes for arbitrary types at code-gen time
//! (removing the table entirely for the send direction) is the remaining
//! `msggen` integration follow-up (ADR-0007).

/// Wildcard used in the type-hash slot of a *receiver's* key expression so it
/// matches publishers/clients of any hash. A single-chunk `*` matches exactly
/// one key-expression chunk (the hash), which is what we want here.
pub const WILDCARD: &str = "*";

/// Concrete placeholder hash used by a *sender* (publisher/client) when the
/// real REP-2016 hash is unknown. A `put` cannot target a wildcard key, so a
/// concrete value is required; this well-formed all-zero `RIHS01_` hash lets
/// two `ros2-client` peers match each other. Interop with C++ peers in the send
/// direction still needs the real hash (see [`known_type_hash`], ADR-0007).
pub const PLACEHOLDER_HASH: &str =
  "RIHS01_0000000000000000000000000000000000000000000000000000000000000000";

/// Look up the REP-2016 type hash for a DDS-form type name
/// (e.g. `std_msgs::msg::dds_::String_`).
///
/// Returns `None` for types not in the table; callers then fall back to a
/// wildcard/placeholder for the send direction.
pub fn known_type_hash(dds_type_name: &str) -> Option<&'static str> {
  Some(match dds_type_name {
    "std_msgs::msg::dds_::String_" => {
      "RIHS01_df668c740482bbd48fb39d76a70dfd4bd59db1288021743503259e948f6b1a18"
    }
    "example_interfaces::srv::dds_::AddTwoInts_" => {
      "RIHS01_e118de6bf5eeb66a2491b5bda11202e7b68f198d6f67922cf30364858239c81a"
    }
    _ => return None,
  })
}

/// The hash to place in a *sender's* (publisher/client) concrete key: the known
/// hash if we have it, otherwise [`PLACEHOLDER_HASH`] (documented limitation —
/// send-direction interop with C++ peers needs the real hash).
pub fn sender_hash(dds_type_name: &str) -> &str {
  known_type_hash(dds_type_name).unwrap_or(PLACEHOLDER_HASH)
}

// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn known_hashes_present() {
    assert_eq!(
      known_type_hash("std_msgs::msg::dds_::String_"),
      Some("RIHS01_df668c740482bbd48fb39d76a70dfd4bd59db1288021743503259e948f6b1a18")
    );
    assert_eq!(
      known_type_hash("example_interfaces::srv::dds_::AddTwoInts_"),
      Some("RIHS01_e118de6bf5eeb66a2491b5bda11202e7b68f198d6f67922cf30364858239c81a")
    );
  }

  #[test]
  fn unknown_sender_uses_concrete_placeholder() {
    assert_eq!(known_type_hash("pkg::msg::dds_::Nope_"), None);
    // A sender must use a concrete key, never the wildcard.
    assert_eq!(sender_hash("pkg::msg::dds_::Nope_"), PLACEHOLDER_HASH);
    assert_ne!(sender_hash("pkg::msg::dds_::Nope_"), WILDCARD);
    assert!(sender_hash("std_msgs::msg::dds_::String_").starts_with("RIHS01_"));
  }
}
