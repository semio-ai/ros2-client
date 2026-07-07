# 7. Type-hash (REP-2016) interop strategy

- Status: accepted
- Date: 2026-07-07
- Relates to: `docs/zenoh_study/feature_map.md` §11

## Context

`rmw_zenoh` key expressions and liveliness tokens include a REP-2016 type hash
(`RIHS01_<64 hex>`), a SHA-256 over a canonical JSON description of the message
type. Two Zenoh entities match only if **name AND type AND hash** agree.
`ros2-client` is a generic serde client and computes **no** type hash today;
computing a correct RIHS01 requires the full IDL type description (all nested
fields), which the crate does not generally have at hand.

Without the correct hash:
- a `ros2-client` **subscriber/queryable** with a concrete-hash key won't receive
  from a C++ publisher that uses the real hash;
- a `ros2-client` **publisher** with a wrong/placeholder hash won't be received by
  a C++ subscriber that subscribes to the real-hash key.

`ros2-client`↔`ros2-client` works with any consistently-used placeholder.

## Decision

A layered strategy, MVP = (a) + (b):

- **(a) Liberal receive.** Subscribers and queryables declare their key with a
  wildcard (`*`/`**`) in the type-hash position, so they receive from **any**
  publisher/client regardless of hash. This fixes the receive direction against
  real ROS 2 immediately and is legal because Zenoh subscriber keys may contain
  wildcards.
- **(b) Known-types hash table.** For the send direction to C++ peers, ship a
  small table of correct RIHS01 hashes for the common interop types
  (`std_msgs/String`, `example_interfaces/srv/AddTwoInts`,
  `action_tutorials_interfaces/action/Fibonacci`, `geometry_msgs/msg/Twist`, …),
  and expose an API to supply a hash explicitly per topic/type. When a type is
  unknown, fall back to a wildcard-or-placeholder and log that send-direction
  interop with C++ peers may not match.
- **(c) Future: compute RIHS01 from parsed `.msg`/`.srv`/`.action` IDL** in
  `msggen`. Tracked as a follow-up issue, not required for MVP.

## Consequences

- **Pro:** interop works today for the receive direction universally and for the
  send direction on the common types, without building a full IDL hasher.
- **Con:** send-direction interop with C++ peers is limited to known types until
  (c) lands; wildcard-receive slightly weakens type-safety at the transport layer
  (mitigated by the type name still being in the key and CDR deserialization
  failing loudly on a true mismatch).
- The hash table must be kept in sync with upstream type definitions; a test
  compares a few entries against values observed from a live `rmw_zenoh` peer.
