# 10. Raw action server and raw publisher

Date: 2026-07-29

## Status

Accepted

## Context

[ADR 0006](0006-services-over-queryable-get.md)'s raw dynamic service server
(`RawServer`, 0.10.2) lets a caller back a *service* whose request/response
types are known only at runtime — a schema-directed codec drives the wire as
CDR bytes. Actions have the same consumer: a bridge that synthesises a ROS 2
surface from a runtime type description (methods discovered at runtime, message
types built from their signatures) cannot instantiate the typed
`ActionServer<A>`/`ActionTypes`, whose goal/result/feedback are compile-time
`Message` types. Topics were also uncovered: both backends could *subscribe*
raw (`take_raw`) and the Zenoh `Publisher` had `publish_raw`, but the DDS
backend had no raw publish path at all, and neither backend could assemble an
action's five endpoints without the crate-private type-name mangling
(`ActionTypeName::dds_action_service` / `dds_action_topic`).

## Decision

Two additions, mirrored on both backends:

- **`Node::create_raw_publisher` → `RawPublisher`** — the topic-plane sibling
  of `create_raw_server`: publishes pre-serialized full standalone CDR messages
  (encapsulation header included) on a topic created with `create_topic`. On
  DDS it reuses the byte pass-through serializer adapter the raw service
  endpoints ride (the sample payload has no service framing, so the
  pass-through *is* the topic serialization); on Zenoh it carries
  `Publisher::publish_raw` without a compile-time message type.
- **`Node::create_raw_action_server` → `RawActionServer`** — assembles the five
  action endpoints with the same names, types, and QoS as the typed
  `create_action_server`, but raw where the action's user-defined types appear:
  send_goal and get_result on `RawServer`s, feedback on a `RawPublisher`. The
  fixed `action_msgs` halves stay typed and are handled inside the crate — the
  cancel service and status topic wholesale, and the fixed parts of the raw
  messages (the SendGoal response, the GetResult request) so a caller only
  ever touches bytes for the parts whose types it alone knows. The goal
  lifecycle (acceptance, states, result caching) stays with the caller:
  `RawActionServer` is transport, not policy.

Raw bytes are full standalone CDR messages, little-endian, matching
`RawServer`'s contract; on DDS the raw endpoints speak the Enhanced service
mapping only, like the raw services.

## Consequences

- A runtime-typed action server interoperates with ordinary typed clients —
  covered by an in-process Zenoh loopback test (`ActionClient` ↔
  `RawActionServer` over the full goal→feedback→cancel→result lifecycle) and
  byte-level round-trip tests of the fixed message halves on DDS.
- The service `wrappers` module (the pass-through adapters) widens from
  `pub(super)` to `pub(crate)`; no public API is removed, so this ships as a
  minor version (0.11.0).
- Like every raw endpoint, the raw action endpoints do not interoperate with
  the Basic/Cyclone DDS service mappings; Cyclone-mapped peers remain future
  work.
