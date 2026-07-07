//! Zenoh middleware backend for `ros2-client` (cargo feature `zenoh`).
//!
//! This module tree implements the ROS 2 ↔ Zenoh mapping described in
//! `docs/zenoh_study/` and mirrors the official `rmw_zenoh` design:
//!
//! - topics → key expressions `<domain>/<name>/<type>/<type_hash>`
//! - discovery → liveliness tokens under `@ros2_lv/<domain>/**` + a graph cache
//! - every message carries an attachment `(seq, source_timestamp, source_gid)`
//! - services → Zenoh queryable / get
//! - GID → XXH3-128 of the entity's liveliness key
//!
//! The submodules are populated by the E2–E6 work items (see
//! `docs/zenoh_study/refactoring_plan.md`). They are declared here as they land
//! so the backend grows behind a single `#[cfg(feature = "zenoh")]` boundary.

// E2 — Zenoh wire primitives (keyexpr / attachment / type-hash / gid / CDR).
// pub(crate) mod keyexpr;
// pub(crate) mod attachment;
// pub(crate) mod type_hash;
// pub(crate) mod gid;
// pub(crate) mod cdr;

// E3 — Zenoh session / Context.
// pub(crate) mod session;

// E4 — Pub/Sub.
// pub(crate) mod pubsub;

// E5 — Discovery: liveliness tokens + graph cache.
// pub(crate) mod liveliness;
// pub(crate) mod graph_cache;

// E6 — Services over queryable / get.
// pub(crate) mod service;
