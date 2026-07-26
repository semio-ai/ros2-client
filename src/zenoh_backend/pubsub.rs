//! Zenoh `Publisher` and `Subscription` (E4).
//!
//! A publisher `put`s a CDR payload (see [`super::cdr`]) with an
//! [`AttachmentData`] carrying `(sequence, source_timestamp, source_gid)` — the
//! `rmw_zenoh` message shape. A subscription declares a Zenoh subscriber with a
//! wildcard type-hash (liberal receive, ADR-0007) and yields decoded messages
//! plus their [`MessageInfo`].

use std::{
  marker::PhantomData,
  sync::atomic::{AtomicI64, Ordering},
  time::{SystemTime, UNIX_EPOCH},
};

use serde::{de::DeserializeOwned, Serialize};
use zenoh::{
  handlers::FifoChannelHandler,
  liveliness::LivelinessToken,
  pubsub::{Publisher as ZenohPublisher, Subscriber},
  sample::Sample,
  Wait,
};

use super::{attachment::AttachmentData, cdr};

/// Metadata about a received message, extracted from its Zenoh attachment.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct MessageInfo {
  source_timestamp_nanos: i64,
  sequence_number: i64,
  source_gid: [u8; 16],
}

impl MessageInfo {
  /// Source timestamp (ns since UNIX epoch) set by the publisher, or 0 if the
  /// message carried no ROS attachment.
  pub fn source_timestamp_nanos(&self) -> i64 {
    self.source_timestamp_nanos
  }

  /// Per-publisher sequence number of this message.
  pub fn sequence_number(&self) -> i64 {
    self.sequence_number
  }

  /// 16-byte GID of the publishing entity.
  pub fn source_gid(&self) -> [u8; 16] {
    self.source_gid
  }
}

/// Failure to publish a message.
#[derive(Debug)]
pub enum PublishError {
  /// CDR serialization of the message failed.
  Cdr(cdr::CdrError),
  /// The Zenoh `put` failed.
  Zenoh(zenoh::Error),
}

impl std::fmt::Display for PublishError {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    match self {
      PublishError::Cdr(e) => write!(f, "publish: {e}"),
      PublishError::Zenoh(e) => write!(f, "publish: zenoh error: {e}"),
    }
  }
}
impl std::error::Error for PublishError {}

/// Failure to receive/decode a message.
#[derive(Debug)]
pub enum TakeError {
  /// CDR deserialization failed.
  Cdr(cdr::CdrError),
  /// The message attachment was malformed.
  Attachment,
  /// The subscriber channel was closed.
  Closed,
}

impl std::fmt::Display for TakeError {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    match self {
      TakeError::Cdr(e) => write!(f, "take: {e}"),
      TakeError::Attachment => write!(f, "take: malformed attachment"),
      TakeError::Closed => write!(f, "take: subscriber closed"),
    }
  }
}
impl std::error::Error for TakeError {}

fn now_nanos() -> i64 {
  SystemTime::now()
    .duration_since(UNIX_EPOCH)
    .map(|d| d.as_nanos() as i64)
    .unwrap_or(0)
}

/// A ROS 2 publisher over Zenoh.
pub struct Publisher<M> {
  zenoh_publisher: ZenohPublisher<'static>,
  seq: AtomicI64,
  source_gid: [u8; 16],
  // Kept alive so the entity stays discoverable; dropped => token undeclared.
  _liveliness_token: Option<LivelinessToken>,
  phantom: PhantomData<fn() -> M>,
}

impl<M: Serialize> Publisher<M> {
  pub(crate) fn new(
    zenoh_publisher: ZenohPublisher<'static>,
    source_gid: [u8; 16],
    liveliness_token: Option<LivelinessToken>,
  ) -> Self {
    Self {
      zenoh_publisher,
      seq: AtomicI64::new(0),
      source_gid,
      _liveliness_token: liveliness_token,
      phantom: PhantomData,
    }
  }

  fn encode(&self, msg: &M) -> Result<(Vec<u8>, zenoh::bytes::ZBytes), PublishError> {
    let payload = cdr::to_cdr(msg).map_err(PublishError::Cdr)?;
    let sequence_number = self.seq.fetch_add(1, Ordering::Relaxed) + 1; // start at 1
    let attachment = AttachmentData {
      sequence_number,
      source_timestamp: now_nanos(),
      source_gid: self.source_gid,
    }
    .to_zbytes();
    Ok((payload, attachment))
  }

  /// Publish a message (async).
  pub async fn async_publish(&self, msg: M) -> Result<(), PublishError> {
    let (payload, attachment) = self.encode(&msg)?;
    self
      .zenoh_publisher
      .put(payload)
      .attachment(attachment)
      .await
      .map_err(PublishError::Zenoh)
  }

  /// Publish a message (blocking).
  pub fn publish(&self, msg: M) -> Result<(), PublishError> {
    let (payload, attachment) = self.encode(&msg)?;
    self
      .zenoh_publisher
      .put(payload)
      .attachment(attachment)
      .wait()
      .map_err(PublishError::Zenoh)
  }

  /// This publisher's 16-byte source GID.
  pub fn gid(&self) -> [u8; 16] {
    self.source_gid
  }
}

impl<M> Publisher<M> {
  /// Publish pre-encoded CDR bytes (which must include the 4-byte encapsulation
  /// header) on this publisher's topic, with the standard rmw_zenoh attachment.
  /// Lets a runtime-typed codec drive the wire without a compile-time message
  /// type; does not touch `M`.
  pub async fn publish_raw(&self, cdr_bytes: &[u8]) -> Result<(), PublishError> {
    let sequence_number = self.seq.fetch_add(1, Ordering::Relaxed) + 1;
    let attachment = AttachmentData {
      sequence_number,
      source_timestamp: now_nanos(),
      source_gid: self.source_gid,
    }
    .to_zbytes();
    self
      .zenoh_publisher
      .put(cdr_bytes.to_vec())
      .attachment(attachment)
      .await
      .map_err(PublishError::Zenoh)
  }
}

/// A ROS 2 subscription over Zenoh.
pub struct Subscription<M> {
  zenoh_subscriber: Subscriber<FifoChannelHandler<Sample>>,
  // Kept alive so the entity stays discoverable; dropped => token undeclared.
  _liveliness_token: Option<LivelinessToken>,
  phantom: PhantomData<fn() -> M>,
}

/// Parse the rmw_zenoh attachment `(seq, ts, gid)` from a sample, if present.
fn message_info_from(sample: &Sample) -> Result<MessageInfo, TakeError> {
  match sample.attachment() {
    Some(zbytes) => {
      let a = AttachmentData::from_zbytes(zbytes).map_err(|_| TakeError::Attachment)?;
      Ok(MessageInfo {
        source_timestamp_nanos: a.source_timestamp,
        sequence_number: a.sequence_number,
        source_gid: a.source_gid,
      })
    }
    None => Ok(MessageInfo::default()),
  }
}

impl<M: DeserializeOwned> Subscription<M> {
  pub(crate) fn new(
    zenoh_subscriber: Subscriber<FifoChannelHandler<Sample>>,
    liveliness_token: Option<LivelinessToken>,
  ) -> Self {
    Self {
      zenoh_subscriber,
      _liveliness_token: liveliness_token,
      phantom: PhantomData,
    }
  }

  fn decode(sample: &Sample) -> Result<(M, MessageInfo), TakeError> {
    let msg = cdr::from_cdr::<M>(&sample.payload().to_bytes()).map_err(TakeError::Cdr)?;
    Ok((msg, message_info_from(sample)?))
  }

  /// Await the next message and its metadata.
  pub async fn async_take(&self) -> Result<(M, MessageInfo), TakeError> {
    let sample = self
      .zenoh_subscriber
      .recv_async()
      .await
      .map_err(|_| TakeError::Closed)?;
    Self::decode(&sample)
  }

  /// Take a message if one is immediately available (non-blocking).
  pub fn try_take(&self) -> Result<Option<(M, MessageInfo)>, TakeError> {
    match self.zenoh_subscriber.try_recv() {
      Ok(Some(sample)) => Self::decode(&sample).map(Some),
      Ok(None) => Ok(None),
      Err(_) => Err(TakeError::Closed),
    }
  }
}

impl<M> Subscription<M> {
  /// Await the next message as raw CDR bytes (with the 4-byte encapsulation
  /// header) plus its metadata, for a runtime-typed codec instead of a
  /// compile-time message type; does not touch `M`.
  pub async fn take_raw(&self) -> Result<(Vec<u8>, MessageInfo), TakeError> {
    let sample = self
      .zenoh_subscriber
      .recv_async()
      .await
      .map_err(|_| TakeError::Closed)?;
    Ok((
      sample.payload().to_bytes().to_vec(),
      message_info_from(&sample)?,
    ))
  }
}

// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
  use std::time::{Duration, Instant};

  use zenoh::Config;

  use super::{Publisher, Subscription};
  use crate::{Context, ContextOptions, MessageTypeName, Name, NodeName, NodeOptions, QosProfile};

  // Build a peer config on IPv4 loopback with multicast off. `listen`/`connect`
  // pin explicit ports so two in-process peers connect directly — no router
  // (matches Tier B in docs/zenoh_study/test_plan.md).
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
  fn pub_sub_roundtrip_in_process() {
    // Distinct fixed ports (CI runs zenoh tests with --test-threads=1).
    let sub_port = 17513;
    let pub_port = 17514;

    let sub_ctx =
      Context::with_options(ContextOptions::new().zenoh_config(make_config(sub_port, None)))
        .expect("open subscriber context");
    let pub_ctx = Context::with_options(
      ContextOptions::new().zenoh_config(make_config(pub_port, Some(sub_port))),
    )
    .expect("open publisher context");

    let sub_node = sub_ctx.new_node(NodeName::new("/", "test_sub").unwrap(), NodeOptions::new());
    let pub_node = pub_ctx.new_node(NodeName::new("/", "test_pub").unwrap(), NodeOptions::new());

    let make_topic = |n: &crate::Node| {
      n.create_topic(
        &Name::new("/", "chatter").unwrap(),
        MessageTypeName::new("std_msgs", "String"),
        &QosProfile::default(),
      )
    };
    let sub: Subscription<String> = sub_node
      .create_subscription(&make_topic(&sub_node), None)
      .expect("create subscription");
    let publisher: Publisher<String> = pub_node
      .create_publisher(&make_topic(&pub_node), None)
      .expect("create publisher");

    // Publish repeatedly until the peers have connected and a sample arrives.
    let deadline = Instant::now() + Duration::from_secs(15);
    let mut got = None;
    while Instant::now() < deadline {
      publisher
        .publish("hello zenoh!".to_string())
        .expect("publish");
      if let Some(m) = sub.try_take().expect("try_take") {
        got = Some(m);
        break;
      }
      std::thread::sleep(Duration::from_millis(100));
    }

    let (msg, info) = got.expect("no message received within timeout");
    assert_eq!(msg, "hello zenoh!");
    assert!(info.sequence_number() >= 1);
    assert_ne!(info.source_gid(), [0u8; 16]);
  }

  // Raw pub/sub: `publish_raw` puts pre-encoded CDR bytes and `take_raw` returns
  // them unchanged (with the attachment), letting a runtime-typed codec drive
  // the wire. Zenoh needs a multi-thread runtime for its async API.
  #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
  async fn raw_pub_sub_roundtrip() {
    let sub_port = 17515;
    let pub_port = 17516;

    let sub_ctx =
      Context::with_options(ContextOptions::new().zenoh_config(make_config(sub_port, None)))
        .expect("open subscriber context");
    let pub_ctx = Context::with_options(
      ContextOptions::new().zenoh_config(make_config(pub_port, Some(sub_port))),
    )
    .expect("open publisher context");

    let sub_node = sub_ctx.new_node(NodeName::new("/", "raw_sub").unwrap(), NodeOptions::new());
    let pub_node = pub_ctx.new_node(NodeName::new("/", "raw_pub").unwrap(), NodeOptions::new());

    let make_topic = |n: &crate::Node| {
      n.create_topic(
        &Name::new("/", "raw_chatter").unwrap(),
        MessageTypeName::new("std_msgs", "String"),
        &QosProfile::default(),
      )
    };
    // `M` is unused by the raw path; a unit placeholder is enough.
    let sub: Subscription<()> = sub_node
      .create_subscription(&make_topic(&sub_node), None)
      .expect("create subscription");
    let publisher: Publisher<()> = pub_node
      .create_publisher(&make_topic(&pub_node), None)
      .expect("create publisher");

    // A CDR_LE payload: the 4-byte encapsulation header plus an opaque body.
    let payload: Vec<u8> = vec![0x00, 0x01, 0x00, 0x00, 0xDE, 0xAD, 0xBE, 0xEF];

    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
      publisher.publish_raw(&payload).await.expect("publish_raw");
      if let Ok(Ok((bytes, info))) =
        tokio::time::timeout(Duration::from_millis(200), sub.take_raw()).await
      {
        assert_eq!(bytes, payload, "raw bytes must survive the wire unchanged");
        assert!(info.sequence_number() >= 1);
        return;
      }
      assert!(Instant::now() < deadline, "no raw message within timeout");
    }
  }
}
