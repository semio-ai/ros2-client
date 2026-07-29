//! A **raw** action server: the dynamic counterpart of
//! [`ActionServer`](super::ActionServer), for actions whose goal, result, and
//! feedback types are known only at runtime.
//!
//! A ROS 2 action decomposes into three services and two topics. Three of the
//! five endpoints embed the action's user-defined types, so here they are
//! byte-level ([`RawServer`], [`RawPublisher`]); the other two carry fixed
//! `action_msgs` types and stay typed — their handling (and the fixed halves of
//! the raw messages) is done in here, so a caller only ever touches bytes for
//! the parts whose types it alone knows:
//!
//! - **send_goal** (raw service): request = `goal_id: uint8[16]` + Goal;
//!   response is fixed (`accepted: bool` + `stamp`) and encoded here by
//!   [`respond_goal`](RawActionServer::respond_goal).
//! - **cancel_goal** (typed service): fixed `action_msgs/CancelGoal`; surfaced
//!   as [`GoalInfo`] in /
//!   [`CancelGoalResponse`](action_msgs::CancelGoalResponse) out.
//! - **get_result** (raw service): request is fixed (`goal_id`) and decoded
//!   here; the response — `status: int8` + Result — is caller-built bytes.
//! - **feedback** (raw topic): `goal_id: uint8[16]` + Feedback, caller-built.
//! - **status** (typed topic): fixed `action_msgs/GoalStatusArray`.
//!
//! Raw bytes are always **full standalone CDR messages** (4-byte encapsulation
//! header + little-endian body), the same contract as [`RawServer`]. The goal
//! lifecycle (acceptance, states, result caching) is the caller's: this type is
//! transport, not policy — where the typed [`AsyncActionServer`] tracks goals,
//! a runtime-typed caller has its own book keyed by whatever it decodes from
//! the goals.
//!
//! [`AsyncActionServer`]: super::AsyncActionServer

use bytes::BufMut;
#[allow(unused_imports)]
use log::{debug, error, info, warn};
use rustdds::{
  dds::{ReadError, ReadResult, WriteResult},
  serialization::{self, deserialize_from_cdr_with_rep_id},
  *,
};

use crate::{
  action_msgs::{self, GoalId, GoalInfo},
  builtin_interfaces,
  message::Message,
  names::Name,
  pubsub::{Publisher, RawPublisher},
  service::{request_id::RmwRequestId, AService, RawServer, Server},
};
use super::{GetResultRequest, SendGoalResponse};

/// The five endpoints of one runtime-typed action, created with
/// [`Node::create_raw_action_server`](crate::Node::create_raw_action_server).
/// See the [module docs](self) for which endpoint speaks bytes and which speaks
/// `action_msgs` types.
pub struct RawActionServer {
  goal_server: RawServer,
  cancel_server: Server<AService<action_msgs::CancelGoalRequest, action_msgs::CancelGoalResponse>>,
  result_server: RawServer,
  feedback_publisher: RawPublisher,
  status_publisher: Publisher<action_msgs::GoalStatusArray>,
  action_name: Name,
}

impl RawActionServer {
  pub(crate) fn new(
    goal_server: RawServer,
    cancel_server: Server<
      AService<action_msgs::CancelGoalRequest, action_msgs::CancelGoalResponse>,
    >,
    result_server: RawServer,
    feedback_publisher: RawPublisher,
    status_publisher: Publisher<action_msgs::GoalStatusArray>,
    action_name: Name,
  ) -> Self {
    Self {
      goal_server,
      cancel_server,
      result_server,
      feedback_publisher,
      status_publisher,
      action_name,
    }
  }

  pub fn name(&self) -> &Name {
    &self.action_name
  }

  /// Await the next goal request, as a full standalone CDR `SendGoal` request
  /// message: `goal_id: uint8[16]` first, the goal fields after it. The caller
  /// decodes it with its runtime codec.
  pub async fn receive_goal_request(&self) -> ReadResult<(RmwRequestId, Vec<u8>)> {
    self.goal_server.async_receive_request().await
  }

  /// Answer a goal request: the fixed `SendGoal` response (`accepted` +
  /// `stamp`), encoded here. The caller keeps `stamp` — the status array and
  /// the cancel policy's accepted-at-or-before matching use it.
  pub fn respond_goal(
    &self,
    req_id: RmwRequestId,
    accepted: bool,
    stamp: builtin_interfaces::Time,
  ) -> WriteResult<(), ()> {
    let response = to_standalone_cdr(&SendGoalResponse { accepted, stamp })?;
    self.goal_server.send_response(req_id, &response)
  }

  /// Await the next cancel request, surfaced as the [`GoalInfo`] selecting the
  /// goals to cancel (per the `action_msgs/CancelGoal` policy: a zero goal id
  /// and/or zero stamp widen the selection).
  pub async fn receive_cancel_request(&self) -> ReadResult<(RmwRequestId, GoalInfo)> {
    let (req_id, request) = self.cancel_server.async_receive_request().await?;
    Ok((req_id, request.goal_info))
  }

  /// Answer a cancel request with the goals that transition to `CANCELING`.
  pub fn respond_cancel(
    &self,
    req_id: RmwRequestId,
    response: action_msgs::CancelGoalResponse,
  ) -> WriteResult<(), ()> {
    self.cancel_server.send_response(req_id, response)
  }

  /// Await the next result request, decoded here (its one field is the fixed
  /// `goal_id`).
  pub async fn receive_result_request(&self) -> ReadResult<(RmwRequestId, GoalId)> {
    let (req_id, bytes) = self.result_server.async_receive_request().await?;
    let (request, _consumed) =
      deserialize_from_cdr_with_rep_id::<GetResultRequest>(strip_header(&bytes)?, rep_id(&bytes)?)?;
    Ok((req_id, request.goal_id))
  }

  /// Answer a result request with a caller-built full standalone CDR
  /// `GetResult` response message: `status: int8` first (the terminal
  /// `action_msgs/GoalStatus` value), the result fields after it.
  pub fn respond_result_raw(
    &self,
    req_id: RmwRequestId,
    cdr_response: &[u8],
  ) -> WriteResult<(), ()> {
    self.result_server.send_response(req_id, cdr_response)
  }

  /// Publish a caller-built full standalone CDR feedback message: `goal_id:
  /// uint8[16]` first, the feedback fields after it.
  pub fn publish_feedback_raw(&self, cdr_message: &[u8]) -> WriteResult<(), ()> {
    self.feedback_publisher.publish(cdr_message)
  }

  /// Publish the status of every known goal.
  pub fn publish_statuses(
    &self,
    statuses: action_msgs::GoalStatusArray,
  ) -> WriteResult<(), action_msgs::GoalStatusArray> {
    self.status_publisher.publish(statuses)
  }
}

/// Serialize a fixed-type message to a full standalone CDR message (4-byte
/// `CDR_LE` encapsulation header + body).
fn to_standalone_cdr<M: Message>(message: &M) -> WriteResult<Vec<u8>, ()> {
  let mut writer = bytes::BytesMut::new().writer();
  serialization::to_writer_with_rep_id(&mut writer, message, RepresentationIdentifier::CDR_LE)?;
  let body = writer.into_inner();
  let mut out = Vec::with_capacity(4 + body.len());
  out.extend_from_slice(&[0x00, 0x01, 0x00, 0x00]); // CDR_LE, options 0
  out.extend_from_slice(&body);
  Ok(out)
}

/// The representation identifier a full standalone CDR message's encapsulation
/// header declares.
fn rep_id(message: &[u8]) -> ReadResult<RepresentationIdentifier> {
  match message.get(0..2) {
    Some([0x00, 0x00]) => Ok(RepresentationIdentifier::CDR_BE),
    Some([0x00, 0x01]) => Ok(RepresentationIdentifier::CDR_LE),
    other => {
      read_error_deserialization!("unsupported CDR encapsulation {other:?} in a raw action message")
    }
  }
}

/// The body of a full standalone CDR message (after the 4-byte encapsulation
/// header).
fn strip_header(message: &[u8]) -> ReadResult<&[u8]> {
  if message.len() < 4 {
    read_error_deserialization!("a raw action message shorter than its CDR encapsulation header")
  } else {
    Ok(&message[4..])
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  /// The fixed-type halves of the raw action protocol round-trip through the
  /// standalone-CDR helpers: what `respond_goal` encodes decodes as the typed
  /// `SendGoalResponse`, and a typed `GetResultRequest` decodes the way
  /// `receive_result_request` does.
  #[test]
  fn standalone_cdr_round_trips_the_fixed_action_types() {
    let response = SendGoalResponse {
      accepted: true,
      stamp: builtin_interfaces::Time::now(),
    };
    let bytes = to_standalone_cdr(&response).expect("encodes");
    assert_eq!(&bytes[0..4], &[0x00, 0x01, 0x00, 0x00], "CDR_LE header");
    let (decoded, _) = deserialize_from_cdr_with_rep_id::<SendGoalResponse>(
      strip_header(&bytes).unwrap(),
      rep_id(&bytes).unwrap(),
    )
    .expect("decodes");
    assert!(decoded.accepted);
    assert_eq!(decoded.stamp, response.stamp);

    let request = GetResultRequest {
      goal_id: GoalId::new_random(),
    };
    let bytes = to_standalone_cdr(&request).expect("encodes");
    let (decoded, _) = deserialize_from_cdr_with_rep_id::<GetResultRequest>(
      strip_header(&bytes).unwrap(),
      rep_id(&bytes).unwrap(),
    )
    .expect("decodes");
    assert_eq!(decoded.goal_id, request.goal_id);
  }

  /// Malformed encapsulations are rejected, not misparsed.
  #[test]
  fn malformed_encapsulations_are_rejected() {
    assert!(rep_id(&[0xFF, 0xFF, 0x00, 0x00]).is_err(), "unknown rep id");
    assert!(
      strip_header(&[0x00, 0x01]).is_err(),
      "shorter than a header"
    );
  }
}
