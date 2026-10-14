//! Request and foreground delivery ownership through confirmed completion.
//! Detached tasks and persistent registrations require independent owners.

use crate::{commit_staged_read_cursors, wait_owner, AppState, DaemonError, DispatchOutcome};
use chanvoy_core::CoreError;
use tokio::io::{AsyncWrite, AsyncWriteExt};

/// Local handler failure only: no new serialized control response or code.
pub(super) fn admission_error(error: wait_owner::AdmissionError) -> DaemonError {
    match error {
        wait_owner::AdmissionError::Closed => CoreError::RuntimeAdmissionClosed.into(),
        wait_owner::AdmissionError::Exhausted => CoreError::RuntimeOwnershipExhausted.into(),
        wait_owner::AdmissionError::WrongInstance | wait_owner::AdmissionError::OwnerChanged => {
            std::io::Error::other("runtime request completion owner changed").into()
        }
    }
}

pub(super) async fn finish<W: AsyncWrite + Unpin>(
    writer: &mut W,
    state: &AppState,
    dispatched: DispatchOutcome,
    permit: wait_owner::RequestPermit,
) -> Result<(), DaemonError> {
    let response = dispatched.response;
    writer
        .write_all(serde_json::to_string(&response)?.as_bytes())
        .await?;
    writer.write_all(b"\n").await?;
    writer.flush().await?;
    if response.error.is_none()
        && (dispatched.staged_poll_ack.is_some() || dispatched.staged_attention.is_some())
    {
        commit_staged_read_cursors(
            state,
            dispatched.staged_poll_ack,
            dispatched.staged_attention,
        )
        .await?;
    }
    if let Some(consume) = dispatched.staged_fanin_consume {
        if response.error.is_none() {
            state
                .fanin_replay
                .consume(&consume.channel_id, &consume.post_id)?;
        }
    }
    if let Some(hold) = dispatched.fanin_hold {
        hold.release();
    }
    // Any earlier error or cancellation loses this handle and keeps Busy.
    // It does not establish inverse cleanup or detached-work completion.
    permit.confirm_done().map_err(admission_error)
}

/// Input/refusal responses have no staged work, but still require full framing.
pub(super) async fn finish_response<W: AsyncWrite + Unpin>(
    writer: &mut W,
    response: &chanvoy_core::JsonRpcResponse,
    permit: wait_owner::RequestPermit,
) -> Result<(), DaemonError> {
    writer
        .write_all(serde_json::to_string(response)?.as_bytes())
        .await?;
    writer.write_all(b"\n").await?;
    writer.flush().await?;
    permit.confirm_done().map_err(admission_error)
}

/// Call only after producer completion, selected ACKs, fixed drain and terminal
/// framing. EOF/cancellation alone must never reach this completion frontier.
pub(super) async fn finish_held<W: AsyncWrite + Unpin>(
    writer: &mut W,
    request: wait_owner::RequestPermit,
    delivery: wait_owner::RetainedOwner,
) -> Result<(), DaemonError> {
    writer.flush().await?;
    delivery.confirm_done().map_err(admission_error)?;
    request.confirm_done().map_err(admission_error)
}

pub(super) fn acknowledge(
    written: tokio::sync::oneshot::Sender<Result<(), String>>,
    result: std::io::Result<()>,
) -> std::io::Result<()> {
    let ack = written.send(result.as_ref().map(|_| ()).map_err(ToString::to_string));
    result?;
    ack.map_err(|_| {
        std::io::Error::new(
            std::io::ErrorKind::BrokenPipe,
            "stream write acknowledgement unavailable",
        )
    })
}

#[cfg(test)]
mod tests;
