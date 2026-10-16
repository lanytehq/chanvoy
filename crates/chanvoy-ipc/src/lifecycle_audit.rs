//! Independent lifecycle effects and error-preserving session cleanup.

use super::request_frontier::{RequestTransport, SendResult, SessionTransport};
use chanvoy_core::request_ownership::RequestAdmission;
use chanvoy_core::CoreError;
use std::future::Future;

#[derive(Debug, PartialEq, Eq)]
pub(super) enum Selection {
    Closed,
    Confirmed,
}

// The callback constructs the event only after admission. A dropped or failed
// send never confirms the independently retained owner.
async fn selected<F, Fut>(
    admission: &dyn RequestAdmission,
    send: F,
) -> Result<Selection, Box<dyn std::error::Error + Send + Sync>>
where
    F: FnOnce() -> Fut,
    Fut: Future<Output = SendResult>,
{
    let owner = match admission.reserve_request() {
        Ok(owner) => owner,
        Err(CoreError::RuntimeAdmissionClosed) => return Ok(Selection::Closed),
        Err(error) => return Err(error.into()),
    };
    send().await?;
    owner.confirm_done()?;
    Ok(Selection::Confirmed)
}

pub(super) async fn emit<T: RequestTransport>(
    admission: &dyn RequestAdmission,
    tx: &T,
    peer_id: &str,
    action: &str,
) -> Result<Selection, Box<dyn std::error::Error + Send + Sync>> {
    selected(admission, || {
        super::emit_audit(tx, peer_id, action, None, "info")
    })
    .await
}

pub(super) async fn finish_session<T: SessionTransport>(
    admission: &dyn RequestAdmission,
    tx: &T,
    peer_id: &str,
    forwarder: tokio::task::JoinHandle<()>,
    outcome: SendResult,
) -> SendResult {
    let disconnected = emit(admission, tx, peer_id, "chat.peer_disconnected")
        .await
        .map(|_| ());
    // No early propagation: even an earlier error must collect the actual old
    // task before transport shutdown or a subsequent session replacement.
    let collected = super::subscription_owner::abort_and_collect(forwarder).await;
    let shutdown = tx.shutdown().await;
    outcome.and(disconnected).and(collected).and(shutdown)
}

/// Connected-audit failure skips request processing but still tears down.
pub(super) async fn session<T: SessionTransport>(
    admission: &dyn RequestAdmission,
    tx: &T,
    peer_id: &str,
    forwarder: tokio::task::JoinHandle<()>,
    serve: impl Future<Output = SendResult>,
) -> SendResult {
    let outcome = match emit(admission, tx, peer_id, "chat.peer_connected").await {
        Ok(_) => serve.await,
        Err(error) => Err(error),
    };
    finish_session(admission, tx, peer_id, forwarder, outcome).await
}

#[cfg(test)]
mod tests;
