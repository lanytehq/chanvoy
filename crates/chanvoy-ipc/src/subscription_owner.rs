//! Persistent IPC registrations and independent forwarded output.

use super::*;
use chanvoy_core::request_ownership::{RegistrationCompletion, RequestCompletion};
use std::collections::hash_map::Entry;
use tokio::sync::{broadcast, Mutex};

pub(super) struct SubscriptionEntry {
    pub(super) filter: IpcSubscriptionFilter,
    pub(super) owner: Box<dyn RegistrationCompletion>,
}

pub(super) fn insert(
    subscriptions: &mut HashMap<String, SubscriptionEntry>,
    id: String,
    filter: IpcSubscriptionFilter,
    request: &dyn RequestCompletion,
) -> SendResult {
    match subscriptions.entry(id) {
        Entry::Occupied(_) => Err(std::io::Error::new(
            std::io::ErrorKind::AlreadyExists,
            "subscription id already registered",
        )
        .into()),
        Entry::Vacant(entry) => {
            let owner = request.reserve_registration()?;
            entry.insert(SubscriptionEntry { filter, owner });
            Ok(())
        }
    }
}

pub(super) fn remove(
    subscriptions: &mut HashMap<String, SubscriptionEntry>,
    id: &str,
) -> Result<bool, CoreError> {
    let Some(SubscriptionEntry { filter, owner }) = subscriptions.remove(id) else {
        return Ok(false);
    };
    drop(filter);
    owner.confirm_done()?;
    Ok(true)
}

#[derive(Clone)]
pub(super) struct Forwarding {
    pub(super) subscriptions: Arc<Mutex<HashMap<String, SubscriptionEntry>>>,
    pub(super) admission: Arc<dyn RequestAdmission>,
    pub(super) identity_drift: Arc<AtomicBool>,
}

impl Forwarding {
    pub(super) async fn event<T: RequestTransport>(
        &self,
        event: &DaemonEvent,
        tx: &T,
    ) -> SendResult {
        if self.identity_drift.load(Ordering::Relaxed) {
            return Ok(());
        }
        // Preserve selection exclusion with unsubscribe. This is data locking,
        // not the daemon's admission/phase mutex.
        let subscriptions = self.subscriptions.lock().await;
        for (id, entry) in subscriptions.iter() {
            if !event_matches_ipc_filter(event, &entry.filter) {
                continue;
            }
            let owner = self.admission.reserve_request()?;
            let notification = if let DaemonEventPayloadInner::Gap(gap) = &event.payload {
                Some(ChatFrame::SubscriptionGap {
                    subscription_id: id.clone(),
                    expected_seq: gap.missed_from_seq,
                    next_seq: gap.missed_to_seq,
                    reason: "overflow".into(),
                    message: Some(format!(
                        "missed {} events",
                        gap.missed_to_seq.saturating_sub(gap.missed_from_seq)
                    )),
                })
            } else {
                daemon_event_to_chat_notification(event, id)
            };
            if let Some(notification) = notification {
                let payload = serde_json::to_vec(&notification)?;
                tx.send(CHANNEL_260, &payload).await?;
            }
            // An unmappable event has no output; that known no-op may finish.
            // Serialization, output failure or cancellation never reaches here.
            owner.confirm_done()?;
        }
        Ok(())
    }

    pub(super) async fn run<T: RequestTransport>(
        self,
        mut events: broadcast::Receiver<Arc<DaemonEvent>>,
        tx: T,
        cancel: CancellationToken,
    ) {
        loop {
            tokio::select! {
                received = events.recv() => {
                    match received {
                        Ok(event) => if self.event(event.as_ref(), &tx).await.is_err() { return; },
                        Err(_) => return,
                    }
                }
                _ = cancel.cancelled() => return,
            }
        }
    }
}

/// This observes actual task termination before the caller may reconnect.
/// It has no registration/effect handle and cannot confirm their ownership.
pub(super) async fn abort_and_collect(handle: tokio::task::JoinHandle<()>) -> SendResult {
    handle.abort();
    match handle.await {
        Ok(()) => Ok(()),
        Err(error) if error.is_cancelled() => Ok(()),
        Err(error) => Err(error.into()),
    }
}
