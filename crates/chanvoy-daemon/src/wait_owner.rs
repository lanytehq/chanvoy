//! In-memory single-waiter registry for one profile daemon.
//!
//! Keyed only by canonical channel id. Not persisted and not host-global.
//! A future fan-in waiter will need multi-key acquisition; this cut
//! admits exactly one key per wait.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use chanvoy_core::{
    can_install_replacement, decide_acquire, new_wait_id, now_unix_millis, should_release,
    CoreError, WaitAcquireDecision, WaitAcquireIntent, WaitSlotView, REPLACE_CLEANUP_BUDGET_SECS,
};
use tokio::sync::Notify;
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;

pub struct WaitOwnerRegistry {
    inner: Mutex<Inner>,
}

struct Inner {
    slots: HashMap<String, LiveSlot>,
    next_generation: u64,
}

struct LiveSlot {
    view: WaitSlotView,
    cancel: CancellationToken,
    cleanup_acked: Arc<AtomicBool>,
    cleanup_notify: Arc<Notify>,
    replaced_by: Arc<Mutex<Option<String>>>,
}

pub struct WaitLease {
    registry: Arc<WaitOwnerRegistry>,
    pub channel_id: String,
    pub wait_id: String,
    pub generation: u64,
    pub replaced_wait_id: Option<String>,
    pub cancel: CancellationToken,
    replaced_by: Arc<Mutex<Option<String>>>,
    cleanup_acked: Arc<AtomicBool>,
    cleanup_notify: Arc<Notify>,
}

pub struct WaitGuard {
    registry: Arc<WaitOwnerRegistry>,
    channel_id: String,
    generation: u64,
    cleanup_acked: Arc<AtomicBool>,
    cleanup_notify: Arc<Notify>,
}

impl Default for WaitOwnerRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl WaitOwnerRegistry {
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(Inner {
                slots: HashMap::new(),
                next_generation: 1,
            }),
        }
    }

    pub async fn acquire(
        self: &Arc<Self>,
        channel_id: &str,
        team: &str,
        channel: &str,
        replace_wait_id: Option<&str>,
        remaining: Duration,
    ) -> Result<WaitLease, CoreError> {
        let intent = match replace_wait_id {
            Some(id) => WaitAcquireIntent::Replace {
                wait_id: id.to_string(),
            },
            None => WaitAcquireIntent::Default,
        };
        enum Prepared {
            Ready(WaitLease),
            Replacing {
                old_wait_id: String,
                old_generation: u64,
                new_wait_id: String,
                new_generation: u64,
                started_at_ms: i64,
                acked: Arc<AtomicBool>,
                notify: Arc<Notify>,
                cancel: CancellationToken,
            },
        }

        let prepared = {
            let mut inner = self.inner.lock().expect("wait registry");
            let current = inner.slots.get(channel_id).map(|s| s.view.clone());
            let decision = decide_acquire(
                current.as_ref(),
                &intent,
                inner.next_generation,
                now_unix_millis(),
                new_wait_id(),
            );
            match decision {
                WaitAcquireDecision::Admit {
                    wait_id,
                    generation,
                    started_at_ms,
                } => {
                    inner.next_generation = generation.saturating_add(1);
                    Prepared::Ready(self.install_locked(
                        &mut inner,
                        channel_id,
                        wait_id,
                        generation,
                        started_at_ms,
                        None,
                    ))
                }
                WaitAcquireDecision::RefuseActive {
                    existing_wait_id,
                    started_at_ms,
                } => {
                    return Err(CoreError::WaitAlreadyActive {
                        team: team.to_string(),
                        channel: channel.to_string(),
                        existing_wait_id,
                        started_at_ms,
                    });
                }
                WaitAcquireDecision::ConflictChanged => {
                    return Err(CoreError::WaitConflictChanged {
                        team: team.to_string(),
                        channel: channel.to_string(),
                    });
                }
                WaitAcquireDecision::BeginReplace {
                    old_wait_id,
                    old_generation,
                    new_wait_id,
                    new_generation,
                    started_at_ms,
                } => {
                    inner.next_generation = new_generation.saturating_add(1);
                    let slot = inner.slots.get_mut(channel_id).ok_or_else(|| {
                        CoreError::WaitConflictChanged {
                            team: team.to_string(),
                            channel: channel.to_string(),
                        }
                    })?;
                    slot.view.replacing = true;
                    *slot.replaced_by.lock().expect("replaced_by") = Some(new_wait_id.clone());
                    Prepared::Replacing {
                        old_wait_id,
                        old_generation,
                        new_wait_id,
                        new_generation,
                        started_at_ms,
                        acked: Arc::clone(&slot.cleanup_acked),
                        notify: Arc::clone(&slot.cleanup_notify),
                        cancel: slot.cancel.clone(),
                    }
                }
            }
        };

        match prepared {
            Prepared::Ready(lease) => Ok(lease),
            Prepared::Replacing {
                old_wait_id,
                old_generation,
                new_wait_id,
                new_generation,
                started_at_ms,
                acked,
                notify,
                cancel,
            } => {
                cancel.cancel();
                let budget = remaining.min(Duration::from_secs(REPLACE_CLEANUP_BUDGET_SECS));
                if !wait_for_cleanup(budget, &acked, &notify).await {
                    return Err(CoreError::WaitReplaceUnconfirmed {
                        team: team.to_string(),
                        channel: channel.to_string(),
                        existing_wait_id: old_wait_id,
                    });
                }
                let mut inner = self.inner.lock().expect("wait registry");
                let current = inner.slots.get(channel_id).map(|s| s.view.clone());
                if current.is_some()
                    && !can_install_replacement(current.as_ref(), old_generation, new_generation)
                {
                    return Err(CoreError::WaitReplaceUnconfirmed {
                        team: team.to_string(),
                        channel: channel.to_string(),
                        existing_wait_id: old_wait_id,
                    });
                }
                Ok(self.install_locked(
                    &mut inner,
                    channel_id,
                    new_wait_id,
                    new_generation,
                    started_at_ms,
                    Some(old_wait_id),
                ))
            }
        }
    }

    fn install_locked(
        self: &Arc<Self>,
        inner: &mut Inner,
        channel_id: &str,
        wait_id: String,
        generation: u64,
        started_at_ms: i64,
        replaced_wait_id: Option<String>,
    ) -> WaitLease {
        let cleanup_acked = Arc::new(AtomicBool::new(false));
        let cleanup_notify = Arc::new(Notify::new());
        let cancel = CancellationToken::new();
        let replaced_by = Arc::new(Mutex::new(None));
        inner.slots.insert(
            channel_id.to_string(),
            LiveSlot {
                view: WaitSlotView {
                    wait_id: wait_id.clone(),
                    generation,
                    started_at_ms,
                    replacing: false,
                },
                cancel: cancel.clone(),
                cleanup_acked: Arc::clone(&cleanup_acked),
                cleanup_notify: Arc::clone(&cleanup_notify),
                replaced_by: Arc::clone(&replaced_by),
            },
        );
        WaitLease {
            registry: Arc::clone(self),
            channel_id: channel_id.to_string(),
            wait_id,
            generation,
            replaced_wait_id,
            cancel,
            replaced_by,
            cleanup_acked,
            cleanup_notify,
        }
    }

    fn release(&self, channel_id: &str, generation: u64) {
        let mut inner = self.inner.lock().expect("wait registry");
        let current = inner.slots.get(channel_id).map(|s| s.view.clone());
        if should_release(current.as_ref(), generation) {
            inner.slots.remove(channel_id);
        }
    }

    pub fn cancel_all(&self) {
        let inner = self.inner.lock().expect("wait registry");
        for slot in inner.slots.values() {
            slot.cancel.cancel();
        }
    }
}

async fn wait_for_cleanup(budget: Duration, acked: &AtomicBool, notify: &Notify) -> bool {
    if acked.load(Ordering::SeqCst) {
        return true;
    }
    let _ = timeout(budget, notify.notified()).await;
    acked.load(Ordering::SeqCst)
}

impl WaitLease {
    pub fn into_guard(self) -> (WaitSession, WaitGuard) {
        let session = WaitSession {
            wait_id: self.wait_id,
            replaced_wait_id: self.replaced_wait_id,
            cancel: self.cancel,
            replaced_by: self.replaced_by,
        };
        let guard = WaitGuard {
            registry: self.registry,
            channel_id: self.channel_id,
            generation: self.generation,
            cleanup_acked: self.cleanup_acked,
            cleanup_notify: self.cleanup_notify,
        };
        (session, guard)
    }
}

pub struct WaitSession {
    pub wait_id: String,
    pub replaced_wait_id: Option<String>,
    pub cancel: CancellationToken,
    replaced_by: Arc<Mutex<Option<String>>>,
}

impl WaitSession {
    pub fn replaced_by_id(&self) -> String {
        self.replaced_by
            .lock()
            .expect("replaced_by")
            .clone()
            .unwrap_or_default()
    }
}

impl Drop for WaitGuard {
    fn drop(&mut self) {
        self.registry.release(&self.channel_id, self.generation);
        self.cleanup_acked.store(true, Ordering::SeqCst);
        self.cleanup_notify.notify_one();
    }
}
