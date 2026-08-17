//! In-memory single-waiter registry for one profile daemon.
//!
//! Keyed only by canonical channel id. Not persisted and not host-global.
//! A future fan-in waiter will need multi-key acquisition; this cut
//! admits exactly one key per wait.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;

use chanvoy_core::{
    can_install_replacement, decide_acquire, new_wait_id, now_unix_millis, should_release,
    CoreError, WaitAcquireDecision, WaitAcquireIntent, WaitSlotView, REPLACE_CLEANUP_BUDGET_SECS,
};
use tokio::sync::Notify;
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;

/// Test-only barrier: park `complete_guard` after it has observed the
/// reservation and released the registry lock, before it publishes ack.
pub struct CompleteGuardHold {
    parked: AtomicBool,
    go: Mutex<bool>,
    cv: Condvar,
}

impl CompleteGuardHold {
    #[cfg(test)]
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            parked: AtomicBool::new(false),
            go: Mutex::new(false),
            cv: Condvar::new(),
        })
    }

    #[cfg(test)]
    pub fn is_parked(&self) -> bool {
        self.parked.load(Ordering::SeqCst)
    }

    #[cfg(test)]
    pub fn release(&self) {
        let mut go = self.go.lock().expect("complete-guard go");
        *go = true;
        self.cv.notify_one();
    }

    fn park(&self) {
        self.parked.store(true, Ordering::SeqCst);
        let mut go = self.go.lock().expect("complete-guard go");
        while !*go {
            go = self.cv.wait(go).expect("complete-guard wait");
        }
    }
}

pub struct WaitOwnerRegistry {
    inner: Mutex<Inner>,
    cleanup_budget: Mutex<Duration>,
    /// Incremented only by the wait engine after a successful acquire.
    /// Refused / unconfirmed acquires never arm, so this is the
    /// subscribe/backfill gate for tests.
    armed_after_acquire: AtomicU64,
    provider_io: AtomicU64,
    post_ack_hold: Mutex<Option<Arc<Notify>>>,
    complete_guard_hold: Mutex<Option<Arc<CompleteGuardHold>>>,
}

#[derive(Debug)]
struct Inner {
    slots: HashMap<String, LiveSlot>,
    next_generation: u64,
}

#[derive(Debug)]
struct LiveSlot {
    view: WaitSlotView,
    cancel: CancellationToken,
    cleanup_acked: Arc<AtomicBool>,
    cleanup_notify: Arc<Notify>,
    replaced_by: Arc<Mutex<Option<String>>>,
    /// True from BeginReplace until install or abandoned unconfirmed.
    handoff_pending: bool,
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

struct HandoffTxn {
    registry: Arc<WaitOwnerRegistry>,
    channel_id: String,
    generation: u64,
    completed: bool,
}

impl Drop for HandoffTxn {
    fn drop(&mut self) {
        if !self.completed {
            self.registry
                .abandon_handoff(&self.channel_id, self.generation);
        }
    }
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
            cleanup_budget: Mutex::new(Duration::from_secs(REPLACE_CLEANUP_BUDGET_SECS)),
            armed_after_acquire: AtomicU64::new(0),
            provider_io: AtomicU64::new(0),
            post_ack_hold: Mutex::new(None),
            complete_guard_hold: Mutex::new(None),
        }
    }

    #[allow(dead_code)]
    #[cfg(test)]
    pub fn set_cleanup_budget(&self, budget: Duration) {
        *self.cleanup_budget.lock().expect("cleanup budget") = budget;
    }

    pub fn note_arm(&self) {
        self.armed_after_acquire.fetch_add(1, Ordering::SeqCst);
    }

    #[allow(dead_code)]
    #[cfg(test)]
    pub fn armed_count(&self) -> u64 {
        self.armed_after_acquire.load(Ordering::SeqCst)
    }

    pub fn note_provider_io(&self) {
        self.provider_io.fetch_add(1, Ordering::SeqCst);
    }

    #[cfg(test)]
    pub fn provider_io_count(&self) -> u64 {
        self.provider_io.load(Ordering::SeqCst)
    }

    #[cfg(test)]
    pub fn set_post_ack_hold(&self, gate: Arc<Notify>) {
        *self.post_ack_hold.lock().expect("post-ack hold") = Some(gate);
    }

    #[cfg(test)]
    pub fn set_complete_guard_hold(&self, hold: Arc<CompleteGuardHold>) {
        *self
            .complete_guard_hold
            .lock()
            .expect("complete-guard hold") = Some(hold);
    }

    #[cfg(test)]
    pub fn cleanup_acked(&self, channel_id: &str) -> bool {
        self.inner
            .lock()
            .expect("wait registry")
            .slots
            .get(channel_id)
            .is_some_and(|s| s.cleanup_acked.load(Ordering::SeqCst))
    }

    #[allow(dead_code)]
    #[cfg(test)]
    pub fn snapshot(&self, channel_id: &str) -> Option<WaitSlotView> {
        self.inner
            .lock()
            .expect("wait registry")
            .slots
            .get(channel_id)
            .map(|s| s.view.clone())
    }

    #[allow(dead_code)]
    #[cfg(test)]
    pub fn release_generation(&self, channel_id: &str, generation: u64) {
        self.release(channel_id, generation);
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
                    slot.handoff_pending = true;
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
                let mut txn = HandoffTxn {
                    registry: Arc::clone(self),
                    channel_id: channel_id.to_string(),
                    generation: old_generation,
                    completed: false,
                };
                cancel.cancel();
                let configured = *self.cleanup_budget.lock().expect("cleanup budget");
                let budget = remaining.min(configured);
                if !wait_for_cleanup(budget, &acked, &notify).await {
                    self.abandon_handoff(channel_id, old_generation);
                    return Err(CoreError::WaitReplaceUnconfirmed {
                        team: team.to_string(),
                        channel: channel.to_string(),
                        existing_wait_id: old_wait_id,
                    });
                }
                let gate = self.post_ack_hold.lock().expect("post-ack hold").take();
                if let Some(gate) = gate {
                    gate.notified().await;
                }
                let mut inner = self.inner.lock().expect("wait registry");
                let current = inner.slots.get(channel_id).map(|s| s.view.clone());
                if !can_install_replacement(current.as_ref(), old_generation, new_generation) {
                    if let Some(slot) = inner.slots.get_mut(channel_id) {
                        if slot.view.generation == old_generation {
                            slot.handoff_pending = false;
                        }
                    }
                    return Err(CoreError::WaitReplaceUnconfirmed {
                        team: team.to_string(),
                        channel: channel.to_string(),
                        existing_wait_id: old_wait_id,
                    });
                }
                let lease = self.install_locked(
                    &mut inner,
                    channel_id,
                    new_wait_id,
                    new_generation,
                    started_at_ms,
                    Some(old_wait_id),
                );
                txn.completed = true;
                Ok(lease)
            }
        }
    }

    /// Acquire every key or release those already taken. Refusal happens
    /// before the caller starts additional provider observation.
    pub async fn acquire_all(
        self: &Arc<Self>,
        keys: &[(String, String, String)],
        remaining: Duration,
    ) -> Result<Vec<WaitLease>, CoreError> {
        let mut held = Vec::with_capacity(keys.len());
        for (channel_id, team, channel) in keys {
            match self
                .acquire(channel_id, team, channel, None, remaining)
                .await
            {
                Ok(lease) => held.push(lease),
                Err(err) => {
                    for lease in held {
                        drop(lease.into_guard());
                    }
                    return Err(err);
                }
            }
        }
        Ok(held)
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
                handoff_pending: false,
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

    fn abandon_handoff(&self, channel_id: &str, generation: u64) {
        let mut inner = self.inner.lock().expect("wait registry");
        Self::abandon_handoff_locked(&mut inner, channel_id, generation);
    }

    fn abandon_handoff_locked(inner: &mut Inner, channel_id: &str, generation: u64) {
        let acked = inner.slots.get(channel_id).is_some_and(|slot| {
            slot.view.generation == generation && slot.cleanup_acked.load(Ordering::SeqCst)
        });
        if let Some(slot) = inner.slots.get_mut(channel_id) {
            if slot.view.generation == generation {
                slot.handoff_pending = false;
            }
        }
        if acked
            && inner
                .slots
                .get(channel_id)
                .is_some_and(|s| s.view.generation == generation)
        {
            inner.slots.remove(channel_id);
        }
    }

    /// Observe reservation, optionally park (tests), then re-check and
    /// either publish ack on a live handoff or release the generation.
    fn complete_guard(&self, channel_id: &str, generation: u64, cleanup_acked: &AtomicBool) {
        let observed_pending = {
            let inner = self.inner.lock().expect("wait registry");
            inner
                .slots
                .get(channel_id)
                .is_some_and(|s| s.view.generation == generation && s.handoff_pending)
        };
        let hold = self
            .complete_guard_hold
            .lock()
            .expect("complete-guard hold")
            .take();
        if let Some(hold) = hold {
            hold.park();
        }
        let mut inner = self.inner.lock().expect("wait registry");
        let view = inner.slots.get(channel_id).map(|s| s.view.clone());
        let pending = inner
            .slots
            .get(channel_id)
            .is_some_and(|s| s.handoff_pending);
        if view.as_ref().is_some_and(|v| v.generation == generation) {
            if pending {
                if let Some(slot) = inner.slots.get(channel_id) {
                    slot.cleanup_acked.store(true, Ordering::SeqCst);
                }
            } else if observed_pending || should_release(view.as_ref(), generation) {
                inner.slots.remove(channel_id);
            }
        }
        cleanup_acked.store(true, Ordering::SeqCst);
    }

    #[cfg(test)]
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
        self.registry
            .complete_guard(&self.channel_id, self.generation, &self.cleanup_acked);
        self.cleanup_notify.notify_one();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Instant;

    fn id(n: u8) -> String {
        format!("wait_{n:032x}")
    }

    #[tokio::test]
    async fn replace_waits_for_old_cleanup_before_admit() {
        let reg = Arc::new(WaitOwnerRegistry::new());
        let old = reg
            .acquire("ch-1", "org", "brief", None, Duration::from_secs(5))
            .await
            .expect("admit old");
        let old_id = old.wait_id.clone();
        let (_session, guard) = old.into_guard();
        reg.note_arm();
        assert_eq!(reg.armed_count(), 1);

        let replacing = Arc::clone(&reg);
        let old_id_clone = old_id.clone();
        let replace = tokio::spawn(async move {
            replacing
                .acquire(
                    "ch-1",
                    "org",
                    "brief",
                    Some(&old_id_clone),
                    Duration::from_secs(5),
                )
                .await
        });
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(
            !replace.is_finished(),
            "replace must not admit before old cleanup"
        );
        assert_eq!(reg.armed_count(), 1, "new waiter must not arm yet");
        assert!(reg.snapshot("ch-1").is_some_and(|s| s.replacing));

        drop(guard);
        let new_lease = replace.await.expect("join").expect("replace admitted");
        assert_eq!(new_lease.replaced_wait_id.as_deref(), Some(old_id.as_str()));
        reg.note_arm();
        assert_eq!(reg.armed_count(), 2);
        assert!(reg.snapshot("ch-1").is_some_and(|s| !s.replacing));
    }

    #[tokio::test]
    async fn reservation_blocks_default_between_ack_and_install() {
        let reg = Arc::new(WaitOwnerRegistry::new());
        let old = reg
            .acquire("ch-1", "org", "brief", None, Duration::from_secs(5))
            .await
            .expect("admit");
        let old_id = old.wait_id.clone();
        let (_session, guard) = old.into_guard();

        let gate = Arc::new(Notify::new());
        reg.set_post_ack_hold(Arc::clone(&gate));

        let replacing = Arc::clone(&reg);
        let old_id_clone = old_id.clone();
        let replace = tokio::spawn(async move {
            replacing
                .acquire(
                    "ch-1",
                    "org",
                    "brief",
                    Some(&old_id_clone),
                    Duration::from_secs(5),
                )
                .await
        });
        let mark = Instant::now() + Duration::from_millis(200);
        while !reg.snapshot("ch-1").is_some_and(|s| s.replacing) {
            assert!(Instant::now() < mark, "replace never marked reservation");
            tokio::time::sleep(Duration::from_millis(5)).await;
        }

        drop(guard);
        let deadline = Instant::now() + Duration::from_millis(200);
        while !reg.cleanup_acked("ch-1") {
            assert!(Instant::now() < deadline, "cleanup ack never published");
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        assert!(
            !replace.is_finished(),
            "replacer must park after ack before install"
        );
        assert!(
            reg.snapshot("ch-1").is_some(),
            "reservation must survive acknowledgement"
        );

        match reg
            .acquire("ch-1", "org", "brief", None, Duration::from_secs(1))
            .await
        {
            Err(CoreError::WaitAlreadyActive {
                existing_wait_id, ..
            }) => assert_eq!(existing_wait_id, old_id),
            Ok(_) => panic!("default acquire won the post-ack window"),
            Err(other) => panic!("expected already-active in post-ack window, got {other}"),
        }

        gate.notify_one();
        let new_lease = replace.await.expect("join").expect("install after gate");
        assert_eq!(new_lease.replaced_wait_id.as_deref(), Some(old_id.as_str()));
        assert_eq!(reg.armed_count(), 0);
    }

    #[tokio::test]
    async fn aborted_replace_during_cleanup_does_not_strand_key() {
        let reg = Arc::new(WaitOwnerRegistry::new());
        let old = reg
            .acquire("ch-1", "org", "brief", None, Duration::from_secs(5))
            .await
            .expect("admit");
        let old_id = old.wait_id.clone();
        let (_session, guard) = old.into_guard();

        let replacing = Arc::clone(&reg);
        let replace = tokio::spawn(async move {
            replacing
                .acquire(
                    "ch-1",
                    "org",
                    "brief",
                    Some(&old_id),
                    Duration::from_secs(5),
                )
                .await
        });
        let mark = Instant::now() + Duration::from_millis(200);
        while !reg.snapshot("ch-1").is_some_and(|s| s.replacing) {
            assert!(Instant::now() < mark, "replace never started");
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        replace.abort();
        let _ = replace.await;

        drop(guard);
        assert!(
            reg.snapshot("ch-1").is_none(),
            "old cleanup after aborted replace must release the key"
        );
        assert!(
            reg.acquire("ch-1", "org", "brief", None, Duration::from_secs(1))
                .await
                .is_ok(),
            "later default waiter must admit"
        );
    }

    #[tokio::test]
    async fn aborted_replace_after_ack_releases_reservation() {
        let reg = Arc::new(WaitOwnerRegistry::new());
        let old = reg
            .acquire("ch-1", "org", "brief", None, Duration::from_secs(5))
            .await
            .expect("admit");
        let old_id = old.wait_id.clone();
        let (_session, guard) = old.into_guard();
        let gate = Arc::new(Notify::new());
        reg.set_post_ack_hold(Arc::clone(&gate));

        let replacing = Arc::clone(&reg);
        let replace = tokio::spawn(async move {
            replacing
                .acquire(
                    "ch-1",
                    "org",
                    "brief",
                    Some(&old_id),
                    Duration::from_secs(5),
                )
                .await
        });
        let mark = Instant::now() + Duration::from_millis(200);
        while !reg.snapshot("ch-1").is_some_and(|s| s.replacing) {
            assert!(Instant::now() < mark, "replace never started");
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        drop(guard);
        while !reg.cleanup_acked("ch-1") {
            assert!(Instant::now() < mark + Duration::from_millis(200), "no ack");
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        replace.abort();
        let _ = replace.await;
        assert!(
            reg.snapshot("ch-1").is_none(),
            "aborted replacer must not leave a stranded reservation"
        );
        assert!(reg
            .acquire("ch-1", "org", "brief", None, Duration::from_secs(1))
            .await
            .is_ok());
    }

    #[tokio::test]
    async fn unconfirmed_replace_is_deterministic_and_retains_old() {
        let reg = Arc::new(WaitOwnerRegistry::new());
        reg.set_cleanup_budget(Duration::from_millis(1));
        let old = reg
            .acquire("ch-1", "org", "brief", None, Duration::from_secs(5))
            .await
            .expect("admit");
        let old_id = old.wait_id.clone();
        let old_gen = old.generation;
        let (_session, guard) = old.into_guard();
        reg.note_arm();

        let started = Instant::now();
        let err = match reg
            .acquire(
                "ch-1",
                "org",
                "brief",
                Some(&old_id),
                Duration::from_secs(5),
            )
            .await
        {
            Err(e) => e,
            Ok(_) => panic!("held guard must not ack"),
        };
        assert!(
            started.elapsed() < Duration::from_millis(200),
            "cleanup budget must not be a wall-clock 5s: {:?}",
            started.elapsed()
        );
        match err {
            CoreError::WaitReplaceUnconfirmed {
                existing_wait_id, ..
            } => assert_eq!(existing_wait_id, old_id),
            other => panic!("expected unconfirmed, got {other:?}"),
        }
        assert_eq!(reg.armed_count(), 1);
        let snap = reg.snapshot("ch-1").expect("old slot retained");
        assert_eq!(snap.wait_id, old_id);
        assert_eq!(snap.generation, old_gen);
        assert!(snap.replacing);

        drop(guard);
        // Late old guard releases that generation only.
        assert!(reg.snapshot("ch-1").is_none());
    }

    #[tokio::test]
    async fn timeout_and_old_guard_drop_cannot_strand_reservation() {
        let reg = Arc::new(WaitOwnerRegistry::new());
        reg.set_cleanup_budget(Duration::from_millis(15));
        let hold = CompleteGuardHold::new();
        reg.set_complete_guard_hold(Arc::clone(&hold));
        let old = reg
            .acquire("ch-1", "org", "brief", None, Duration::from_secs(5))
            .await
            .expect("admit");
        let old_id = old.wait_id.clone();
        let old_gen = old.generation;
        let (_session, guard) = old.into_guard();
        let before_io = reg.provider_io_count();

        let replacing = Arc::clone(&reg);
        let replace = tokio::spawn(async move {
            replacing
                .acquire(
                    "ch-1",
                    "org",
                    "brief",
                    Some(&old_id),
                    Duration::from_secs(5),
                )
                .await
        });
        let mark = Instant::now() + Duration::from_millis(200);
        while !reg.snapshot("ch-1").is_some_and(|s| s.replacing) {
            assert!(Instant::now() < mark, "replace never started");
            tokio::time::sleep(Duration::from_millis(1)).await;
        }

        let dropper = tokio::task::spawn_blocking(move || drop(guard));
        let parked = Instant::now() + Duration::from_millis(200);
        while !hold.is_parked() {
            assert!(Instant::now() < parked, "complete_guard never parked");
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        assert!(
            !reg.cleanup_acked("ch-1"),
            "ack must not publish until after abandon starts"
        );

        let replace_result = replace.await.expect("join");
        assert!(
            matches!(
                replace_result,
                Err(CoreError::WaitReplaceUnconfirmed { .. })
            ),
            "parked ack must cause cleanup timeout"
        );
        hold.release();
        dropper.await.expect("dropper");

        assert!(
            reg.snapshot("ch-1").is_none()
                || reg
                    .snapshot("ch-1")
                    .is_some_and(|s| s.generation != old_gen),
            "old generation must not remain stranded"
        );
        assert_eq!(reg.provider_io_count(), before_io);

        match reg
            .acquire("ch-1", "org", "brief", None, Duration::from_secs(1))
            .await
        {
            Ok(lease) => {
                assert_ne!(lease.generation, old_gen);
                let (_s, g) = lease.into_guard();
                drop(g);
            }
            Err(err) => panic!("stranded reservation after timeout/drop race: {err}"),
        }
    }

    #[tokio::test]
    async fn replace_acquire_does_not_increment_provider_io() {
        let reg = Arc::new(WaitOwnerRegistry::new());
        let old = reg
            .acquire("ch-1", "org", "brief", None, Duration::from_secs(5))
            .await
            .expect("admit");
        let old_id = old.wait_id.clone();
        let (_session, guard) = old.into_guard();
        reg.note_provider_io();
        let before = reg.provider_io_count();
        let gate = Arc::new(Notify::new());
        reg.set_post_ack_hold(Arc::clone(&gate));

        let replacing = Arc::clone(&reg);
        let replace = tokio::spawn(async move {
            replacing
                .acquire(
                    "ch-1",
                    "org",
                    "brief",
                    Some(&old_id),
                    Duration::from_secs(5),
                )
                .await
        });
        let mark = Instant::now() + Duration::from_millis(200);
        while !reg.snapshot("ch-1").is_some_and(|s| s.replacing) {
            assert!(Instant::now() < mark, "replace never started");
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        drop(guard);
        while !reg.cleanup_acked("ch-1") {
            assert!(Instant::now() < mark + Duration::from_millis(200), "no ack");
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        assert_eq!(reg.provider_io_count(), before);
        gate.notify_one();
        assert!(replace.await.expect("join").is_ok());
        assert_eq!(
            reg.provider_io_count(),
            before,
            "install still precedes wait-engine provider I/O"
        );
    }

    #[tokio::test]
    async fn third_waiter_and_late_guard_cannot_steal_generation() {
        let reg = Arc::new(WaitOwnerRegistry::new());
        let old = reg
            .acquire("ch-1", "org", "brief", None, Duration::from_secs(5))
            .await
            .expect("admit");
        let old_id = old.wait_id.clone();
        let old_gen = old.generation;
        let (_session, guard) = old.into_guard();

        let replacing = Arc::clone(&reg);
        let old_id_for_replace = old_id.clone();
        let replace = tokio::spawn(async move {
            replacing
                .acquire(
                    "ch-1",
                    "org",
                    "brief",
                    Some(&old_id_for_replace),
                    Duration::from_secs(5),
                )
                .await
        });
        tokio::time::sleep(Duration::from_millis(15)).await;

        match reg
            .acquire("ch-1", "org", "brief", None, Duration::from_secs(1))
            .await
        {
            Err(CoreError::WaitAlreadyActive {
                existing_wait_id, ..
            }) => assert_eq!(existing_wait_id, old_id),
            Ok(_) => panic!("default third waiter must refuse"),
            Err(other) => panic!("default third waiter must refuse, got {other}"),
        }
        match reg
            .acquire(
                "ch-1",
                "org",
                "brief",
                Some(&old_id),
                Duration::from_secs(1),
            )
            .await
        {
            Err(CoreError::WaitConflictChanged { .. }) => {}
            Ok(_) => panic!("in-flight token is consumed"),
            Err(other) => panic!("in-flight token is consumed, got {other}"),
        }
        assert_eq!(reg.armed_count(), 0);

        drop(guard);
        let new_lease = replace.await.expect("join").expect("install new");
        let new_gen = new_lease.generation;
        assert_ne!(new_gen, old_gen);
        let (_s, new_guard) = new_lease.into_guard();

        reg.release_generation("ch-1", old_gen);
        let snap = reg.snapshot("ch-1").expect("newer generation remains");
        assert_eq!(snap.generation, new_gen);
        drop(new_guard);
    }

    #[tokio::test]
    async fn bad_replace_tokens_do_not_cancel_active() {
        let reg = Arc::new(WaitOwnerRegistry::new());
        let old = reg
            .acquire("ch-1", "org", "brief", None, Duration::from_secs(5))
            .await
            .expect("admit");
        let old_id = old.wait_id.clone();
        let old_gen = old.generation;
        let (_s, guard) = old.into_guard();

        for token in [None, Some(id(9).as_str()), Some("not-a-wait-id"), Some("")] {
            if token.is_none() {
                assert!(matches!(
                    reg.acquire(
                        "ch-2",
                        "org",
                        "other",
                        Some(&old_id),
                        Duration::from_secs(1)
                    )
                    .await,
                    Err(CoreError::WaitConflictChanged { .. })
                ));
                continue;
            }
            assert!(matches!(
                reg.acquire("ch-1", "org", "brief", token, Duration::from_secs(1))
                    .await,
                Err(CoreError::WaitConflictChanged { .. })
                    | Err(CoreError::WaitAlreadyActive { .. })
            ));
        }

        let snap = reg.snapshot("ch-1").expect("still owned");
        assert_eq!(snap.wait_id, old_id);
        assert_eq!(snap.generation, old_gen);
        assert!(!snap.replacing);
        drop(guard);
    }
}
