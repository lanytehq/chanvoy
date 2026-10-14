//! Non-serialized ownership foundation inside the existing registry mutex.
//! No RPC, publisher, shutdown, provider or filesystem I/O occurs here.
//! Callers must keep request guards with actual work and transfer retained
//! ownership before an asynchronous effect can outlive that guard.
// The foundation is intentionally not routed until the schema-pin gate.
#![allow(dead_code)]

use super::{Inner, WaitOwnerRegistry};
use chanvoy_core::runtime::InstanceId;
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Phase {
    Running,
    Retiring,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Kind {
    Request,
    Stream,
    Cleanup,
}

#[derive(Debug)]
pub(super) struct Inventory {
    instance: InstanceId,
    phase: Phase,
    next_owner: Option<u64>,
    owners: HashMap<u64, Kind>,
}

impl Inventory {
    pub(super) fn new(instance: InstanceId) -> Self {
        Self {
            instance,
            phase: Phase::Running,
            next_owner: Some(1),
            owners: HashMap::new(),
        }
    }

    pub(super) fn ensure_running(&self) -> Result<(), AdmissionError> {
        match self.phase {
            Phase::Running => Ok(()),
            Phase::Retiring => Err(AdmissionError::Closed),
        }
    }

    fn reserve(&mut self) -> Result<OwnerToken, AdmissionError> {
        self.ensure_running()?;
        let sequence = self.next_owner.ok_or(AdmissionError::Exhausted)?;
        self.next_owner = sequence.checked_add(1);
        assert!(self.owners.insert(sequence, Kind::Request).is_none());
        Ok(OwnerToken {
            instance: self.instance,
            sequence,
            kind: Kind::Request,
        })
    }

    fn validate(&self, token: OwnerToken) -> Result<(), AdmissionError> {
        if token.instance != self.instance {
            return Err(AdmissionError::WrongInstance);
        }
        if self.owners.get(&token.sequence) != Some(&token.kind) {
            return Err(AdmissionError::OwnerChanged);
        }
        Ok(())
    }

    fn transfer(&mut self, token: OwnerToken, kind: Kind) -> Result<OwnerToken, AdmissionError> {
        self.validate(token)?;
        *self.owners.get_mut(&token.sequence).unwrap() = kind;
        Ok(OwnerToken { kind, ..token })
    }

    fn release(&mut self, token: OwnerToken) -> Result<(), AdmissionError> {
        self.validate(token)?;
        self.owners.remove(&token.sequence);
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AdmissionError {
    Closed,
    Exhausted,
    WrongInstance,
    OwnerChanged,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct OwnerToken {
    instance: InstanceId,
    sequence: u64,
    kind: Kind,
}

/// Application memory facts only. Durable/native authority is independent.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Snapshot {
    pub instance: InstanceId,
    pub phase: Phase,
    pub requests: usize,
    pub streams: usize,
    pub slots: usize,
    pub replacements: usize,
    pub cleanup: usize,
}

impl Snapshot {
    fn from_inner(inner: &Inner) -> Self {
        let count = |kind| {
            inner
                .admission
                .owners
                .values()
                .filter(|v| **v == kind)
                .count()
        };
        Self {
            instance: inner.admission.instance,
            phase: inner.admission.phase,
            requests: count(Kind::Request),
            streams: count(Kind::Stream),
            slots: inner.slots.len(),
            replacements: inner.slots.values().filter(|s| s.handoff_pending).count(),
            cleanup: count(Kind::Cleanup),
        }
    }

    fn is_empty(self) -> bool {
        self.requests == 0
            && self.streams == 0
            && self.slots == 0
            && self.replacements == 0
            && self.cleanup == 0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RetirementDecision {
    Closed(Snapshot),
    AlreadyRetiring(Snapshot),
    Busy(Snapshot),
}

/// Drop does not confirm completion. Callers retain this through actual work
/// and explicitly confirm its exact owner or transfer it to retained effects.
#[must_use = "keep until exact completion, or transfer before detaching work"]
pub(crate) struct RequestPermit {
    registry: Arc<WaitOwnerRegistry>,
    token: OwnerToken,
}

impl RequestPermit {
    pub(crate) fn into_stream(self) -> Result<RetainedOwner, AdmissionError> {
        self.retain(Kind::Stream)
    }

    pub(crate) fn into_cleanup(self) -> Result<RetainedOwner, AdmissionError> {
        self.retain(Kind::Cleanup)
    }

    fn retain(self, kind: Kind) -> Result<RetainedOwner, AdmissionError> {
        let mut inner = self.registry.inner.lock().expect("wait registry");
        let token = inner.admission.transfer(self.token, kind)?;
        drop(inner);
        Ok(RetainedOwner {
            registry: self.registry,
            token,
        })
    }

    /// Call only after this request/task's work and inverse cleanup have
    /// actually ended. Detached work must still own a separate permit.
    pub(crate) fn confirm_done(self) -> Result<(), AdmissionError> {
        self.registry
            .inner
            .lock()
            .expect("wait registry")
            .admission
            .release(self.token)
    }
}

/// Dropping this handle deliberately leaves ownership in the arena. Transport
/// loss, abort or timeout cannot substitute for explicit cleanup confirmation.
#[must_use = "retain until exact unsubscribe or cleanup is confirmed"]
pub(crate) struct RetainedOwner {
    registry: Arc<WaitOwnerRegistry>,
    token: OwnerToken,
}

impl RetainedOwner {
    pub(crate) fn into_cleanup(self) -> Result<Self, AdmissionError> {
        let token = self
            .registry
            .inner
            .lock()
            .expect("wait registry")
            .admission
            .transfer(self.token, Kind::Cleanup)?;
        Ok(Self {
            registry: self.registry,
            token,
        })
    }

    /// Call only after exact owned work cannot insert, send, publish or recur.
    /// This method performs no cancellation or join on the caller's behalf.
    pub(crate) fn confirm_done(self) -> Result<(), AdmissionError> {
        self.registry
            .inner
            .lock()
            .expect("wait registry")
            .admission
            .release(self.token)
    }
}

impl WaitOwnerRegistry {
    pub(crate) fn request_permit(self: &Arc<Self>) -> Result<RequestPermit, AdmissionError> {
        let token = self
            .inner
            .lock()
            .expect("wait registry")
            .admission
            .reserve()?;
        Ok(RequestPermit {
            registry: Arc::clone(self),
            token,
        })
    }

    pub(crate) fn admission_snapshot(&self) -> Snapshot {
        Snapshot::from_inner(&self.inner.lock().expect("wait registry"))
    }

    /// Latch only an empty Running arena. This makes no durable publication,
    /// cancellation, shutdown or native death claim and never reopens phase.
    pub(crate) fn retire_if_idle(
        &self,
        expected_instance: InstanceId,
    ) -> Result<RetirementDecision, AdmissionError> {
        let mut inner = self.inner.lock().expect("wait registry");
        if expected_instance != inner.admission.instance {
            return Err(AdmissionError::WrongInstance);
        }
        let before = Snapshot::from_inner(&inner);
        if before.phase == Phase::Retiring {
            return Ok(RetirementDecision::AlreadyRetiring(before));
        }
        if !before.is_empty() {
            return Ok(RetirementDecision::Busy(before));
        }
        inner.admission.phase = Phase::Retiring;
        Ok(RetirementDecision::Closed(Snapshot::from_inner(&inner)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chanvoy_core::CoreError;
    use std::sync::{mpsc, Barrier};
    use std::time::Duration;

    fn registry() -> (Arc<WaitOwnerRegistry>, InstanceId) {
        let instance = InstanceId::new();
        (
            Arc::new(WaitOwnerRegistry::for_instance(instance)),
            instance,
        )
    }

    #[test]
    fn preflight_without_a_slot_blocks_retirement_until_confirmed_abandonment() {
        let (reg, instance) = registry();
        let preflight = reg.request_permit().unwrap();
        let snapshot = reg.admission_snapshot();
        assert_eq!(snapshot.requests, 1);
        assert_eq!(snapshot.slots, 0);
        assert!(matches!(
            reg.retire_if_idle(instance),
            Ok(RetirementDecision::Busy(_))
        ));
        assert_eq!(reg.admission_snapshot().phase, Phase::Running);
        preflight.confirm_done().unwrap(); // No detached work/publication started.
        assert!(matches!(
            reg.retire_if_idle(instance),
            Ok(RetirementDecision::Closed(_))
        ));
        assert!(matches!(reg.request_permit(), Err(AdmissionError::Closed)));
        assert!(matches!(
            reg.retire_if_idle(instance),
            Ok(RetirementDecision::AlreadyRetiring(_))
        ));
        assert!(reg.admission_snapshot().is_empty());
    }

    #[tokio::test]
    async fn closure_refuses_every_registry_admission_without_install_or_cancel() {
        let (reg, instance) = registry();
        assert!(matches!(
            reg.retire_if_idle(instance),
            Ok(RetirementDecision::Closed(_))
        ));
        let remaining = Duration::from_secs(1);
        for result in [
            reg.acquire("channel", "team", "name", None, remaining)
                .await,
            reg.acquire_direct("dm", "team", "direct", None, remaining)
                .await,
            reg.acquire_inbox(None, remaining).await,
        ] {
            assert!(matches!(result, Err(CoreError::RuntimeAdmissionClosed)));
        }
        let keys = vec![("channel".into(), "team".into(), "name".into())];
        assert!(matches!(
            reg.acquire_all(&keys, remaining).await,
            Err(CoreError::RuntimeAdmissionClosed)
        ));
        assert_eq!(reg.armed_count(), 0);
        assert_eq!(reg.provider_io_count(), 0);
        assert!(reg.admission_snapshot().is_empty());
    }

    #[test]
    fn transfer_has_no_empty_interval_and_stale_kind_or_epoch_cannot_release() {
        let (reg, instance) = registry();
        let request = reg.request_permit().unwrap();
        let old = request.token;
        let stream = request.into_stream().unwrap();
        assert_eq!(reg.admission_snapshot().requests, 0);
        assert_eq!(reg.admission_snapshot().streams, 1);
        {
            let mut inner = reg.inner.lock().unwrap();
            assert_eq!(
                inner.admission.release(old),
                Err(AdmissionError::OwnerChanged)
            );
            let foreign = OwnerToken {
                instance: InstanceId::new(),
                ..stream.token
            };
            assert_eq!(
                inner.admission.release(foreign),
                Err(AdmissionError::WrongInstance)
            );
        }
        assert!(matches!(
            reg.retire_if_idle(InstanceId::new()),
            Err(AdmissionError::WrongInstance)
        ));
        assert_eq!(reg.admission_snapshot().phase, Phase::Running);
        assert!(matches!(
            reg.retire_if_idle(instance),
            Ok(RetirementDecision::Busy(_))
        ));
        let cleanup = stream.into_cleanup().unwrap();
        assert_eq!(reg.admission_snapshot().streams, 0);
        assert_eq!(reg.admission_snapshot().cleanup, 1);
        assert!(matches!(
            reg.retire_if_idle(instance),
            Ok(RetirementDecision::Busy(_))
        ));
        cleanup.confirm_done().unwrap();
        assert!(matches!(
            reg.retire_if_idle(instance),
            Ok(RetirementDecision::Closed(_))
        ));
    }

    #[test]
    fn losing_a_retained_handle_does_not_confirm_cleanup_or_clear_other_owners() {
        let (reg, instance) = registry();
        let lost_request = reg.request_permit().unwrap();
        drop(lost_request);
        assert_eq!(reg.admission_snapshot().requests, 1);
        let lost = reg.request_permit().unwrap().into_cleanup().unwrap();
        drop(lost);
        let other = reg.request_permit().unwrap().into_cleanup().unwrap();
        assert_eq!(reg.admission_snapshot().cleanup, 2);
        other.confirm_done().unwrap();
        assert_eq!(reg.admission_snapshot().cleanup, 1);
        assert!(matches!(
            reg.retire_if_idle(instance),
            Ok(RetirementDecision::Busy(_))
        ));
    }

    #[test]
    fn losing_a_stream_handle_preserves_registration_ownership_across_transport_loss() {
        let (reg, instance) = registry();
        let stream = reg.request_permit().unwrap().into_stream().unwrap();
        drop(stream); // A transport wrapper is not an unsubscribe confirmation.
        let reconnect_work = reg.request_permit().unwrap();
        reconnect_work.confirm_done().unwrap();
        assert_eq!(reg.admission_snapshot().streams, 1);
        assert_eq!(reg.admission_snapshot().requests, 0);
        assert!(matches!(
            reg.retire_if_idle(instance),
            Ok(RetirementDecision::Busy(_))
        ));
    }

    #[test]
    fn transfer_and_retirement_race_cannot_expose_an_empty_arena() {
        for _ in 0..32 {
            let (reg, instance) = registry();
            let request = reg.request_permit().unwrap();
            let barrier = Arc::new(Barrier::new(3));
            let transfer = {
                let barrier = Arc::clone(&barrier);
                std::thread::spawn(move || {
                    barrier.wait();
                    request.into_stream()
                })
            };
            let retire = {
                let reg = Arc::clone(&reg);
                let barrier = Arc::clone(&barrier);
                std::thread::spawn(move || {
                    barrier.wait();
                    reg.retire_if_idle(instance)
                })
            };
            barrier.wait();
            let stream = transfer.join().unwrap().unwrap();
            let decision = retire.join().unwrap().unwrap();
            let RetirementDecision::Busy(snapshot) = decision else {
                panic!("a reservation transfer cannot expose zero: {decision:?}");
            };
            assert_eq!(snapshot.requests + snapshot.streams, 1);
            assert_eq!(reg.admission_snapshot().streams, 1);
            stream.confirm_done().unwrap();
            assert!(matches!(
                reg.retire_if_idle(instance),
                Ok(RetirementDecision::Closed(_))
            ));
        }
    }

    #[test]
    fn owner_counter_exhaustion_never_reuses_an_old_completion_token() {
        let (reg, instance) = registry();
        let first = reg.request_permit().unwrap();
        let stale = first.token;
        first.confirm_done().unwrap();
        reg.inner.lock().unwrap().admission.next_owner = Some(u64::MAX);
        let final_owner = reg.request_permit().unwrap();
        assert_eq!(final_owner.token.sequence, u64::MAX);
        assert_eq!(
            reg.inner.lock().unwrap().admission.release(stale),
            Err(AdmissionError::OwnerChanged)
        );
        assert!(matches!(
            reg.request_permit(),
            Err(AdmissionError::Exhausted)
        ));
        assert_eq!(reg.admission_snapshot().requests, 1);
        assert!(matches!(
            reg.retire_if_idle(instance),
            Ok(RetirementDecision::Busy(_))
        ));
        final_owner.confirm_done().unwrap();
        assert!(matches!(
            reg.retire_if_idle(instance),
            Ok(RetirementDecision::Closed(_))
        ));
    }

    #[test]
    fn simultaneous_admission_and_retirement_have_only_the_two_linearized_outcomes() {
        for _ in 0..32 {
            let (reg, instance) = registry();
            let barrier = Arc::new(Barrier::new(3));
            let admit = {
                let reg = Arc::clone(&reg);
                let barrier = Arc::clone(&barrier);
                std::thread::spawn(move || {
                    barrier.wait();
                    reg.request_permit()
                })
            };
            let retire = {
                let reg = Arc::clone(&reg);
                let barrier = Arc::clone(&barrier);
                std::thread::spawn(move || {
                    barrier.wait();
                    reg.retire_if_idle(instance)
                })
            };
            barrier.wait();
            let permit = admit.join().unwrap();
            match retire.join().unwrap().unwrap() {
                RetirementDecision::Closed(snapshot) => {
                    assert!(snapshot.is_empty());
                    assert!(matches!(permit, Err(AdmissionError::Closed)));
                }
                RetirementDecision::Busy(snapshot) => {
                    assert_eq!(snapshot.requests, 1);
                    let permit = permit.unwrap();
                    assert_eq!(reg.admission_snapshot().requests, 1);
                    permit.confirm_done().unwrap();
                    assert!(matches!(
                        reg.retire_if_idle(instance),
                        Ok(RetirementDecision::Closed(_))
                    ));
                }
                RetirementDecision::AlreadyRetiring(_) => panic!("fresh arena"),
            }
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn aborting_a_running_blocking_task_does_not_release_its_actual_work_permit() {
        let (reg, instance) = registry();
        let permit = reg.request_permit().unwrap();
        let (parked_tx, parked_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let (done_tx, done_rx) = tokio::sync::oneshot::channel();
        let task = tokio::task::spawn_blocking(move || {
            parked_tx.send(()).unwrap();
            release_rx.recv().unwrap();
            permit.confirm_done().unwrap();
            done_tx.send(()).unwrap();
        });
        parked_rx.recv().unwrap();
        task.abort();
        drop(task);
        assert_eq!(reg.admission_snapshot().requests, 1);
        assert!(matches!(
            reg.retire_if_idle(instance),
            Ok(RetirementDecision::Busy(_))
        ));
        release_tx.send(()).unwrap();
        done_rx.await.unwrap();
        assert!(matches!(
            reg.retire_if_idle(instance),
            Ok(RetirementDecision::Closed(_))
        ));
    }

    #[tokio::test]
    async fn existing_wait_slots_and_post_ack_replacement_reservations_are_not_empty() {
        let (reg, instance) = registry();
        let lease = reg
            .acquire("ch", "team", "name", None, Duration::from_secs(5))
            .await
            .unwrap();
        let id = lease.wait_id.clone();
        let (_session, guard) = lease.into_guard();
        let gate = Arc::new(tokio::sync::Notify::new());
        reg.set_post_ack_hold(Arc::clone(&gate));
        let replace = {
            let reg = Arc::clone(&reg);
            tokio::spawn(async move {
                reg.acquire("ch", "team", "name", Some(&id), Duration::from_secs(5))
                    .await
            })
        };
        // Await cancellation, which is published after the shared reservation.
        _session.cancel.cancelled().await;
        drop(guard);
        let snapshot = reg.admission_snapshot();
        assert_eq!(snapshot.slots, 1);
        assert_eq!(snapshot.replacements, 1);
        assert!(matches!(
            reg.retire_if_idle(instance),
            Ok(RetirementDecision::Busy(_))
        ));
        gate.notify_one();
        let lease = replace.await.unwrap().unwrap();
        assert_eq!(reg.admission_snapshot().replacements, 0);
        assert!(matches!(
            reg.retire_if_idle(instance),
            Ok(RetirementDecision::Busy(_))
        ));
        drop(lease.into_guard());
        assert!(matches!(
            reg.retire_if_idle(instance),
            Ok(RetirementDecision::Closed(_))
        ));
    }

    #[tokio::test]
    async fn wait_generation_exhaustion_cannot_cancel_an_old_owner_or_reuse_its_generation() {
        let (reg, _) = registry();
        let lease = reg
            .acquire("ch", "team", "name", None, Duration::from_secs(1))
            .await
            .unwrap();
        let id = lease.wait_id.clone();
        let (session, guard) = lease.into_guard();
        reg.inner.lock().unwrap().next_generation = u64::MAX;
        assert!(matches!(
            reg.acquire("ch", "team", "name", Some(&id), Duration::from_secs(1))
                .await,
            Err(CoreError::RuntimeOwnershipExhausted)
        ));
        assert!(!session.cancel.is_cancelled());
        assert!(!reg.snapshot("ch").unwrap().replacing);
        assert!(matches!(
            reg.acquire("other", "team", "other", None, Duration::from_secs(1))
                .await,
            Err(CoreError::RuntimeOwnershipExhausted)
        ));
        assert_eq!(reg.admission_snapshot().slots, 1);
        drop(guard);
        assert!(reg.admission_snapshot().is_empty());
    }
}
