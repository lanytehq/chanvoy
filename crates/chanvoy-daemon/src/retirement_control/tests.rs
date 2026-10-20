use super::*;
use chanvoy_core::runtime::{InodeIdentity, NativeBirth, PathBytes, ProcessId, RuntimeSocket};
use serde_json::json;
use std::{
    path::Path,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Barrier,
    },
};

#[derive(Clone)]
struct FakePublication {
    state: Arc<Mutex<PublicationView>>,
    writes: Arc<AtomicUsize>,
    failure: Arc<AtomicUsize>,
    registry: Arc<WaitOwnerRegistry>,
}
impl Publication for FakePublication {
    fn inspect(&self) -> Result<PublicationView, RuntimeError> {
        Ok(self.state.lock().unwrap().clone())
    }
    fn begin(&self) -> Result<(), RuntimeError> {
        // Actual arena operations here would deadlock if its mutex crossed I/O.
        assert_eq!(
            self.registry.admission_snapshot().phase,
            AdmissionPhase::Retiring
        );
        assert!(self.registry.request_permit().is_err());
        self.writes.fetch_add(1, Ordering::SeqCst);
        let mut s = self.state.lock().unwrap();
        assert_eq!(s.logical.phase, RuntimePhase::Running);
        if self.failure.load(Ordering::SeqCst) == 3 {
            return Err(RuntimeError::Unavailable("stage failed"));
        }
        s.logical.revision = s
            .logical
            .revision
            .checked_add(1)
            .ok_or(RuntimeError::Invalid("exhausted"))?;
        s.logical.phase = RuntimePhase::Retiring;
        s.logical.ready = false;
        s.confirmed = false;
        if self.failure.load(Ordering::SeqCst) == 1 {
            return Err(RuntimeError::PublicationUncertain);
        }
        s.observed = s.logical.clone();
        if self.failure.load(Ordering::SeqCst) == 2 {
            return Err(RuntimeError::PublicationUncertain);
        }
        s.confirmed = true;
        Ok(())
    }
    fn retry(&self) -> Result<(), RuntimeError> {
        assert_eq!(
            self.registry.admission_snapshot().phase,
            AdmissionPhase::Retiring
        );
        self.writes.fetch_add(1, Ordering::SeqCst);
        let mut s = self.state.lock().unwrap();
        assert_eq!(s.logical.phase, RuntimePhase::Retiring);
        if self.failure.load(Ordering::SeqCst) != 0 {
            return Err(RuntimeError::PublicationUncertain);
        }
        s.observed = s.logical.clone();
        s.confirmed = true;
        Ok(())
    }
}
fn setup() -> (RetirementControl<FakePublication>, FakePublication) {
    let instance = InstanceId::new();
    let registry = Arc::new(WaitOwnerRegistry::for_instance(instance));
    let config_root = PathBytes::from_path(Path::new("/owned/config")).unwrap();
    let record = RuntimeRecord {
        schema_version: 1,
        namespace: Namespace::derive(501, &config_root, "profile").unwrap(),
        instance_uuid: instance,
        revision: 2,
        phase: RuntimePhase::Running,
        uid: 501,
        config_root,
        profile: "profile".into(),
        ready: true,
        process: Some(RuntimeProcess {
            pid: ProcessId::new(123).unwrap(),
            uid: 501,
            birth: NativeBirth::MacOs {
                seconds: 1,
                microseconds: 2,
            },
            executable: PathBytes::from_path(Path::new("/owned/chanvoy")).unwrap(),
            pid_namespace: None,
            mount_namespace: None,
        }),
        socket: Some(RuntimeSocket {
            path: PathBytes::from_path(Path::new("/owned/r/123.sock")).unwrap(),
            identity: InodeIdentity {
                device: 1,
                inode: 3,
            },
        }),
        build: Some(RuntimeBuildPin {
            version: "0.3.3".into(),
            commit: "a".repeat(40),
            dirty: Some(false),
        }),
    };
    let publisher = FakePublication {
        state: Arc::new(Mutex::new(PublicationView {
            observed: record.clone(),
            logical: record,
            confirmed: true,
        })),
        writes: Arc::new(AtomicUsize::new(0)),
        failure: Arc::new(AtomicUsize::new(0)),
        registry: Arc::clone(&registry),
    };
    (
        RetirementControl::bind(registry, publisher.clone()).unwrap(),
        publisher,
    )
}
fn request(
    p: &FakePublication,
    action: &str,
    change: impl FnOnce(&mut serde_json::Value),
) -> ControlDocument {
    let s = p.state.lock().unwrap();
    let r = &s.observed;
    let mut value = json!({
        "jsonrpc": "2.0", "id": "87654321-4321-4321-8321-cba987654321", "method": "runtime_retire_v1",
        "params": {"schema_version": 1, "kind": "retirement_params", "action": action,
            "target": {"namespace": r.namespace, "instance_uuid": r.instance_uuid, "record_revision": r.revision, "process": r.process},
            "caller_generation": r.build}
    });
    change(&mut value);
    ControlDocument::decode(
        ControlRoot::RetireRequest,
        &serde_json::to_vec(&value).unwrap(),
    )
    .unwrap()
}
fn attempt(
    c: &RetirementControl<FakePublication>,
    p: &FakePublication,
    action: &str,
) -> ResultFacts {
    c.attempt(&request(p, action, |_| {}), |_| Ok(())).unwrap()
}

#[test]
fn busy_uses_shared_arena_and_publication_observes_closed_admission() {
    let (c, p) = setup();
    let permit = p.registry.request_permit().unwrap();
    assert_eq!(attempt(&c, &p, "begin").outcome, Outcome::Busy);
    assert_eq!(p.writes.load(Ordering::SeqCst), 0);
    permit.confirm_done().unwrap();
    let result = attempt(&c, &p, "begin");
    assert_eq!(result.outcome, Outcome::Committed);
    assert_eq!(result.arena.unwrap().phase, AdmissionPhase::Retiring);
    assert_eq!(result.publication.unwrap().observed.revision, 3);
    assert_eq!(attempt(&c, &p, "begin").outcome, Outcome::AlreadyRetiring);
    assert_eq!(p.writes.load(Ordering::SeqCst), 1);
    assert_eq!(
        attempt(&c, &p, "continue_same_epoch").outcome,
        Outcome::Continued
    );
    assert_eq!(p.state.lock().unwrap().observed.revision, 3);
}

#[test]
fn every_publication_failure_retains_closure_and_retry_is_forward_only() {
    for mode in 1..=3 {
        let (c, p) = setup();
        p.failure.store(mode, Ordering::SeqCst);
        assert_eq!(
            attempt(&c, &p, "begin").outcome,
            Outcome::PublicationUnconfirmed
        );
        assert_eq!(
            p.registry.admission_snapshot().phase,
            AdmissionPhase::Retiring
        );
        assert!(p.registry.request_permit().is_err());
        assert_eq!(attempt(&c, &p, "begin").outcome, Outcome::AlreadyRetiring);
        assert_eq!(
            attempt(&c, &p, "continue_same_epoch").outcome,
            Outcome::PublicationUnconfirmed
        );
        p.failure.store(0, Ordering::SeqCst);
        assert_eq!(
            attempt(&c, &p, "continue_same_epoch").outcome,
            Outcome::Continued
        );
        assert_eq!(p.state.lock().unwrap().observed.revision, 3);
        assert_eq!(
            p.state.lock().unwrap().observed.instance_uuid,
            c.anchor.instance_uuid
        );
    }
}

#[test]
fn stale_continuation_does_not_retry_or_clear_retiring_facts() {
    let (c, p) = setup();
    p.failure.store(2, Ordering::SeqCst);
    let old = request(&p, "continue_same_epoch", |_| {});
    assert_eq!(
        attempt(&c, &p, "begin").outcome,
        Outcome::PublicationUnconfirmed
    );
    p.failure.store(0, Ordering::SeqCst);
    let result = c.attempt(&old, |_| Ok(())).unwrap();
    assert_eq!(result.outcome, Outcome::RevisionChanged);
    assert_eq!(result.arena.unwrap().phase, AdmissionPhase::Retiring);
    assert!(!result.publication.unwrap().confirmed);
    assert_eq!(p.writes.load(Ordering::SeqCst), 1);
    assert_eq!(
        attempt(&c, &p, "continue_same_epoch").outcome,
        Outcome::Continued
    );
}

#[test]
fn mismatched_claims_and_missing_native_binding_never_close_or_publish() {
    for case in 0..5 {
        let (c, p) = setup();
        let req = request(&p, "begin", |v| match case {
            0 => v["params"]["target"]["instance_uuid"] = json!(InstanceId::new()),
            1 => v["params"]["target"]["process"]["birth"]["seconds"] = json!(9),
            2 => v["params"]["target"]["record_revision"] = json!(1),
            3 => v["params"]["caller_generation"]["dirty"] = json!(true),
            _ => {}
        });
        let result = c
            .attempt(&req, |_| Err(RuntimeError::Unavailable("native evidence")))
            .unwrap();
        assert_eq!(
            result.outcome,
            [
                Outcome::TargetMismatch,
                Outcome::TargetMismatch,
                Outcome::RevisionChanged,
                Outcome::GenerationRefused,
                Outcome::NativeUnavailable
            ][case]
        );
        assert_eq!(
            p.registry.admission_snapshot().phase,
            AdmissionPhase::Running
        );
        assert_eq!(p.writes.load(Ordering::SeqCst), 0);
        if case < 2 {
            assert!(result.arena.is_none());
            assert!(result.publication.is_none());
        }
    }
    let (c, p) = setup();
    assert_eq!(
        attempt(&c, &p, "continue_same_epoch").outcome,
        Outcome::NotRetiring
    );
    assert_eq!(p.writes.load(Ordering::SeqCst), 0);
}

#[test]
fn admission_during_native_recheck_is_seen_at_atomic_retirement() {
    let (c, p) = setup();
    let mut owner = None;
    let result = c
        .attempt(&request(&p, "begin", |_| {}), |_| {
            owner = Some(p.registry.request_permit().unwrap());
            Ok(())
        })
        .unwrap();
    assert_eq!(result.outcome, Outcome::Busy);
    assert_eq!(p.writes.load(Ordering::SeqCst), 0);
    owner.unwrap().confirm_done().unwrap();
    assert_eq!(attempt(&c, &p, "begin").outcome, Outcome::Committed);
}

#[test]
fn concurrent_begins_have_exactly_one_commit_and_one_publication() {
    let (c, p) = setup();
    let c = Arc::new(c);
    let gate = Arc::new(Barrier::new(3));
    let threads: Vec<_> = (0..2)
        .map(|_| {
            let c = Arc::clone(&c);
            let p = p.clone();
            let gate = Arc::clone(&gate);
            std::thread::spawn(move || {
                gate.wait();
                attempt(&c, &p, "begin").outcome
            })
        })
        .collect();
    gate.wait();
    let outcomes: Vec<_> = threads.into_iter().map(|t| t.join().unwrap()).collect();
    assert_eq!(
        outcomes
            .iter()
            .filter(|o| **o == Outcome::Committed)
            .count(),
        1
    );
    assert!(outcomes
        .iter()
        .any(|o| matches!(o, Outcome::AlreadyRetiring | Outcome::RevisionChanged)));
    assert_eq!(p.writes.load(Ordering::SeqCst), 1);
}

#[test]
fn changed_publisher_identity_cannot_be_adopted() {
    let (c, p) = setup();
    {
        let mut state = p.state.lock().unwrap();
        state.observed.instance_uuid = InstanceId::new();
        state.logical = state.observed.clone();
    }
    assert_eq!(attempt(&c, &p, "begin").outcome, Outcome::TargetMismatch);
    assert_eq!(
        p.registry.admission_snapshot().phase,
        AdmissionPhase::Running
    );
    assert_eq!(p.writes.load(Ordering::SeqCst), 0);
}
