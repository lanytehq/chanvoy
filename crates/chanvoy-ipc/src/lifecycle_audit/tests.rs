use super::*;
use chanvoy_core::request_ownership::{RegistrationCompletion, RequestCompletion};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::Notify;

// Test trace only: the daemon adapter separately exercises its actual arena.
#[derive(Default)]
struct Trace {
    active: AtomicUsize,
    confirmed: AtomicUsize,
    reservations: AtomicUsize,
    refusal: AtomicUsize,
}
struct Admission(Arc<Trace>);
struct Owner(Arc<Trace>);
impl RequestAdmission for Admission {
    fn reserve_request(&self) -> Result<Box<dyn RequestCompletion>, CoreError> {
        self.0.reservations.fetch_add(1, Ordering::SeqCst);
        match self.0.refusal.load(Ordering::SeqCst) {
            1 => return Err(CoreError::RuntimeAdmissionClosed),
            2 => return Err(CoreError::RuntimeOwnershipExhausted),
            3 => return Err(std::io::Error::other("unexpected admission").into()),
            _ => {}
        }
        self.0.active.fetch_add(1, Ordering::SeqCst);
        Ok(Box::new(Owner(Arc::clone(&self.0))))
    }
}
impl RequestCompletion for Owner {
    fn reserve_registration(&self) -> Result<Box<dyn RegistrationCompletion>, CoreError> {
        unreachable!("lifecycle audits must never borrow registration ownership")
    }
    fn confirm_done(self: Box<Self>) -> Result<(), CoreError> {
        self.0.active.fetch_sub(1, Ordering::SeqCst);
        self.0.confirmed.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}
fn error(message: &'static str) -> Box<dyn std::error::Error + Send + Sync> {
    std::io::Error::other(message).into()
}
struct Wire {
    trace: Arc<Trace>,
    pending: bool,
    fail: bool,
    entered: Notify,
    release: Notify,
    frames: Mutex<Vec<serde_json::Value>>,
    shutdowns: AtomicUsize,
    shutdown_fail: bool,
    destroyed: Option<Arc<AtomicBool>>,
}
impl Wire {
    fn new(trace: &Arc<Trace>, pending: bool, fail: bool) -> Self {
        Self {
            trace: Arc::clone(trace),
            pending,
            fail,
            entered: Notify::new(),
            release: Notify::new(),
            frames: Mutex::new(vec![]),
            shutdowns: AtomicUsize::new(0),
            shutdown_fail: false,
            destroyed: None,
        }
    }
}
impl RequestTransport for Wire {
    async fn send(&self, channel: u16, payload: &[u8]) -> SendResult {
        assert_eq!(channel, super::super::CHANNEL_3);
        assert!(
            self.trace.active.load(Ordering::SeqCst) > 0,
            "audit output without owner"
        );
        if self.pending {
            self.entered.notify_one();
            self.release.notified().await;
            assert!(
                self.trace.active.load(Ordering::SeqCst) > 0,
                "confirmed pending audit"
            );
        }
        if self.fail {
            return Err(error("audit send failed"));
        }
        self.frames
            .lock()
            .unwrap()
            .push(serde_json::from_slice(payload)?);
        Ok(())
    }
}
impl SessionTransport for Wire {
    async fn shutdown(&self) -> SendResult {
        if let Some(destroyed) = &self.destroyed {
            assert!(
                destroyed.load(Ordering::SeqCst),
                "shutdown before actual forwarder collection"
            );
        }
        self.shutdowns.fetch_add(1, Ordering::SeqCst);
        if self.shutdown_fail {
            Err(error("shutdown failed"))
        } else {
            Ok(())
        }
    }
}
async fn signaled(notify: &Notify) {
    tokio::time::timeout(Duration::from_secs(3), notify.notified())
        .await
        .unwrap();
}
fn counts(trace: &Trace) -> (usize, usize) {
    (
        trace.active.load(Ordering::SeqCst),
        trace.confirmed.load(Ordering::SeqCst),
    )
}

#[tokio::test]
async fn both_lifecycle_audits_hold_independent_owner_until_send_completes() {
    for action in ["chat.peer_connected", "chat.peer_disconnected"] {
        let trace = Arc::new(Trace::default());
        let wire = Arc::new(Wire::new(&trace, true, false));
        let admission = Admission(Arc::clone(&trace));
        let existing = admission.reserve_request().unwrap();
        let tx = Arc::clone(&wire);
        let task =
            tokio::spawn(async move { emit(&admission, tx.as_ref(), "fixture", action).await });
        signaled(&wire.entered).await;
        assert_eq!(
            counts(&trace),
            (2, 0),
            "audit must have an independent owner"
        );
        assert!(!task.is_finished());
        wire.release.notify_one();
        assert_eq!(task.await.unwrap().unwrap(), Selection::Confirmed);
        assert_eq!(counts(&trace), (1, 1));
        let frames = wire.frames.lock().unwrap();
        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0]["action"], action);
        drop(frames);
        existing.confirm_done().unwrap();
        assert_eq!(counts(&trace), (0, 2));
    }
}

#[tokio::test]
async fn failed_and_dropped_audits_never_confirm() {
    for action in ["chat.peer_connected", "chat.peer_disconnected"] {
        for dropped in [false, true] {
            let trace = Arc::new(Trace::default());
            let wire = Arc::new(Wire::new(&trace, true, true));
            let admission = Admission(Arc::clone(&trace));
            let tx = Arc::clone(&wire);
            let task =
                tokio::spawn(async move { emit(&admission, tx.as_ref(), "fixture", action).await });
            signaled(&wire.entered).await;
            assert_eq!(counts(&trace), (1, 0));
            if dropped {
                task.abort();
                assert!(task.await.unwrap_err().is_cancelled());
            } else {
                wire.release.notify_one();
                assert_eq!(
                    task.await.unwrap().unwrap_err().to_string(),
                    "audit send failed"
                );
            }
            assert_eq!(
                counts(&trace),
                (1, 0),
                "failed or dropped audit must remain busy"
            );
            assert!(wire.frames.lock().unwrap().is_empty());
        }
    }
}

#[tokio::test]
async fn closed_omits_construction_but_exhausted_and_unexpected_remain_errors() {
    for refusal in [1, 2, 3] {
        let trace = Arc::new(Trace::default());
        trace.refusal.store(refusal, Ordering::SeqCst);
        let constructed = AtomicUsize::new(0);
        let result = selected(&Admission(Arc::clone(&trace)), || {
            constructed.fetch_add(1, Ordering::SeqCst);
            async { panic!("refused audit constructed output") }
        })
        .await;
        if refusal == 1 {
            assert_eq!(result.unwrap(), Selection::Closed);
        } else {
            let err = result.unwrap_err();
            if refusal == 2 {
                assert!(matches!(
                    err.downcast_ref::<CoreError>(),
                    Some(CoreError::RuntimeOwnershipExhausted)
                ));
            } else {
                assert_eq!(err.to_string(), "io error: unexpected admission");
            }
        }
        assert_eq!(constructed.load(Ordering::SeqCst), 0);
        assert_eq!(counts(&trace), (0, 0));
        assert_eq!(trace.reservations.load(Ordering::SeqCst), 1);
        let wire = Wire::new(&trace, false, false);
        for action in ["chat.peer_connected", "chat.peer_disconnected"] {
            let result = emit(&Admission(Arc::clone(&trace)), &wire, "fixture", action).await;
            assert_eq!(result.is_ok(), refusal == 1);
        }
        assert!(wire.frames.lock().unwrap().is_empty());
        assert_eq!(counts(&trace), (0, 0));
    }
}

#[tokio::test]
async fn connected_failure_skips_request_processing_and_keeps_first_error() {
    let trace = Arc::new(Trace::default());
    let mut wire = Wire::new(&trace, false, true);
    wire.shutdown_fail = true;
    let served = AtomicBool::new(false);
    let result = session(
        &Admission(Arc::clone(&trace)),
        &wire,
        "fixture",
        tokio::spawn(async {}),
        async {
            served.store(true, Ordering::SeqCst);
            Ok(())
        },
    )
    .await;
    assert_eq!(result.unwrap_err().to_string(), "audit send failed");
    assert!(!served.load(Ordering::SeqCst));
    assert_eq!(counts(&trace), (2, 0));
    assert_eq!(wire.shutdowns.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn closed_audits_are_omissions_while_session_processing_and_cleanup_continue() {
    let trace = Arc::new(Trace::default());
    trace.refusal.store(1, Ordering::SeqCst);
    let wire = Wire::new(&trace, false, false);
    let served = AtomicBool::new(false);
    session(
        &Admission(Arc::clone(&trace)),
        &wire,
        "fixture",
        tokio::spawn(async {}),
        async {
            served.store(true, Ordering::SeqCst);
            Ok(())
        },
    )
    .await
    .unwrap();
    assert!(served.load(Ordering::SeqCst));
    assert_eq!(trace.reservations.load(Ordering::SeqCst), 2);
    assert_eq!(counts(&trace), (0, 0));
    assert!(wire.frames.lock().unwrap().is_empty());
    assert_eq!(wire.shutdowns.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn finish_preserves_error_precedence_and_always_attempts_shutdown() {
    for prior in [false, true] {
        for fail_audit in [false, true] {
            let trace = Arc::new(Trace::default());
            let mut wire = Wire::new(&trace, false, fail_audit);
            wire.shutdown_fail = true;
            let result = finish_session(
                &Admission(Arc::clone(&trace)),
                &wire,
                "fixture",
                tokio::spawn(async {}),
                if prior {
                    Err(error("first session error"))
                } else {
                    Ok(())
                },
            )
            .await;
            assert_eq!(
                result.unwrap_err().to_string(),
                if prior {
                    "first session error"
                } else if fail_audit {
                    "audit send failed"
                } else {
                    "shutdown failed"
                }
            );
            assert_eq!(wire.shutdowns.load(Ordering::SeqCst), 1);
            assert_eq!(counts(&trace), if fail_audit { (1, 0) } else { (0, 1) });
        }
    }
}

#[tokio::test]
async fn forwarder_panic_remains_error_and_still_shuts_down() {
    let trace = Arc::new(Trace::default());
    let wire = Wire::new(&trace, false, false);
    let forwarder = tokio::spawn(async { panic!("controlled forwarder panic") });
    while !forwarder.is_finished() {
        tokio::task::yield_now().await;
    }
    let result = finish_session(
        &Admission(Arc::clone(&trace)),
        &wire,
        "fixture",
        forwarder,
        Ok(()),
    )
    .await;
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("controlled forwarder panic"));
    assert_eq!(wire.shutdowns.load(Ordering::SeqCst), 1);
    assert_eq!(counts(&trace), (0, 1));
}

// A bounded synchronous destructor lets the test distinguish abort request
// from actual destruction/join. The timeout prevents a failed assertion from
// stranding a runtime worker indefinitely.
struct BlockDrop {
    entered: Arc<Notify>,
    release: Arc<(Mutex<bool>, std::sync::Condvar)>,
    destroyed: Arc<AtomicBool>,
}
impl Drop for BlockDrop {
    fn drop(&mut self) {
        self.entered.notify_one();
        let (lock, cv) = self.release.as_ref();
        let (allowed, _) = cv
            .wait_timeout_while(lock.lock().unwrap(), Duration::from_secs(5), |allowed| {
                !*allowed
            })
            .unwrap();
        assert!(*allowed, "test failed to release old forwarder destruction");
        self.destroyed.store(true, Ordering::SeqCst);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cleanup_waits_for_actual_old_forwarder_join_before_shutdown_even_on_errors() {
    let trace = Arc::new(Trace::default());
    let destroyed = Arc::new(AtomicBool::new(false));
    let entered = Arc::new(Notify::new());
    let ready = Arc::new(Notify::new());
    let release = Arc::new((Mutex::new(false), std::sync::Condvar::new()));
    let guard = BlockDrop {
        entered: Arc::clone(&entered),
        release: Arc::clone(&release),
        destroyed: Arc::clone(&destroyed),
    };
    let started = Arc::clone(&ready);
    let forwarder = tokio::spawn(async move {
        let _guard = guard;
        started.notify_one();
        std::future::pending::<()>().await;
    });
    signaled(&ready).await;
    let mut wire = Wire::new(&trace, false, true);
    wire.destroyed = Some(Arc::clone(&destroyed));
    let wire = Arc::new(wire);
    let tx = Arc::clone(&wire);
    let admission = Admission(Arc::clone(&trace));
    let finish = tokio::spawn(async move {
        finish_session(
            &admission,
            tx.as_ref(),
            "fixture",
            forwarder,
            Err(error("original connected error")),
        )
        .await
    });
    signaled(&entered).await;
    assert!(!destroyed.load(Ordering::SeqCst));
    assert!(!finish.is_finished(), "abort alone is not collection");
    assert_eq!(wire.shutdowns.load(Ordering::SeqCst), 0);
    *release.0.lock().unwrap() = true;
    release.1.notify_one();
    let result = tokio::time::timeout(Duration::from_secs(3), finish)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(result.unwrap_err().to_string(), "original connected error");
    assert!(destroyed.load(Ordering::SeqCst));
    assert_eq!(wire.shutdowns.load(Ordering::SeqCst), 1);
    assert_eq!(counts(&trace), (1, 0));
}
