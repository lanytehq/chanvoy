use super::*;
use crate::ipc_ownership::IpcAdmission;
use crate::wait_owner::{RetirementDecision, WaitOwnerRegistry};
use chanvoy_core::request_ownership::RequestAdmission;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;
use tokio::sync::Notify;

fn failure(message: &'static str) -> DaemonError {
    std::io::Error::other(message).into()
}
async fn notified(signal: &Notify) {
    tokio::time::timeout(Duration::from_secs(3), signal.notified())
        .await
        .unwrap();
}
async fn joined<T>(handle: tokio::task::JoinHandle<T>) -> T {
    tokio::time::timeout(Duration::from_secs(3), handle)
        .await
        .unwrap()
        .unwrap()
}
struct Exit(Arc<AtomicBool>);
impl Drop for Exit {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst);
    }
}

#[tokio::test]
async fn idle_reconnect_infrastructure_does_not_keep_the_arena_busy() {
    let arena = Arc::new(WaitOwnerRegistry::new());
    let parent = CancellationToken::new();
    let exited = Arc::new(AtomicBool::new(false));
    let flag = Arc::clone(&exited);
    let task = IpcTask::spawn(&parent, move |cancel| async move {
        let _exit = Exit(flag);
        cancel.cancelled().await;
    });
    let before = arena.admission_snapshot();
    assert_eq!((before.requests, before.streams, before.cleanup), (0, 0, 0));
    assert!(matches!(
        arena.retire_if_idle(before.instance),
        Ok(RetirementDecision::Closed(_))
    ));
    let cleaned = AtomicBool::new(false);
    let value = task
        .finish(Ok(7), || {
            assert!(
                exited.load(Ordering::SeqCst),
                "cleanup before actual IPC join"
            );
            cleaned.store(true, Ordering::SeqCst);
            Ok(())
        })
        .await
        .unwrap();
    assert_eq!(value, 7);
    assert!(cleaned.load(Ordering::SeqCst));
    assert!(
        !parent.is_cancelled(),
        "child shutdown must not cancel daemon siblings"
    );
}

#[tokio::test]
async fn actual_request_ownership_survives_cancel_until_work_and_join_complete() {
    let arena = Arc::new(WaitOwnerRegistry::new());
    let owner = IpcAdmission(Arc::clone(&arena)).reserve_request().unwrap();
    let ready = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let seen = Arc::clone(&ready);
    let go = Arc::clone(&release);
    let parent = CancellationToken::new();
    let task = IpcTask::spawn(&parent, move |cancel| async move {
        cancel.cancelled().await;
        seen.notify_one();
        go.notified().await;
        owner.confirm_done().unwrap();
    });
    let cleaned = Arc::new(AtomicBool::new(false));
    let mark = Arc::clone(&cleaned);
    let finish = tokio::spawn(task.finish(Ok(()), move || {
        mark.store(true, Ordering::SeqCst);
        Ok(())
    }));
    notified(&ready).await;
    let pending = arena.admission_snapshot();
    assert_eq!(pending.requests, 1);
    assert!(matches!(
        arena.retire_if_idle(pending.instance),
        Ok(RetirementDecision::Busy(_))
    ));
    assert!(
        !finish.is_finished(),
        "cancel request is not actual completion"
    );
    assert!(!cleaned.load(Ordering::SeqCst));
    release.notify_one();
    joined(finish).await.unwrap();
    assert!(cleaned.load(Ordering::SeqCst));
    let after = arena.admission_snapshot();
    assert_eq!(after.requests, 0);
    assert!(matches!(
        arena.retire_if_idle(after.instance),
        Ok(RetirementDecision::Closed(_))
    ));
}

#[tokio::test]
async fn dropped_supervisor_signals_cancel_but_cannot_confirm_an_active_request() {
    let arena = Arc::new(WaitOwnerRegistry::new());
    let owner = IpcAdmission(Arc::clone(&arena)).reserve_request().unwrap();
    let ready = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let done = Arc::new(Notify::new());
    let seen = Arc::clone(&ready);
    let go = Arc::clone(&release);
    let exited = Arc::clone(&done);
    let parent = CancellationToken::new();
    let task = IpcTask::spawn(&parent, move |cancel| async move {
        cancel.cancelled().await;
        seen.notify_one();
        go.notified().await;
        owner.confirm_done().unwrap();
        exited.notify_one();
    });
    drop(task);
    notified(&ready).await;
    assert_eq!(arena.admission_snapshot().requests, 1);
    assert!(!parent.is_cancelled());
    release.notify_one();
    notified(&done).await;
    assert_eq!(arena.admission_snapshot().requests, 0);
}

#[tokio::test]
async fn daemon_error_is_preserved_only_after_actual_join_and_does_not_clean_artifacts() {
    let ready = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let seen = Arc::clone(&ready);
    let go = Arc::clone(&release);
    let task = IpcTask::spawn(&CancellationToken::new(), move |cancel| async move {
        cancel.cancelled().await;
        seen.notify_one();
        go.notified().await;
    });
    let cleaned = Arc::new(AtomicBool::new(false));
    let mark = Arc::clone(&cleaned);
    let finish = tokio::spawn(task.finish::<()>(
        Err(failure("original daemon error")),
        move || {
            mark.store(true, Ordering::SeqCst);
            Ok(())
        },
    ));
    notified(&ready).await;
    assert!(
        !finish.is_finished(),
        "daemon error bypassed actual IPC join"
    );
    release.notify_one();
    assert!(joined(finish)
        .await
        .unwrap_err()
        .to_string()
        .contains("original daemon error"));
    assert!(!cleaned.load(Ordering::SeqCst));
}

#[tokio::test]
async fn a_panicked_child_retains_request_ownership_and_prevents_artifact_cleanup() {
    for prior_error in [false, true] {
        let arena = Arc::new(WaitOwnerRegistry::new());
        let owner = IpcAdmission(Arc::clone(&arena)).reserve_request().unwrap();
        let task = IpcTask::spawn(&CancellationToken::new(), move |_| async move {
            let _owner = owner;
            panic!("controlled IPC child panic");
        });
        let cleaned = AtomicBool::new(false);
        let result = task
            .finish(
                if prior_error {
                    Err(failure("earlier daemon error"))
                } else {
                    Ok(())
                },
                || {
                    cleaned.store(true, Ordering::SeqCst);
                    Ok(())
                },
            )
            .await;
        let message = result.unwrap_err().to_string();
        assert!(message.contains(if prior_error {
            "earlier daemon error"
        } else {
            "controlled IPC child panic"
        }));
        assert!(!cleaned.load(Ordering::SeqCst));
        let state = arena.admission_snapshot();
        assert_eq!(state.requests, 1);
        assert!(matches!(
            arena.retire_if_idle(state.instance),
            Ok(RetirementDecision::Busy(_))
        ));
    }
}

#[tokio::test]
async fn clean_infrastructure_exit_does_not_release_retained_registration_or_other_request() {
    let arena = Arc::new(WaitOwnerRegistry::new());
    let admission = IpcAdmission(Arc::clone(&arena));
    let initial = admission.reserve_request().unwrap();
    let registration = initial.reserve_registration().unwrap();
    initial.confirm_done().unwrap();
    let other = admission.reserve_request().unwrap();
    let task = IpcTask::spawn(&CancellationToken::new(), |cancel| async move {
        cancel.cancelled().await;
    });
    task.finish(Ok(()), || Ok(())).await.unwrap();
    let state = arena.admission_snapshot();
    assert_eq!((state.requests, state.streams), (1, 1));
    assert!(matches!(
        arena.retire_if_idle(state.instance),
        Ok(RetirementDecision::Busy(_))
    ));
    registration.confirm_done().unwrap();
    assert_eq!(arena.admission_snapshot().requests, 1);
    other.confirm_done().unwrap();
}

struct BlockDrop {
    entered: Arc<Notify>,
    release: Arc<(Mutex<bool>, Condvar)>,
    destroyed: Arc<AtomicBool>,
}
impl Drop for BlockDrop {
    fn drop(&mut self) {
        self.entered.notify_one();
        let (lock, cv) = self.release.as_ref();
        let (allowed, _) = cv
            .wait_timeout_while(lock.lock().unwrap(), Duration::from_secs(5), |v| !*v)
            .unwrap();
        assert!(*allowed, "old IPC destructor not released");
        self.destroyed.store(true, Ordering::SeqCst);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn runtime_cleanup_waits_for_actual_child_destruction_and_collection() {
    let entered = Arc::new(Notify::new());
    let ready = Arc::new(Notify::new());
    let release = Arc::new((Mutex::new(false), Condvar::new()));
    let destroyed = Arc::new(AtomicBool::new(false));
    let guard = BlockDrop {
        entered: Arc::clone(&entered),
        release: Arc::clone(&release),
        destroyed: Arc::clone(&destroyed),
    };
    let seen = Arc::clone(&ready);
    let task = IpcTask::spawn(&CancellationToken::new(), move |cancel| async move {
        let _guard = guard;
        seen.notify_one();
        cancel.cancelled().await;
    });
    notified(&ready).await;
    let cleaned = Arc::new(AtomicUsize::new(0));
    let count = Arc::clone(&cleaned);
    let exited = Arc::clone(&destroyed);
    let finish = tokio::spawn(task.finish(Ok(()), move || {
        assert!(
            exited.load(Ordering::SeqCst),
            "runtime cleanup ran before actual join"
        );
        count.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }));
    notified(&entered).await;
    assert!(
        !finish.is_finished(),
        "IPC cancellation was mistaken for completed join"
    );
    assert_eq!(cleaned.load(Ordering::SeqCst), 0);
    *release.0.lock().unwrap() = true;
    release.1.notify_one();
    joined(finish).await.unwrap();
    assert_eq!(cleaned.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn cleanup_failure_stays_an_error_after_successful_collection() {
    let task = IpcTask::spawn(&CancellationToken::new(), |_| async {});
    let result = task
        .finish(Ok(()), || Err(failure("runtime cleanup failed")))
        .await;
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("runtime cleanup failed"));
}

#[tokio::test]
async fn actual_ipc_reconnect_backoff_stays_idle_and_is_collected_on_shutdown() {
    use chanvoy_core::{
        CapabilityClass, CredentialMode, EventBus, MattermostClient, Profile, Provider,
    };
    let arena = Arc::new(WaitOwnerRegistry::new());
    let socket = format!("/tmp/chanvoy-ipc-{}.sock", uuid::Uuid::new_v4());
    assert_eq!(
        std::fs::symlink_metadata(&socket).unwrap_err().kind(),
        std::io::ErrorKind::NotFound
    );
    let profile = Profile {
        name: "ipc-supervision-fixture".into(),
        role: "devlead".into(),
        scope: "test".into(),
        provider: Provider::Mattermost,
        bot_username: "fixture-bot".into(),
        team_name: "fixture-team".into(),
        server_url: "http://127.0.0.1:9".into(),
        env_name: "FIXTURE_TOKEN".into(),
        env_file: None,
        credential_mode: CredentialMode::EnvName,
        capability_class: CapabilityClass::Standard,
        monitored_channels: vec![],
        ipc: None,
        reduce: None,
    };
    let client = MattermostClient::new(&profile, "test-token-ipc-supervision".into()).unwrap();
    let peer = Arc::new(chanvoy_ipc::IpcPeer::new(
        &profile,
        client,
        Arc::new(EventBus::new(16)),
        socket,
        Arc::new(AtomicBool::new(false)),
        Arc::new(IpcAdmission(Arc::clone(&arena))),
    ));
    let state = peer.state();
    let parent = CancellationToken::new();
    let task = IpcTask::spawn(&parent, move |cancel| peer.run(cancel));
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if state.lock().await.reconnect_count > 0 {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let before = arena.admission_snapshot();
    assert_eq!((before.requests, before.streams, before.cleanup), (0, 0, 0));
    assert!(matches!(
        arena.retire_if_idle(before.instance),
        Ok(RetirementDecision::Closed(_))
    ));
    tokio::time::timeout(Duration::from_secs(3), task.finish(Ok(()), || Ok(())))
        .await
        .unwrap()
        .unwrap();
    assert!(!state.lock().await.connected);
    assert!(!parent.is_cancelled());
}
