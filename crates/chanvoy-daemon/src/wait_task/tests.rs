use super::*;
use crate::wait_owner::RetirementDecision;
use std::sync::{Condvar, Mutex};
use std::time::Duration;
use tokio::sync::{oneshot, Notify};

fn registry() -> Arc<WaitOwnerRegistry> {
    Arc::new(WaitOwnerRegistry::new())
}

fn busy(registry: &WaitOwnerRegistry, expected: usize) {
    let snapshot = registry.admission_snapshot();
    assert_eq!(snapshot.cleanup, expected);
    assert!(matches!(
        registry.retire_if_idle(snapshot.instance),
        Ok(RetirementDecision::Busy(_))
    ));
}

async fn notified(signal: &Notify) {
    tokio::time::timeout(Duration::from_secs(2), signal.notified())
        .await
        .unwrap();
}

async fn collect(mut handle: tokio::task::JoinHandle<()>) {
    match tokio::time::timeout(Duration::from_secs(2), &mut handle).await {
        Ok(result) => result.unwrap(),
        Err(_) => {
            handle.abort();
            let _ = handle.await;
            panic!("collector did not finish");
        }
    }
}

#[tokio::test]
async fn collector_alone_stays_busy_after_child_join_until_its_final_action() {
    let registry = registry();
    let request = registry.request_permit().unwrap();
    let joined = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let (result, receipt) = oneshot::channel();
    let mut task = OwnedTask::spawn_inner(
        &registry,
        async {},
        CollectorHooks {
            joined: Some(Arc::clone(&joined)),
            release: Some(Arc::clone(&release)),
            result: Some(result),
        },
    )
    .unwrap();
    let collector = task.collector.take().unwrap();
    notified(&joined).await;
    request.confirm_done().unwrap();
    busy(&registry, 1);
    drop(task); // An abort request against the terminated child changes nothing.
    busy(&registry, 1);
    release.notify_one();
    collect(collector).await;
    assert_eq!(receipt.await.unwrap(), Terminal::Completed);
    let snapshot = registry.admission_snapshot();
    assert_eq!(snapshot.cleanup, 0);
    assert!(matches!(
        registry.retire_if_idle(snapshot.instance),
        Ok(RetirementDecision::Closed(_))
    ));
}

struct DestructionGate {
    entered: Arc<Notify>,
    gate: Arc<(Mutex<bool>, Condvar)>,
}
impl Drop for DestructionGate {
    fn drop(&mut self) {
        self.entered.notify_one();
        let (lock, changed) = &*self.gate;
        let mut released = lock.lock().unwrap();
        while !*released {
            let (next, timeout) = changed
                .wait_timeout(released, Duration::from_secs(5))
                .unwrap();
            released = next;
            if timeout.timed_out() {
                break;
            } // Bounded teardown on a failed test.
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn abort_and_parent_drop_stay_busy_through_actual_child_destruction() {
    let registry = registry();
    let started = Arc::new(Notify::new());
    let entered = Arc::new(Notify::new());
    let gate = Arc::new((Mutex::new(false), Condvar::new()));
    let child_started = Arc::clone(&started);
    let guard = DestructionGate {
        entered: Arc::clone(&entered),
        gate: Arc::clone(&gate),
    };
    let (result, receipt) = oneshot::channel();
    let mut task = OwnedTask::spawn_inner(
        &registry,
        async move {
            let _guard = guard;
            child_started.notify_one();
            std::future::pending::<()>().await;
        },
        CollectorHooks {
            result: Some(result),
            ..Default::default()
        },
    )
    .unwrap();
    let collector = task.collector.take().unwrap();
    notified(&started).await;
    busy(&registry, 2);
    drop(task);
    notified(&entered).await;
    // Gather the assertion's evidence while destruction is paused, then always
    // unblock the worker before asserting so a failure cannot hang the runtime.
    let snapshot = registry.admission_snapshot();
    let decision = registry.retire_if_idle(snapshot.instance);
    {
        let (lock, changed) = &*gate;
        *lock.lock().unwrap() = true;
        changed.notify_all();
    }
    collect(collector).await;
    assert_eq!(snapshot.cleanup, 2);
    assert!(matches!(decision, Ok(RetirementDecision::Busy(_))));
    assert_eq!(receipt.await.unwrap(), Terminal::Canceled);
    assert_eq!(registry.admission_snapshot().cleanup, 0);
}

struct Dropped(Arc<Notify>);
impl Drop for Dropped {
    fn drop(&mut self) {
        self.0.notify_one();
    }
}

#[tokio::test]
async fn lost_collector_cannot_clear_child_or_collector_ownership() {
    let registry = registry();
    let started = Arc::new(Notify::new());
    let gone = Arc::new(Notify::new());
    let child_started = Arc::clone(&started);
    let guard = Dropped(Arc::clone(&gone));
    let mut task = OwnedTask::spawn(&registry, async move {
        let _guard = guard;
        child_started.notify_one();
        std::future::pending::<()>().await;
    })
    .unwrap();
    notified(&started).await;
    let collector = task.collector.take().unwrap();
    collector.abort();
    assert!(collector.await.unwrap_err().is_cancelled());
    busy(&registry, 2);
    drop(task);
    notified(&gone).await;
    busy(&registry, 2);
}

#[tokio::test]
async fn collected_panic_is_termination_not_success() {
    let registry = registry();
    let (result, receipt) = oneshot::channel();
    let mut task = OwnedTask::spawn_inner(
        &registry,
        async { panic!("synthetic child panic") },
        CollectorHooks {
            result: Some(result),
            ..Default::default()
        },
    )
    .unwrap();
    collect(task.collector.take().unwrap()).await;
    assert_eq!(receipt.await.unwrap(), Terminal::Panicked);
    assert_eq!(registry.admission_snapshot().cleanup, 0);
}

#[tokio::test]
async fn closed_admission_refuses_before_any_child_is_polled() {
    let registry = registry();
    let snapshot = registry.admission_snapshot();
    assert!(matches!(
        registry.retire_if_idle(snapshot.instance),
        Ok(RetirementDecision::Closed(_))
    ));
    let polled = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let child_polled = Arc::clone(&polled);
    let result = OwnedTask::spawn(&registry, async move {
        child_polled.store(true, std::sync::atomic::Ordering::SeqCst);
    });
    assert!(matches!(result, Err(CoreError::RuntimeAdmissionClosed)));
    tokio::task::yield_now().await;
    assert!(!polled.load(std::sync::atomic::Ordering::SeqCst));
    assert_eq!(registry.admission_snapshot().cleanup, 0);
}

#[tokio::test]
async fn lost_collector_after_child_join_retains_its_own_owner() {
    let registry = registry();
    let joined = Arc::new(Notify::new());
    let mut task = OwnedTask::spawn_inner(
        &registry,
        async {},
        CollectorHooks {
            joined: Some(Arc::clone(&joined)),
            release: Some(Arc::new(Notify::new())),
            result: None,
        },
    )
    .unwrap();
    notified(&joined).await;
    busy(&registry, 1);
    let collector = task.collector.take().unwrap();
    collector.abort();
    assert!(collector.await.unwrap_err().is_cancelled());
    drop(task);
    busy(&registry, 1);
}
