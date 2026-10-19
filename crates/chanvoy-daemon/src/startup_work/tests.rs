use super::*;
use crate::wait_owner::RetirementDecision;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Condvar, Mutex};
use std::time::Duration;
use tokio::sync::Notify;

async fn observed(notify: &Notify) {
    tokio::time::timeout(Duration::from_secs(3), notify.notified())
        .await
        .unwrap();
}
async fn pending<F: Future>(mut future: std::pin::Pin<&mut F>) {
    let polled = std::future::poll_fn(|cx| std::task::Poll::Ready(future.as_mut().poll(cx))).await;
    assert!(
        polled.is_pending(),
        "startup collection returned before blocked work ended"
    );
}

fn counts(registry: &WaitOwnerRegistry) -> (usize, usize) {
    let s = registry.admission_snapshot();
    (s.requests, s.cleanup)
}
fn failed(message: &'static str) -> DaemonError {
    std::io::Error::other(message).into()
}

#[tokio::test]
async fn factory_and_pending_work_already_own_request_child_and_collector() {
    let registry = Arc::new(WaitOwnerRegistry::new());
    let inspected = Arc::clone(&registry);
    let entered = Arc::new(Notify::new());
    let ready = Arc::clone(&entered);
    let release = Arc::new(Notify::new());
    let go = Arc::clone(&release);
    let mut tasks = StartupTasks::default();
    assert!(tasks
        .spawn(&registry, move || {
            assert_eq!(
                counts(&inspected),
                (1, 2),
                "factory constructed before complete admission"
            );
            async move {
                ready.notify_one();
                go.notified().await;
                Ok(())
            }
        })
        .unwrap());
    observed(&entered).await;
    let snapshot = registry.admission_snapshot();
    assert!(matches!(
        registry.retire_if_idle(snapshot.instance),
        Ok(RetirementDecision::Busy(_))
    ));
    let mut finish = Box::pin(tasks.finish(Ok(())));
    pending(finish.as_mut()).await;
    assert_eq!(counts(&registry), (1, 2));
    release.notify_one();
    tokio::time::timeout(Duration::from_secs(3), finish)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(counts(&registry), (0, 0));
}

#[tokio::test]
async fn closed_admission_omits_factory_and_all_work() {
    let registry = Arc::new(WaitOwnerRegistry::new());
    let initial = registry.admission_snapshot();
    assert!(matches!(
        registry.retire_if_idle(initial.instance),
        Ok(RetirementDecision::Closed(_))
    ));
    let constructed = Arc::new(AtomicUsize::new(0));
    let count = Arc::clone(&constructed);
    let mut tasks = StartupTasks::default();
    assert!(!tasks
        .spawn(&registry, move || {
            count.fetch_add(1, Ordering::SeqCst);
            async { panic!("closed startup work polled") }
        })
        .unwrap());
    tasks.finish(Ok(())).await.unwrap();
    assert_eq!(constructed.load(Ordering::SeqCst), 0);
    assert_eq!(counts(&registry), (0, 0));
}

#[tokio::test]
async fn dropped_parent_leaves_actual_work_owned_and_cannot_clear_another_request() {
    let registry = Arc::new(WaitOwnerRegistry::new());
    let other = registry.request_permit().unwrap();
    let entered = Arc::new(Notify::new());
    let ready = Arc::clone(&entered);
    let release = Arc::new(Notify::new());
    let go = Arc::clone(&release);
    let mut tasks = StartupTasks::default();
    tasks
        .spawn(&registry, move || async move {
            ready.notify_one();
            go.notified().await;
            Ok(())
        })
        .unwrap();
    observed(&entered).await;
    drop(tasks);
    assert_eq!(counts(&registry), (2, 2));
    release.notify_one();
    tokio::time::timeout(Duration::from_secs(3), async {
        while counts(&registry) != (1, 0) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    other.confirm_done().unwrap();
    assert_eq!(counts(&registry), (0, 0));
}

#[tokio::test]
async fn unknown_failure_and_panic_retain_request_after_actual_collection() {
    for panic in [false, true] {
        let registry = Arc::new(WaitOwnerRegistry::new());
        let mut tasks = StartupTasks::default();
        tasks
            .spawn(&registry, move || async move {
                if panic {
                    panic!("controlled startup panic");
                }
                Err(failed("publication unconfirmed"))
            })
            .unwrap();
        let result = tasks.finish(Ok(())).await;
        assert!(result.is_err());
        assert_eq!(counts(&registry), (1, 0));
        let state = registry.admission_snapshot();
        assert!(matches!(
            registry.retire_if_idle(state.instance),
            Ok(RetirementDecision::Busy(_))
        ));
    }
}

#[tokio::test]
async fn body_or_first_task_error_never_skips_collection_of_later_work() {
    for body_error in [false, true] {
        let registry = Arc::new(WaitOwnerRegistry::new());
        let mut tasks = StartupTasks::default();
        tasks
            .spawn(&registry, || async { Err(failed("first task error")) })
            .unwrap();
        let entered = Arc::new(Notify::new());
        let ready = Arc::clone(&entered);
        let release = Arc::new(Notify::new());
        let go = Arc::clone(&release);
        let completed = Arc::new(AtomicBool::new(false));
        let done = Arc::clone(&completed);
        tasks
            .spawn(&registry, move || async move {
                ready.notify_one();
                go.notified().await;
                done.store(true, Ordering::SeqCst);
                Ok(())
            })
            .unwrap();
        observed(&entered).await;
        // The first failing task and its collector must be fully finished
        // before polling the group, so Pending proves the later join is awaited.
        tokio::time::timeout(Duration::from_secs(3), async {
            while counts(&registry).1 != 2 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let mut finish = Box::pin(tasks.finish(if body_error {
            Err(failed("original daemon error"))
        } else {
            Ok(())
        }));
        pending(finish.as_mut()).await;
        assert!(!completed.load(Ordering::SeqCst));
        release.notify_one();
        let message = tokio::time::timeout(Duration::from_secs(3), finish)
            .await
            .unwrap()
            .unwrap_err()
            .to_string();
        assert!(message.contains(if body_error {
            "original daemon error"
        } else {
            "first task error"
        }));
        assert!(
            completed.load(Ordering::SeqCst),
            "first failure skipped later child collection"
        );
        assert_eq!(counts(&registry), (1, 0));
    }
}

struct BlockDrop {
    entered: Arc<Notify>,
    release: Arc<(Mutex<bool>, Condvar)>,
}
impl Drop for BlockDrop {
    fn drop(&mut self) {
        self.entered.notify_one();
        let (lock, cv) = self.release.as_ref();
        let (allowed, _) = cv
            .wait_timeout_while(lock.lock().unwrap(), Duration::from_secs(5), |v| !*v)
            .unwrap();
        assert!(*allowed, "startup child destructor not released");
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn successful_future_does_not_confirm_before_actual_child_destruction() {
    let registry = Arc::new(WaitOwnerRegistry::new());
    let entered = Arc::new(Notify::new());
    let release = Arc::new((Mutex::new(false), Condvar::new()));
    let guard = BlockDrop {
        entered: Arc::clone(&entered),
        release: Arc::clone(&release),
    };
    let mut tasks = StartupTasks::default();
    tasks
        .spawn(&registry, move || async move {
            let _guard = guard;
            Ok(())
        })
        .unwrap();
    observed(&entered).await;
    assert_eq!(
        counts(&registry),
        (1, 2),
        "future result is not actual child collection"
    );
    let mut finish = Box::pin(tasks.finish(Ok(())));
    pending(finish.as_mut()).await;
    *release.0.lock().unwrap() = true;
    release.1.notify_one();
    tokio::time::timeout(Duration::from_secs(3), finish)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(counts(&registry), (0, 0));
}
