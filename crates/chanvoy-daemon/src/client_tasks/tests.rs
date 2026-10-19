use super::*;
use crate::wait_owner::WaitOwnerRegistry;
use std::sync::atomic::{AtomicBool, Ordering};
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
async fn pending<F: Future>(mut future: std::pin::Pin<&mut F>) {
    let poll = std::future::poll_fn(|cx| std::task::Poll::Ready(future.as_mut().poll(cx))).await;
    assert!(
        poll.is_pending(),
        "client collection bypassed unfinished child"
    );
}

#[tokio::test]
async fn idle_children_stop_and_completed_collectors_are_reaped_during_serving() {
    let parent = CancellationToken::new();
    let mut tasks = ClientTasks::new(&parent);
    for _ in 0..32 {
        tasks.spawn(|_| async { Ok(()) });
        tokio::time::timeout(Duration::from_secs(3), tasks.collect_next())
            .await
            .unwrap()
            .unwrap();
        assert!(tasks.is_empty(), "finished client handles accumulated");
    }
    let exited = Arc::new(AtomicBool::new(false));
    let done = Arc::clone(&exited);
    tasks.spawn(move |stop| async move {
        stop.cancelled().await;
        done.store(true, Ordering::SeqCst);
        Ok(())
    });
    assert_eq!(tasks.finish(Ok(17)).await.unwrap(), 17);
    assert!(exited.load(Ordering::SeqCst));
    assert!(
        !parent.is_cancelled(),
        "client stop cancelled sibling infrastructure"
    );
}

#[tokio::test]
async fn dropping_group_cancels_idle_but_retains_active_child_and_other_owner() {
    let registry = Arc::new(WaitOwnerRegistry::new());
    let other = registry.request_permit().unwrap();
    let request = registry.request_permit().unwrap();
    let entered = Arc::new(Notify::new());
    let child_entered = Arc::clone(&entered);
    let release = Arc::new(Notify::new());
    let child_release = Arc::clone(&release);
    let ended = Arc::new(Notify::new());
    let child_ended = Arc::clone(&ended);
    let parent = CancellationToken::new();
    let mut tasks = ClientTasks::new(&parent);
    tasks.spawn(move |stop| async move {
        stop.cancelled().await;
        child_entered.notify_one();
        child_release.notified().await;
        request.confirm_done().unwrap();
        child_ended.notify_one();
        Ok(())
    });
    drop(tasks);
    notified(&entered).await;
    assert_eq!(registry.admission_snapshot().requests, 2);
    assert!(!parent.is_cancelled());
    release.notify_one();
    notified(&ended).await;
    assert_eq!(registry.admission_snapshot().requests, 1);
    other.confirm_done().unwrap();
}

#[tokio::test]
async fn collected_protocol_failure_does_not_confirm_its_request() {
    let registry = Arc::new(WaitOwnerRegistry::new());
    let request = registry.request_permit().unwrap();
    let mut tasks = ClientTasks::new(&CancellationToken::new());
    tasks.spawn(move |_| async move {
        let _request = request;
        Err(failure("synthetic client write failure"))
    });
    tasks.finish(Ok(())).await.unwrap();
    assert_eq!(registry.admission_snapshot().requests, 1);
}

#[tokio::test]
async fn panicked_child_retains_request_and_prevents_successful_collection() {
    let registry = Arc::new(WaitOwnerRegistry::new());
    let request = registry.request_permit().unwrap();
    let mut tasks = ClientTasks::new(&CancellationToken::new());
    tasks.spawn(move |_| async move {
        let _request = request;
        panic!("controlled client panic");
    });
    let outcome = tasks.finish(Ok(())).await;
    assert!(outcome
        .unwrap_err()
        .to_string()
        .contains("collection unconfirmed"));
    assert_eq!(registry.admission_snapshot().requests, 1);
}

#[tokio::test]
async fn daemon_or_collector_failure_still_collects_every_later_child() {
    for daemon_failed in [false, true] {
        let ready = Arc::new(Notify::new());
        let child_ready = Arc::clone(&ready);
        let release = Arc::new(Notify::new());
        let child_release = Arc::clone(&release);
        let mut tasks = ClientTasks::new(&CancellationToken::new());
        tasks.spawn(|_| async { panic!("controlled first child panic") });
        tasks.spawn(move |stop| async move {
            stop.cancelled().await;
            child_ready.notify_one();
            child_release.notified().await;
            Ok(())
        });
        let outcome: Result<(), DaemonError> = if daemon_failed {
            Err(failure("original daemon failure"))
        } else {
            Ok(())
        };
        let mut finish = Box::pin(tasks.finish(outcome));
        pending(finish.as_mut()).await;
        notified(&ready).await;
        pending(finish.as_mut()).await;
        release.notify_one();
        let error = tokio::time::timeout(Duration::from_secs(3), finish)
            .await
            .unwrap()
            .unwrap_err()
            .to_string();
        assert!(error.contains(if daemon_failed {
            "original daemon failure"
        } else {
            "collection unconfirmed"
        }));
    }
}

#[tokio::test]
async fn failed_collector_join_prevents_successful_collection() {
    let mut tasks = ClientTasks::new(&CancellationToken::new());
    let handle = tasks.collectors.spawn(async {
        std::future::pending::<()>().await;
        Ok(())
    });
    // Test-only fault: production never aborts collectors or client tasks.
    handle.abort();
    assert!(tasks.finish(Ok(())).await.is_err());
}

struct BlockDrop {
    entered: Arc<Notify>,
    release: Arc<(Mutex<bool>, Condvar)>,
}
impl Drop for BlockDrop {
    fn drop(&mut self) {
        self.entered.notify_one();
        let (allowed, _) = self
            .release
            .1
            .wait_timeout_while(
                self.release.0.lock().unwrap(),
                Duration::from_secs(5),
                |v| !*v,
            )
            .unwrap();
        assert!(*allowed, "client destructor not released");
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn collection_waits_through_actual_child_destruction() {
    let entered = Arc::new(Notify::new());
    let release = Arc::new((Mutex::new(false), Condvar::new()));
    let guard = BlockDrop {
        entered: Arc::clone(&entered),
        release: Arc::clone(&release),
    };
    let mut tasks = ClientTasks::new(&CancellationToken::new());
    tasks.spawn(move |_| async move {
        let _guard = guard;
        Ok(())
    });
    notified(&entered).await;
    let mut finish = Box::pin(tasks.finish(Ok(())));
    // The destructor is held by a condition variable throughout this window;
    // the deadline observes absence of a receipt, never grants completion.
    assert!(
        tokio::time::timeout(Duration::from_millis(50), &mut finish)
            .await
            .is_err(),
        "completion receipt preceded actual child destruction"
    );
    *release.0.lock().unwrap() = true;
    release.1.notify_one();
    tokio::time::timeout(Duration::from_secs(3), finish)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn dropped_group_leaves_collectors_alive_through_actual_join() {
    let release = Arc::new(Notify::new());
    let child_release = Arc::clone(&release);
    let child = tokio::spawn(async move {
        child_release.notified().await;
    });
    let observed = Arc::new(Notify::new());
    let joined = Arc::clone(&observed);
    let mut tasks = ClientTasks::new(&CancellationToken::new());
    // Exercise the actual group Drop policy with an instrumented collector.
    // Only this collector can send the notification, after its real child join.
    tasks.collectors.spawn(async move {
        child.await.map_err(join_error)?;
        joined.notify_one();
        Ok(())
    });
    drop(tasks);
    release.notify_one();
    let result = tokio::time::timeout(Duration::from_secs(3), observed.notified()).await;
    assert!(
        result.is_ok(),
        "group drop aborted its independent collector"
    );
}
