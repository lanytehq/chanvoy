use super::*;
use crate::client_tasks::ClientTasks;
use tokio_util::sync::CancellationToken;

fn start_owned(
    state: &Arc<AppState>,
    writer: FrameWriter,
    parent: &CancellationToken,
) -> (tokio::io::DuplexStream, ClientTasks) {
    let (server, client) = tokio::io::duplex(4096);
    let (shutdown, _rx) = oneshot::channel();
    let state = Arc::clone(state);
    let mut tasks = ClientTasks::new(parent);
    tasks.spawn(move |stop| async move {
        crate::handle_client_io_with_shutdown(
            BufReader::new(server),
            writer,
            state,
            Arc::new(Mutex::new(Some(shutdown))),
            stop,
        )
        .await
    });
    (client, tasks)
}
async fn pending<F: std::future::Future>(mut future: Pin<&mut F>) {
    let poll = std::future::poll_fn(|cx| Poll::Ready(future.as_mut().poll(cx))).await;
    assert!(
        poll.is_pending(),
        "client collection truncated admitted work"
    );
}
async fn finish(tasks: ClientTasks) {
    let result = tokio::time::timeout(Duration::from_secs(3), tasks.finish(Ok(()))).await;
    assert!(result.is_ok(), "idle client did not stop and collect");
    result.unwrap().unwrap();
}

#[tokio::test]
async fn idle_real_client_has_no_operational_owner_and_stop_wakes_its_read() {
    let state = state("http://127.0.0.1:9");
    let parent = CancellationToken::new();
    let mut tasks = ClientTasks::new(&parent);
    let (server, client) = UnixStream::pair().unwrap();
    let task_state = Arc::clone(&state);
    let (shutdown, _rx) = oneshot::channel();
    tasks.spawn(move |stop| async move {
        crate::handle_client_with_shutdown(
            server,
            task_state,
            Arc::new(Mutex::new(Some(shutdown))),
            stop,
        )
        .await
    });
    // A real idle Unix connection remains open until finish requests stop.
    assert_counts(&state, 0, 0);
    finish(tasks).await;
    assert_counts(&state, 0, 0);
    assert!(!parent.is_cancelled());
    drop(client);
}

#[tokio::test]
async fn already_visible_stop_skips_ready_request_and_provider_construction() {
    let server = MockServer::start().await;
    let state = state(&server.uri());
    let stop = CancellationToken::new();
    let (writer, gate) = make_writer(Stage::Flush, 99, false);
    let (reader, mut client) = tokio::io::duplex(4096);
    send(&mut client, "whoami", json!({})).await;
    stop.cancel();
    let (shutdown, _rx) = oneshot::channel();
    crate::handle_client_io_with_shutdown(
        BufReader::new(reader),
        writer,
        Arc::clone(&state),
        Arc::new(Mutex::new(Some(shutdown))),
        stop,
    )
    .await
    .unwrap();
    assert!(server.received_requests().await.unwrap().is_empty());
    assert!(documents(&gate).is_empty());
    assert_counts(&state, 0, 0);
}

#[tokio::test]
async fn active_provider_then_response_flush_finish_before_idle_stop() {
    let mut provider = ProviderGate::new().await;
    let state = state(&provider.url);
    let (writer, gate) = make_writer(Stage::Flush, 0, false);
    let parent = CancellationToken::new();
    let (mut client, tasks) = start_owned(&state, writer, &parent);
    send(&mut client, "whoami", json!({})).await;
    provider.observed().await;
    assert_counts(&state, 1, 0);
    let mut completion = Box::pin(tasks.finish(Ok(())));
    pending(completion.as_mut()).await;
    assert_counts(&state, 1, 0);
    provider.finish().await;
    notified(&gate.reached).await;
    pending(completion.as_mut()).await;
    assert_counts(&state, 1, 0);
    gate.release();
    tokio::time::timeout(Duration::from_secs(3), completion)
        .await
        .unwrap()
        .unwrap();
    assert_counts(&state, 0, 0);
    let frames = documents(&gate);
    assert_eq!(frames.len(), 1);
    assert!(frames[0]["error"].is_null());
    assert_eq!(frames[0]["result"]["username"], "agent-test");
    assert!(!parent.is_cancelled());
}

#[tokio::test]
async fn selected_push_flush_precedes_disconnect_and_preserves_other_registration() {
    let state = state("http://127.0.0.1:9");
    let (writer, gate) = make_writer(Stage::Flush, 1, false);
    let (mut client, tasks) = start_owned(&state, writer, &CancellationToken::new());
    let id = subscribe(&mut client, &gate, &state).await;
    let other = state.wait_owners.request_permit().unwrap();
    subscription_owner::insert(
        &mut *state.subscriptions.lock().await,
        &state.wait_owners,
        "other-client".into(),
        SubscriptionFilter::AllMonitored,
    )
    .unwrap();
    state.event_bus.emit(event());
    notified(&gate.reached).await;
    assert_counts(&state, 2, 2);
    let mut completion = Box::pin(tasks.finish(Ok(())));
    pending(completion.as_mut()).await;
    assert_counts(&state, 2, 2);
    gate.release();
    tokio::time::timeout(Duration::from_secs(3), completion)
        .await
        .unwrap()
        .unwrap();
    assert_counts(&state, 1, 1);
    let mut subs = state.subscriptions.lock().await;
    assert!(!subs.contains_key(&id));
    assert!(subs.contains_key("other-client"));
    subscription_owner::remove(&mut subs, "other-client").unwrap();
    other.confirm_done().unwrap();
    assert_counts(&state, 0, 0);
    assert_eq!(documents(&gate).len(), 2);
}

#[tokio::test]
async fn failed_push_is_collected_without_waiving_send_or_registration_ownership() {
    let state = state("http://127.0.0.1:9");
    let (writer, gate) = make_writer(Stage::Flush, 1, true);
    let (mut client, mut tasks) = start_owned(&state, writer, &CancellationToken::new());
    let id = subscribe(&mut client, &gate, &state).await;
    state.event_bus.emit(event());
    // Successful task collection is distinct from operational completion.
    tokio::time::timeout(Duration::from_secs(3), tasks.collect_next())
        .await
        .unwrap()
        .unwrap();
    assert!(tasks.is_empty());
    assert_counts(&state, 1, 1);
    assert!(state.subscriptions.lock().await.contains_key(&id));
    finish(tasks).await;
    assert_counts(&state, 1, 1);
    assert_busy(&state);
}

#[tokio::test]
async fn stop_after_subscribe_ack_removes_only_confirmed_connection_registration() {
    let state = state("http://127.0.0.1:9");
    let (writer, gate) = make_writer(Stage::Flush, 99, false);
    let (mut client, tasks) = start_owned(&state, writer, &CancellationToken::new());
    subscribe(&mut client, &gate, &state).await;
    finish(tasks).await;
    assert_counts(&state, 0, 0);
    assert!(state.subscriptions.lock().await.is_empty());
}
