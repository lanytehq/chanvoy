use super::*;
use crate::startup_work::StartupTasks;
use wiremock::{matchers::path, Mock, ResponseTemplate};

#[tokio::test]
async fn actual_startup_probe_is_owned_through_provider_and_collection() {
    let mut provider = ProviderGate::new().await;
    let state = state(&provider.url);
    let mut tasks = StartupTasks::default();
    tasks.spawn_probe(Arc::clone(&state)).unwrap();
    provider.observed().await;
    let pending = state.wait_owners.admission_snapshot();
    assert_eq!((pending.requests, pending.cleanup), (1, 2));
    assert!(matches!(
        state.wait_owners.retire_if_idle(pending.instance),
        Ok(wait_owner::RetirementDecision::Busy(_))
    ));
    provider.finish().await;
    tasks.finish(Ok(())).await.unwrap();
    let after = state.wait_owners.admission_snapshot();
    assert_eq!((after.requests, after.cleanup), (0, 0));
}

#[tokio::test]
async fn closed_probe_and_migration_cannot_start_provider_or_publish() {
    let server = MockServer::start().await;
    let state = state(&server.uri());
    state
        .attention_state
        .lock()
        .await
        .channels
        .insert("legacy".into(), chanvoy_core::ChannelCursorState::default());
    let before = state.attention_state.lock().await.clone();
    let snapshot = state.wait_owners.admission_snapshot();
    assert!(matches!(
        state.wait_owners.retire_if_idle(snapshot.instance),
        Ok(wait_owner::RetirementDecision::Closed(_))
    ));
    let mut tasks = StartupTasks::default();
    assert!(!tasks.spawn_probe(Arc::clone(&state)).unwrap());
    assert!(!tasks.spawn_migration(Arc::clone(&state)).unwrap());
    tasks.finish(Ok(())).await.unwrap();
    assert_eq!(*state.attention_state.lock().await, before);
    assert!(server.received_requests().await.unwrap().is_empty());
}

#[tokio::test]
async fn typed_probe_timeout_is_complete_only_after_child_join_and_cannot_late_publish_identity() {
    let mut provider = ProviderGate::new().await;
    let state = state(&provider.url);
    let mut tasks = StartupTasks::default();
    tasks.spawn_probe(Arc::clone(&state)).unwrap();
    provider.observed().await;
    assert_eq!(state.wait_owners.admission_snapshot().requests, 1);
    tokio::time::timeout(Duration::from_secs(3), tasks.finish(Ok(())))
        .await
        .unwrap()
        .unwrap();
    let after = state.wait_owners.admission_snapshot();
    assert_eq!((after.requests, after.cleanup), (0, 0));
    assert!(!state.identity_drift.load(Ordering::SeqCst));
    assert!(!state.observed_identity_drift.load(Ordering::SeqCst));
    provider.finish().await;
    assert!(!state.identity_drift.load(Ordering::SeqCst));
    assert!(!state.observed_identity_drift.load(Ordering::SeqCst));
}

#[tokio::test]
async fn actual_migration_persistence_failure_retains_owner_and_preserves_live_cursor() {
    for failed in [false, true] {
        let server = MockServer::start().await;
        let state = state(&server.uri());
        Mock::given(path("/api/v4/teams/name/org"))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(json!({"id":"team-fixture","name":"org"})),
            )
            .mount(&server)
            .await;
        Mock::given(path("/api/v4/teams/team-fixture/channels/name/legacy"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(json!({"id":"channel-fixture","name":"legacy"})),
            )
            .mount(&server)
            .await;
        state.attention_state.lock().await.channels.insert(
            "legacy".into(),
            chanvoy_core::ChannelCursorState {
                last_seen_post_id: Some("cursor-fixture".into()),
                ..Default::default()
            },
        );
        let before = state.attention_state.lock().await.clone();
        let mut tasks = StartupTasks::default();
        let child = Arc::clone(&state);
        let persist_registry = Arc::clone(&state.wait_owners);
        let persisted = Arc::new(AtomicU64::new(0));
        let calls = Arc::clone(&persisted);
        tasks
            .spawn(&state.wait_owners, move || async move {
                crate::run_attention_migration_with_persist(
                    child.profile.name.clone(),
                    Arc::clone(&child.attention_state),
                    child.client.clone(),
                    Duration::from_secs(2),
                    move |_| {
                        let s = persist_registry.admission_snapshot();
                        assert_eq!(
                            (s.requests, s.cleanup),
                            (1, 2),
                            "persistence outside owned startup task"
                        );
                        calls.fetch_add(1, Ordering::SeqCst);
                        if failed {
                            Err(io::Error::other("synthetic migration persistence failure").into())
                        } else {
                            Ok(())
                        }
                    },
                )
                .await
                .map_err(DaemonError::from)
            })
            .unwrap();
        let result = tasks.finish(Ok(())).await;
        assert_eq!(result.is_err(), failed);
        assert_eq!(
            persisted.load(Ordering::SeqCst),
            1,
            "actual migration did not reach persistence"
        );
        let after = state.wait_owners.admission_snapshot();
        assert_eq!((after.requests, after.cleanup), (usize::from(failed), 0));
        let live = state.attention_state.lock().await;
        if failed {
            assert_eq!(*live, before);
        } else {
            assert!(!live.channels.contains_key("legacy"));
            assert_eq!(
                live.channels["org/legacy"].last_seen_post_id.as_deref(),
                Some("cursor-fixture")
            );
        }
    }
}

#[tokio::test]
async fn migration_budget_deferral_joins_the_read_and_cannot_late_publish() {
    let mut provider = ProviderGate::new().await;
    let state = state(&provider.url);
    state
        .attention_state
        .lock()
        .await
        .channels
        .insert("legacy".into(), chanvoy_core::ChannelCursorState::default());
    let before = state.attention_state.lock().await.clone();
    let child = Arc::clone(&state);
    let persisted = Arc::new(AtomicU64::new(0));
    let calls = Arc::clone(&persisted);
    let mut tasks = StartupTasks::default();
    tasks
        .spawn(&state.wait_owners, move || async move {
            crate::run_attention_migration_with_persist(
                child.profile.name.clone(),
                Arc::clone(&child.attention_state),
                child.client.clone(),
                Duration::from_secs(2),
                move |_| {
                    calls.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                },
            )
            .await
            .map_err(DaemonError::from)
        })
        .unwrap();
    provider.observed().await;
    assert_eq!(state.wait_owners.admission_snapshot().requests, 1);
    tokio::time::timeout(Duration::from_secs(3), tasks.finish(Ok(())))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(persisted.load(Ordering::SeqCst), 0);
    assert_eq!(*state.attention_state.lock().await, before);
    let after = state.wait_owners.admission_snapshot();
    assert_eq!((after.requests, after.cleanup), (0, 0));
    provider.finish().await;
    assert_eq!(persisted.load(Ordering::SeqCst), 0);
    assert_eq!(*state.attention_state.lock().await, before);
}
