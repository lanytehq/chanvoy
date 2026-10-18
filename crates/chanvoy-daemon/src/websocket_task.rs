//! One private managed websocket runner, composed before runtime cleanup.
use crate::{ipc_ownership::IpcAdmission, wait_owner::WaitOwnerRegistry, DaemonError};
use chanvoy_core::{websocket_ownership::ManagedWs, MattermostWs};
use std::sync::Arc;

#[derive(Default)]
pub(crate) struct WebsocketTask {
    task: Option<ManagedWs>,
    started: bool,
}
impl WebsocketTask {
    pub(crate) fn start(
        &mut self,
        ws: Arc<MattermostWs>,
        arena: Arc<WaitOwnerRegistry>,
    ) -> Result<(), DaemonError> {
        if self.started {
            return Err(std::io::Error::other("websocket runner already started").into());
        }
        self.started = true;
        // The existing adapter delegates to this exact daemon arena. Managed
        // websocket work never calls reserve_registration on its completion.
        self.task = Some(ws.spawn_managed(Arc::new(IpcAdmission(arena))));
        Ok(())
    }
    pub(crate) fn request_stop(&self) {
        if let Some(task) = &self.task {
            task.request_stop();
        }
    }
    pub(crate) async fn finish<T>(
        mut self,
        outcome: Result<T, DaemonError>,
    ) -> Result<T, DaemonError> {
        self.request_stop();
        let Some(task) = self.task.take() else {
            return outcome;
        };
        let (execution, collection) = task.finish().await.into_parts();
        // Both results are collected even when the earlier application failed.
        let collected = execution
            .map_err(DaemonError::from)
            .and_then(|()| collection.map_err(|error| std::io::Error::other(error).into()));
        match outcome {
            Err(error) => Err(error),
            Ok(value) => collected.map(|()| value),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chanvoy_core::{
        CapabilityClass, CredentialMode, EventBus, MattermostClient, Profile, Provider,
    };
    use std::time::Duration;

    fn ws(url: String) -> Arc<MattermostWs> {
        let profile = Profile {
            name: "test".into(),
            role: "test".into(),
            scope: "test".into(),
            provider: Provider::Mattermost,
            bot_username: "test-bot".into(),
            team_name: "test-team".into(),
            server_url: url,
            env_name: "TEST_TOKEN".into(),
            env_file: None,
            credential_mode: CredentialMode::EnvName,
            capability_class: CapabilityClass::Standard,
            monitored_channels: Vec::new(),
            ipc: None,
            reduce: None,
        };
        let client = MattermostClient::new(&profile, "test-token".into()).unwrap();
        Arc::new(MattermostWs::new(
            &profile,
            client,
            Arc::new(EventBus::new(64)),
            "self".into(),
        ))
    }

    #[tokio::test]
    async fn error_exit_still_collects_blocked_websocket_on_actual_arena() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let arena = Arc::new(WaitOwnerRegistry::new());
        let unrelated = arena.request_permit().unwrap();
        let mut task = WebsocketTask::default();
        task.start(
            ws(format!("http://{}", listener.local_addr().unwrap())),
            arena.clone(),
        )
        .unwrap();
        let (stream, _) = listener.accept().await.unwrap();
        assert_eq!(arena.admission_snapshot().requests, 2);
        let original: Result<(), DaemonError> =
            Err(std::io::Error::other("original serve failure").into());
        let mut finish = Box::pin(task.finish(original));
        assert!(tokio::time::timeout(Duration::from_millis(30), &mut finish)
            .await
            .is_err());
        assert_eq!(arena.admission_snapshot().requests, 2);
        drop(stream);
        let error = tokio::time::timeout(Duration::from_secs(2), finish)
            .await
            .unwrap()
            .unwrap_err();
        assert!(error.to_string().contains("original serve failure"));
        assert_eq!(arena.admission_snapshot().requests, 1);
        unrelated.confirm_done().unwrap();
    }

    #[tokio::test]
    async fn private_supervisor_refuses_second_runner() {
        let arena = Arc::new(WaitOwnerRegistry::new());
        let snapshot = arena.admission_snapshot();
        arena.retire_if_idle(snapshot.instance).unwrap();
        let mut task = WebsocketTask::default();
        let websocket = ws("http://127.0.0.1:1".into());
        task.start(websocket.clone(), arena.clone()).unwrap();
        assert!(task.start(websocket, arena).is_err());
        task.finish(Ok(())).await.unwrap();
    }

    #[tokio::test]
    async fn known_unstarted_exit_preserves_original_error() {
        let outcome: Result<(), DaemonError> =
            Err(std::io::Error::other("before websocket start").into());
        let error = WebsocketTask::default().finish(outcome).await.unwrap_err();
        assert!(error.to_string().contains("before websocket start"));
    }
}
