//! Control-plane backend: live `DaemonClient` or a scripted fake.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use chanvoy_core::{
    GetPostParams, PostMessageParams, ReadChannelParams, ReadThreadParams, WaitChannelV3Params,
    WaitChannelsParams,
};
use chanvoy_daemon::{DaemonClient, DaemonError};
use serde_json::Value;

use crate::error::ToolErrorEnvelope;

pub const RPC_UNKNOWN_METHOD: i64 = -32601;
pub const RPC_WAIT_DEADMAN: i64 = -32005;
pub const RPC_WAIT_INPUT: i64 = -32007;
const RPC_WAIT_ALREADY_ACTIVE: i64 = -32009;
const RPC_WAIT_CONFLICT_CHANGED: i64 = -32010;
const RPC_WAIT_REPLACED: i64 = -32011;
const RPC_WAIT_REPLACE_UNCONFIRMED: i64 = -32012;

/// One canned reply from a scripted daemon.
#[derive(Debug, Clone)]
pub enum ScriptedReply {
    Ok(Value),
    Rpc {
        code: i64,
        message: String,
    },
    /// Completes only when the caller drops the future (disconnect).
    HangUntilCancel,
    /// Simulate UDS EOF / peer reset.
    Eof,
}

#[derive(Debug, Clone)]
pub struct ScriptedDaemon {
    inner: Arc<Mutex<VecDeque<(String, ScriptedReply)>>>,
}

impl ScriptedDaemon {
    pub fn new(replies: Vec<(&str, ScriptedReply)>) -> Self {
        Self {
            inner: Arc::new(Mutex::new(
                replies
                    .into_iter()
                    .map(|(m, r)| (m.to_string(), r))
                    .collect(),
            )),
        }
    }

    async fn next(&self, method: &str) -> Result<Value, ToolErrorEnvelope> {
        let reply = {
            let mut q = self.inner.lock().expect("scripted daemon mutex");
            q.pop_front()
        };
        let Some((expected, reply)) = reply else {
            return Err(ToolErrorEnvelope::provider(
                "scripted daemon has no remaining replies",
            ));
        };
        if expected != method {
            return Err(ToolErrorEnvelope::provider(format!(
                "scripted daemon expected {expected}, got {method}"
            )));
        }
        match reply {
            ScriptedReply::Ok(value) => Ok(value),
            ScriptedReply::Rpc { code, message } => {
                Err(map_rpc_failure(code, &message, is_wait(method)))
            }
            ScriptedReply::HangUntilCancel => {
                std::future::pending::<()>().await;
                unreachable!()
            }
            ScriptedReply::Eof => Err(ToolErrorEnvelope::provider(
                "daemon socket closed before a wait result",
            )),
        }
    }
}

#[derive(Clone)]
pub enum ToolBackend {
    Live(DaemonClient),
    Scripted(ScriptedDaemon),
}

impl ToolBackend {
    pub fn live(profile: &str) -> Self {
        Self::Live(DaemonClient::new(profile))
    }

    pub fn scripted(replies: Vec<(&str, ScriptedReply)>) -> Self {
        Self::Scripted(ScriptedDaemon::new(replies))
    }

    pub async fn whoami(&self) -> Result<Value, ToolErrorEnvelope> {
        match self {
            Self::Live(client) => map_live(client.whoami().await, false),
            Self::Scripted(script) => script.next("whoami").await,
        }
    }

    pub async fn read_channel(
        &self,
        params: ReadChannelParams,
    ) -> Result<Value, ToolErrorEnvelope> {
        match self {
            Self::Live(client) => map_live(
                client
                    .read_channel(
                        &params.channel,
                        params.since_secs,
                        params.after_post_id,
                        params.since_last_mine,
                        params.since_bootstrap,
                        params.limit,
                        params.advance,
                        params.team,
                    )
                    .await,
                false,
            ),
            Self::Scripted(script) => script.next("read_channel").await,
        }
    }

    pub async fn get_post(&self, params: GetPostParams) -> Result<Value, ToolErrorEnvelope> {
        match self {
            Self::Live(client) => map_live(
                client
                    .get_post(&params.channel, &params.post_id, params.team)
                    .await,
                false,
            ),
            Self::Scripted(script) => script.next("get_post").await,
        }
    }

    pub async fn read_thread(&self, params: ReadThreadParams) -> Result<Value, ToolErrorEnvelope> {
        match self {
            Self::Live(client) => map_live(
                client
                    .read_thread(&params.channel, &params.post_id, params.latest, params.team)
                    .await,
                false,
            ),
            Self::Scripted(script) => script.next("read_thread").await,
        }
    }

    pub async fn wait_channel_v3(
        &self,
        params: WaitChannelV3Params,
    ) -> Result<Value, ToolErrorEnvelope> {
        match self {
            Self::Live(client) => map_live(client.wait_channel_v3(params).await, true),
            Self::Scripted(script) => script.next("wait_channel_v3").await,
        }
    }

    pub async fn wait_channels_v1(
        &self,
        params: WaitChannelsParams,
    ) -> Result<Value, ToolErrorEnvelope> {
        match self {
            Self::Live(client) => map_live(client.wait_channels_v1(params).await, true),
            Self::Scripted(script) => script.next("wait_channels_v1").await,
        }
    }

    pub async fn post_message(
        &self,
        params: PostMessageParams,
    ) -> Result<Value, ToolErrorEnvelope> {
        match self {
            Self::Live(client) => map_live(
                client
                    .post_message(
                        &params.channel,
                        &params.message,
                        params.team,
                        params.thread_root_id,
                    )
                    .await,
                false,
            ),
            Self::Scripted(script) => script.next("post_message").await,
        }
    }
}

fn is_wait(method: &str) -> bool {
    method == "wait_channel_v3" || method == "wait_channels_v1"
}

fn map_live<T: serde::Serialize>(
    result: Result<T, DaemonError>,
    wait: bool,
) -> Result<Value, ToolErrorEnvelope> {
    match result {
        Ok(value) => serde_json::to_value(value)
            .map_err(|_| ToolErrorEnvelope::provider("failed to encode daemon result")),
        Err(err) => Err(map_daemon_error(err, wait)),
    }
}

pub fn map_daemon_error(err: DaemonError, wait: bool) -> ToolErrorEnvelope {
    match err {
        DaemonError::Rpc {
            code: RPC_UNKNOWN_METHOD,
            ..
        } if wait => ToolErrorEnvelope::capability(
            "the running daemon does not support this wait method \
             (wait_channel_v3 / wait_channels_v1); it was started from an earlier \
             chanvoy. Cycle it with `chanvoy daemon stop` then `chanvoy auto-setup`.",
        ),
        DaemonError::Rpc {
            code: RPC_UNKNOWN_METHOD,
            ..
        } => ToolErrorEnvelope::capability(
            "the running daemon does not support this verb; cycle it with \
             `chanvoy daemon stop` then `chanvoy auto-setup`.",
        ),
        DaemonError::Rpc {
            code: RPC_WAIT_DEADMAN,
            ..
        } if wait => {
            ToolErrorEnvelope::deadman("wait reached the deadman with no matching message")
        }
        DaemonError::Rpc {
            code:
                RPC_WAIT_ALREADY_ACTIVE
                | RPC_WAIT_CONFLICT_CHANGED
                | RPC_WAIT_REPLACED
                | RPC_WAIT_REPLACE_UNCONFIRMED,
            ..
        } => ToolErrorEnvelope::input(
            "wait already active or replace was refused on this profile daemon",
        ),
        DaemonError::Rpc {
            code: RPC_WAIT_INPUT,
            ..
        } => ToolErrorEnvelope::input("wait request was refused"),
        DaemonError::Rpc { message, .. } if looks_not_found(&message) => {
            ToolErrorEnvelope::not_found("requested channel or post was not found")
        }
        DaemonError::Rpc { message, .. }
            if message.contains("wait input error") || message.contains("WaitFilterInvalid") =>
        {
            ToolErrorEnvelope::input("wait request was refused")
        }
        DaemonError::NotRunning(_) => {
            ToolErrorEnvelope::provider("no chanvoy daemon is listening for this profile")
        }
        DaemonError::Rpc { .. } => ToolErrorEnvelope::provider("provider or daemon call failed"),
        DaemonError::Io(_) => ToolErrorEnvelope::provider("daemon socket closed before a result"),
        DaemonError::Json(_) => ToolErrorEnvelope::provider("daemon returned an unreadable result"),
        _ => ToolErrorEnvelope::provider("provider or daemon call failed"),
    }
}

fn map_rpc_failure(code: i64, message: &str, wait: bool) -> ToolErrorEnvelope {
    map_daemon_error(
        DaemonError::Rpc {
            code,
            message: message.to_string(),
            data: None,
        },
        wait,
    )
}

fn looks_not_found(message: &str) -> bool {
    let lower = message.to_ascii_lowercase();
    lower.contains("not found") || lower.contains("no such")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::ErrorClass;

    #[test]
    fn unknown_wait_method_is_capability_not_deadman() {
        let err = map_rpc_failure(RPC_UNKNOWN_METHOD, "unknown method wait_channel_v3", true);
        assert_eq!(err.error.class, ErrorClass::Capability);
        assert!(!err.error.timeout);
        assert!(err.error.message.contains("wait_channel_v3"));
        assert!(!err.error.message.contains("wait_channel_v2"));
    }

    #[test]
    fn wait_deadman_is_timeout_true() {
        let err = map_rpc_failure(RPC_WAIT_DEADMAN, "timeout waiting", true);
        assert_eq!(err.error.class, ErrorClass::Deadman);
        assert!(err.error.timeout);
    }

    #[test]
    fn rpc_data_is_never_copied() {
        let err = map_daemon_error(
            DaemonError::Rpc {
                code: RPC_WAIT_ALREADY_ACTIVE,
                message: "wait already active".into(),
                data: Some(serde_json::json!({
                    "token": "super-secret",
                    "existing_wait_id": "wait_abc"
                })),
            },
            true,
        );
        let dumped = serde_json::to_string(&err).unwrap();
        assert!(!dumped.contains("super-secret"));
        assert!(!dumped.contains("existing_wait_id"));
        assert_eq!(err.error.class, ErrorClass::Input);
        assert!(!err.error.timeout);
    }

    #[test]
    fn daemon_messages_are_not_forwarded() {
        let err = map_daemon_error(
            DaemonError::Rpc {
                code: -32000,
                message: "Authorization: Bearer SECRETTOKEN request_id=abc123 {\"body\":\"leak\"}"
                    .into(),
                data: Some(serde_json::json!({"token":"SECRETTOKEN"})),
            },
            false,
        );
        let dumped = serde_json::to_string(&err).unwrap();
        assert!(!dumped.contains("SECRETTOKEN"));
        assert!(!dumped.contains("request_id"));
        assert!(!dumped.contains("Bearer"));
        assert!(!dumped.contains("leak"));
        assert_eq!(err.error.class, ErrorClass::Provider);
        assert_eq!(err.error.message, "provider or daemon call failed");
    }
}
