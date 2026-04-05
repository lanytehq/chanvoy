use std::collections::BTreeMap;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::{fs, io};

use chanvoy_core::{
    load_profile, load_token, pid_path_for_profile, rpc_error, rpc_result, socket_path_for_profile,
    AddMemberParams, ArchiveChannelParams, CapabilityClass, Channel, CoreError,
    CreateChannelParams, DaemonHealth, DirectMessageParams, JsonRpcRequest, JsonRpcResponse,
    MattermostClient, NotificationsParams, NotifyParams, PostMessageParams, Profile, ProfileStatus,
    Provider, ReadChannelParams, ReadDirectMessageParams, ShutdownResult, WaitChannelParams,
    WaitResult, WAIT_POLL_SECONDS,
};
use serde::de::DeserializeOwned;
use serde::Serialize;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::{oneshot, Mutex};
use tokio::time::{sleep, timeout, Duration};
use tracing::info;

#[derive(Debug, thiserror::Error)]
pub enum DaemonError {
    #[error(transparent)]
    Core(#[from] CoreError),
    #[error("io error: {0}")]
    Io(#[from] io::Error),
    #[error("socket already running at {0}")]
    AlreadyRunning(String),
    #[error("daemon socket not available for profile {0}")]
    NotRunning(String),
    #[error("rpc error {code}: {message}")]
    Rpc { code: i64, message: String },
    #[error("serialization error: {0}")]
    Json(#[from] serde_json::Error),
}

#[derive(Clone)]
struct AppState {
    profile: Profile,
    client: MattermostClient,
    socket_path: PathBuf,
    my_user_id: String,
    wait_cursors: Arc<Mutex<BTreeMap<String, MessageCursor>>>,
}

#[derive(Debug, Clone)]
struct MessageCursor {
    id: String,
    create_at: i64,
}

pub async fn start(profile_name: &str) -> Result<DaemonHealth, DaemonError> {
    let profile = load_profile(profile_name)?;
    let socket_path = socket_path_for_profile(profile_name);
    let pid_path = pid_path_for_profile(profile_name);

    if socket_path.exists() && ping(profile_name).await.is_ok() {
        return Err(DaemonError::AlreadyRunning(
            socket_path.display().to_string(),
        ));
    }
    if let Some(parent) = socket_path.parent() {
        fs::create_dir_all(parent)?;
        fs::set_permissions(parent, fs::Permissions::from_mode(0o700))?;
    }
    if socket_path.exists() {
        fs::remove_file(&socket_path)?;
    }
    let token = load_token(&profile)?;
    let client = MattermostClient::new(&profile, token)?;
    let my_user_id = client.whoami().await?.id;
    let listener = UnixListener::bind(&socket_path)?;
    fs::set_permissions(&socket_path, fs::Permissions::from_mode(0o600))?;
    fs::write(&pid_path, std::process::id().to_string())?;
    fs::set_permissions(&pid_path, fs::Permissions::from_mode(0o600))?;

    let state = Arc::new(AppState {
        profile: profile.clone(),
        client,
        socket_path: socket_path.clone(),
        my_user_id,
        wait_cursors: Arc::new(Mutex::new(BTreeMap::new())),
    });
    let (shutdown_tx, mut shutdown_rx) = oneshot::channel::<()>();
    let shutdown_tx = Arc::new(Mutex::new(Some(shutdown_tx)));

    info!(
        profile = profile_name,
        socket = %socket_path.display(),
        "chanvoy daemon listening"
    );

    loop {
        tokio::select! {
            accept_result = listener.accept() => {
                let (stream, _) = accept_result?;
                let state = Arc::clone(&state);
                let shutdown_tx = Arc::clone(&shutdown_tx);
                tokio::spawn(async move {
                    if let Err(error) = handle_client(stream, state, shutdown_tx).await {
                        tracing::warn!(%error, "client request failed");
                    }
                });
            }
            _ = &mut shutdown_rx => {
                break;
            }
        }
    }

    cleanup_runtime_files(&socket_path, &pid_path)?;
    Ok(DaemonHealth {
        profile: profile_name.to_string(),
        socket_path,
    })
}

pub async fn ping(profile_name: &str) -> Result<DaemonHealth, DaemonError> {
    daemon_client(profile_name)
        .profile_status()
        .await
        .map(|status| DaemonHealth {
            profile: status.profile_name,
            socket_path: status.socket_path,
        })
}

pub async fn stop(profile_name: &str) -> Result<(), DaemonError> {
    daemon_client(profile_name).shutdown().await?;
    Ok(())
}

pub fn status(profile_name: &str) -> Result<PathBuf, DaemonError> {
    let socket_path = socket_path_for_profile(profile_name);
    if socket_path.exists() {
        Ok(socket_path)
    } else {
        Err(DaemonError::NotRunning(profile_name.to_string()))
    }
}

async fn handle_client(
    stream: UnixStream,
    state: Arc<AppState>,
    shutdown_tx: Arc<Mutex<Option<oneshot::Sender<()>>>>,
) -> Result<(), DaemonError> {
    let (reader, mut writer) = stream.into_split();
    let mut reader = BufReader::new(reader);
    let mut line = String::new();
    while reader.read_line(&mut line).await? != 0 {
        let request: JsonRpcRequest = serde_json::from_str(line.trim_end())?;
        let response = dispatch_request(request, &state, &shutdown_tx).await;
        writer
            .write_all(serde_json::to_string(&response)?.as_bytes())
            .await?;
        writer.write_all(b"\n").await?;
        line.clear();
    }
    Ok(())
}

async fn dispatch_request(
    request: JsonRpcRequest,
    state: &AppState,
    shutdown_tx: &Arc<Mutex<Option<oneshot::Sender<()>>>>,
) -> JsonRpcResponse {
    let response: Result<serde_json::Value, DaemonError> = match request.method.as_str() {
        "whoami" => state
            .client
            .whoami()
            .await
            .map(to_value)
            .map_err(DaemonError::from),
        "list_channels" => state
            .client
            .list_channels()
            .await
            .map(to_value)
            .map_err(DaemonError::from),
        "read_channel" => parse_and_call(&request.params, |params: ReadChannelParams| async move {
            state
                .client
                .read_channel(&params.channel, params.since_minutes)
                .await
        })
        .await
        .map(to_value),
        "post_message" => parse_and_call(&request.params, |params: PostMessageParams| async move {
            state
                .client
                .post_message(&params.channel, &params.message)
                .await
        })
        .await
        .map(to_value),
        "direct_message" => {
            parse_and_call(&request.params, |params: DirectMessageParams| async move {
                state
                    .client
                    .direct_message(&params.username, &params.message)
                    .await
            })
            .await
            .map(to_value)
        }
        "read_direct_messages" => parse_and_call(
            &request.params,
            |params: ReadDirectMessageParams| async move {
                state
                    .client
                    .read_dm(&params.username, params.since_minutes)
                    .await
            },
        )
        .await
        .map(to_value),
        "notifications" => {
            parse_and_call(&request.params, |params: NotificationsParams| async move {
                state.client.notifications(params.since_minutes).await
            })
            .await
            .map(to_value)
        }
        "notify" => parse_and_call(&request.params, |params: NotifyParams| async move {
            state
                .client
                .notify(&params.bot_username, &params.message)
                .await
        })
        .await
        .map(to_value),
        "wait_channel" => parse_and_call(&request.params, |params: WaitChannelParams| async move {
            wait_for_messages(state, &params.channel, params.timeout_minutes).await
        })
        .await
        .map(to_value),
        "create_channel" => {
            parse_and_call(&request.params, |params: CreateChannelParams| async move {
                state
                    .client
                    .create_channel(&params.name, &params.display_name, params.purpose)
                    .await
            })
            .await
            .map(to_value)
        }
        "archive_channel" => {
            parse_and_call(&request.params, |params: ArchiveChannelParams| async move {
                state
                    .client
                    .archive_channel(&params.name)
                    .await
                    .map(|_| true)
            })
            .await
            .map(to_value)
        }
        "restore_channel" => {
            parse_and_call(&request.params, |params: ArchiveChannelParams| async move {
                require_elevated_profile(&state.profile)?;
                state
                    .client
                    .restore_channel(&params.name)
                    .await
                    .map(|_| true)
            })
            .await
            .map(to_value)
        }
        "add_member" => parse_and_call(&request.params, |params: AddMemberParams| async move {
            state
                .client
                .add_member(&params.channel, &params.username)
                .await
                .map(|_| true)
        })
        .await
        .map(to_value),
        "profile_status" => Ok(to_value(ProfileStatus {
            profile_name: state.profile.name.clone(),
            role: state.profile.role.clone(),
            scope: state.profile.scope.clone(),
            provider: Provider::Mattermost,
            bot_username: state.profile.bot_username.clone(),
            server_url: state.profile.server_url.clone(),
            socket_path: state.socket_path.clone(),
        })),
        "shutdown" => {
            if let Some(sender) = shutdown_tx.lock().await.take() {
                let _ = sender.send(());
            }
            Ok(to_value(ShutdownResult { stopping: true }))
        }
        _ => Err(DaemonError::Rpc {
            code: -32601,
            message: format!("unknown method {}", request.method),
        }),
    };

    match response {
        Ok(value) => rpc_result(request.id, value),
        Err(error) => rpc_error(request.id, error_code(&error), error.to_string()),
    }
}

async fn parse_and_call<P, F, Fut, T>(params: &serde_json::Value, func: F) -> Result<T, DaemonError>
where
    P: DeserializeOwned,
    F: FnOnce(P) -> Fut,
    Fut: std::future::Future<Output = Result<T, CoreError>>,
{
    let parsed = serde_json::from_value::<P>(params.clone())?;
    func(parsed).await.map_err(DaemonError::from)
}

fn to_value<T: Serialize>(value: T) -> serde_json::Value {
    serde_json::to_value(value).expect("serializable rpc response")
}

fn error_code(error: &DaemonError) -> i64 {
    match error {
        DaemonError::Rpc { code, .. } => *code,
        DaemonError::NotRunning(_) => -32004,
        DaemonError::AlreadyRunning(_) => -32003,
        DaemonError::Core(CoreError::RequiresElevatedCapability) => -32006,
        _ => -32000,
    }
}

fn require_elevated_profile(profile: &Profile) -> Result<(), CoreError> {
    if matches!(profile.capability_class, CapabilityClass::Elevated) {
        Ok(())
    } else {
        Err(CoreError::RequiresElevatedCapability)
    }
}

async fn wait_for_messages(
    state: &AppState,
    channel: &str,
    timeout_minutes: u64,
) -> Result<WaitResult, CoreError> {
    let channel_id = state.client.channel_id_for_name(channel).await?;
    initialize_wait_cursor(state, channel, &channel_id).await?;
    let limit = Duration::from_secs(timeout_minutes * 60);
    let future = async {
        loop {
            let messages = state
                .client
                .latest_channel_messages_by_id(&channel_id, 10)
                .await?;
            let fresh = next_wait_messages(state, channel, &messages).await;
            if !fresh.is_empty() {
                return Ok(WaitResult {
                    channel: channel.to_string(),
                    messages: fresh,
                });
            }
            sleep(Duration::from_secs(WAIT_POLL_SECONDS)).await;
        }
    };
    timeout(limit, future).await.map_err(|_| CoreError::Api {
        status: reqwest::StatusCode::REQUEST_TIMEOUT,
        message: format!("timeout waiting for channel {channel}"),
    })?
}

async fn initialize_wait_cursor(
    state: &AppState,
    channel: &str,
    channel_id: &str,
) -> Result<(), CoreError> {
    let mut cursors = state.wait_cursors.lock().await;
    if cursors.contains_key(channel) {
        return Ok(());
    }
    let messages = state
        .client
        .latest_channel_messages_by_id(channel_id, 10)
        .await?;
    if let Some(last) = messages.last() {
        cursors.insert(
            channel.to_string(),
            MessageCursor {
                id: last.id.clone(),
                create_at: last.create_at,
            },
        );
    }
    Ok(())
}

async fn next_wait_messages(
    state: &AppState,
    channel: &str,
    messages: &[chanvoy_core::Message],
) -> Vec<chanvoy_core::Message> {
    let mut cursors = state.wait_cursors.lock().await;
    let cursor = cursors.get(channel).cloned();
    let fresh = match cursor {
        Some(cursor) => collect_messages_after_cursor(messages, &cursor, &state.my_user_id),
        None => Vec::new(),
    };
    if let Some(last) = messages.last() {
        cursors.insert(
            channel.to_string(),
            MessageCursor {
                id: last.id.clone(),
                create_at: last.create_at,
            },
        );
    }
    fresh
}

fn collect_messages_after_cursor(
    messages: &[chanvoy_core::Message],
    cursor: &MessageCursor,
    my_user_id: &str,
) -> Vec<chanvoy_core::Message> {
    let start_index = messages
        .iter()
        .position(|message| message.id == cursor.id)
        .map(|index| index + 1)
        .unwrap_or_else(|| {
            messages
                .iter()
                .position(|message| message.create_at > cursor.create_at)
                .unwrap_or(messages.len())
        });
    messages[start_index..]
        .iter()
        .filter(|message| message.user_id != my_user_id)
        .cloned()
        .collect()
}

fn cleanup_runtime_files(socket_path: &Path, pid_path: &Path) -> Result<(), io::Error> {
    if socket_path.exists() {
        fs::remove_file(socket_path)?;
    }
    if pid_path.exists() {
        fs::remove_file(pid_path)?;
    }
    Ok(())
}

#[derive(Clone)]
pub struct DaemonClient {
    socket_path: PathBuf,
}

impl DaemonClient {
    pub fn new(profile_name: &str) -> Self {
        Self {
            socket_path: socket_path_for_profile(profile_name),
        }
    }

    pub async fn whoami(&self) -> Result<chanvoy_core::Identity, DaemonError> {
        self.call("whoami", serde_json::json!({})).await
    }

    pub async fn list_channels(&self) -> Result<Vec<Channel>, DaemonError> {
        self.call("list_channels", serde_json::json!({})).await
    }

    pub async fn read_channel(
        &self,
        channel: &str,
        since_minutes: u64,
    ) -> Result<Vec<chanvoy_core::Message>, DaemonError> {
        self.call(
            "read_channel",
            serde_json::to_value(ReadChannelParams {
                channel: channel.to_string(),
                since_minutes,
            })?,
        )
        .await
    }

    pub async fn post_message(
        &self,
        channel: &str,
        message: &str,
    ) -> Result<chanvoy_core::PostReceipt, DaemonError> {
        self.call(
            "post_message",
            serde_json::to_value(PostMessageParams {
                channel: channel.to_string(),
                message: message.to_string(),
            })?,
        )
        .await
    }

    pub async fn direct_message(
        &self,
        username: &str,
        message: &str,
    ) -> Result<chanvoy_core::PostReceipt, DaemonError> {
        self.call(
            "direct_message",
            serde_json::to_value(DirectMessageParams {
                username: username.to_string(),
                message: message.to_string(),
            })?,
        )
        .await
    }

    pub async fn read_direct_messages(
        &self,
        username: &str,
        since_minutes: u64,
    ) -> Result<Vec<chanvoy_core::Message>, DaemonError> {
        self.call(
            "read_direct_messages",
            serde_json::to_value(ReadDirectMessageParams {
                username: username.to_string(),
                since_minutes,
            })?,
        )
        .await
    }

    pub async fn notify(
        &self,
        bot_username: &str,
        message: &str,
    ) -> Result<chanvoy_core::PostReceipt, DaemonError> {
        self.call(
            "notify",
            serde_json::to_value(NotifyParams {
                bot_username: bot_username.to_string(),
                message: message.to_string(),
            })?,
        )
        .await
    }

    pub async fn notifications(
        &self,
        since_minutes: u64,
    ) -> Result<Vec<chanvoy_core::Notification>, DaemonError> {
        self.call(
            "notifications",
            serde_json::to_value(NotificationsParams { since_minutes })?,
        )
        .await
    }

    pub async fn wait_channel(
        &self,
        channel: &str,
        timeout_minutes: u64,
    ) -> Result<WaitResult, DaemonError> {
        self.call(
            "wait_channel",
            serde_json::to_value(WaitChannelParams {
                channel: channel.to_string(),
                timeout_minutes,
            })?,
        )
        .await
    }

    pub async fn create_channel(
        &self,
        name: &str,
        display_name: &str,
        purpose: Option<String>,
    ) -> Result<Channel, DaemonError> {
        self.call(
            "create_channel",
            serde_json::to_value(CreateChannelParams {
                name: name.to_string(),
                display_name: display_name.to_string(),
                purpose,
            })?,
        )
        .await
    }

    pub async fn archive_channel(&self, name: &str) -> Result<bool, DaemonError> {
        self.call(
            "archive_channel",
            serde_json::to_value(ArchiveChannelParams {
                name: name.to_string(),
            })?,
        )
        .await
    }

    pub async fn restore_channel(&self, name: &str) -> Result<bool, DaemonError> {
        self.call(
            "restore_channel",
            serde_json::to_value(ArchiveChannelParams {
                name: name.to_string(),
            })?,
        )
        .await
    }

    pub async fn add_member(&self, channel: &str, username: &str) -> Result<bool, DaemonError> {
        self.call(
            "add_member",
            serde_json::to_value(AddMemberParams {
                channel: channel.to_string(),
                username: username.to_string(),
            })?,
        )
        .await
    }

    pub async fn profile_status(&self) -> Result<ProfileStatus, DaemonError> {
        self.call("profile_status", serde_json::json!({})).await
    }

    pub async fn shutdown(&self) -> Result<ShutdownResult, DaemonError> {
        self.call("shutdown", serde_json::json!({})).await
    }

    async fn call<T: DeserializeOwned>(
        &self,
        method: &str,
        params: serde_json::Value,
    ) -> Result<T, DaemonError> {
        if !self.socket_path.exists() {
            return Err(DaemonError::NotRunning(
                self.socket_path.display().to_string(),
            ));
        }
        let mut stream = UnixStream::connect(&self.socket_path)
            .await
            .map_err(|_| DaemonError::NotRunning(self.socket_path.display().to_string()))?;
        let request = chanvoy_core::rpc_request(method, params);
        stream
            .write_all(serde_json::to_string(&request)?.as_bytes())
            .await?;
        stream.write_all(b"\n").await?;

        let mut reader = BufReader::new(stream);
        let mut line = String::new();
        reader.read_line(&mut line).await?;
        let response: JsonRpcResponse = serde_json::from_str(line.trim_end())?;
        if let Some(error) = response.error {
            return Err(DaemonError::Rpc {
                code: error.code,
                message: error.message,
            });
        }
        let result = response.result.unwrap_or(serde_json::Value::Null);
        Ok(serde_json::from_value(result)?)
    }
}

pub fn daemon_client(profile_name: &str) -> DaemonClient {
    DaemonClient::new(profile_name)
}
