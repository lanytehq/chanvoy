use std::collections::HashMap;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::{fs, io};

use chanvoy_core::{
    daemon_event_to_notification, load_attention_state, load_profile, load_token, now_unix_millis,
    pid_path_for_profile, rpc_error, rpc_result, socket_path_for_profile, store_attention_state,
    AddMemberParams, ArchiveChannelParams, AttentionShowParams, AttentionState, CapabilityClass,
    Channel, CheckChannelParams, CheckResult, CoreError, CreateChannelParams, DaemonEvent,
    DaemonEventKind, DaemonEventPayloadInner, DaemonHealth, DaemonStatus, DirectMessageParams,
    DmConversation, EventBus, IpcConfig, JsonRpcNotification, JsonRpcRequest, JsonRpcResponse,
    MattermostClient, MattermostWs, NotificationsParams, NotifyParams, PostMessageParams, Profile,
    ProfileStatus, Provider, ReadChannelParams, ReadDirectMessageParams, ShutdownResult,
    SubscribeParams, SubscriptionAck, SubscriptionFilter, UnreadNotifications, UnsubscribeParams,
    WaitChannelParams, WaitResult, WsState,
};
use chanvoy_ipc::{IpcPeer, IpcPeerState};
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
    event_bus: Arc<EventBus>,
    subscriptions: Arc<Mutex<HashMap<String, SubscriptionFilter>>>,
    ws_state_holder: Arc<Mutex<Option<Arc<WsState>>>>,
    ipc_state: Option<Arc<tokio::sync::Mutex<IpcPeerState>>>,
    attention_state: Arc<Mutex<AttentionState>>,
}

#[derive(Serialize)]
#[serde(untagged)]
enum NotificationsResponse {
    Messages(Vec<chanvoy_core::Notification>),
    Unread(UnreadNotifications),
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
    let identity = client.whoami().await?;
    if !profile.bot_username.is_empty() && identity.username != profile.bot_username {
        return Err(CoreError::ProfileIdentityMismatch {
            expected: profile.bot_username.clone(),
            actual: identity.username,
        }
        .into());
    }
    let my_user_id = identity.id;
    let listener = UnixListener::bind(&socket_path)?;
    fs::set_permissions(&socket_path, fs::Permissions::from_mode(0o600))?;
    fs::write(&pid_path, std::process::id().to_string())?;
    fs::set_permissions(&pid_path, fs::Permissions::from_mode(0o600))?;

    let ws_state_holder: Arc<Mutex<Option<Arc<WsState>>>> = Arc::new(Mutex::new(None));
    let event_bus: Arc<EventBus> = Arc::new(EventBus::new(256));
    let cancel_token = tokio_util::sync::CancellationToken::new();

    let ipc_state: Option<Arc<tokio::sync::Mutex<IpcPeerState>>> = match &profile.ipc {
        Some(IpcConfig {
            enabled: true,
            gateway_socket,
        }) if !gateway_socket.is_empty() => {
            let token_for_ipc = load_token(&profile)?;
            let client_for_ipc = MattermostClient::new(&profile, token_for_ipc)?;
            let ipc_peer = Arc::new(IpcPeer::new(
                &profile,
                client_for_ipc,
                Arc::clone(&event_bus),
                gateway_socket.clone(),
            ));
            let state = ipc_peer.state();
            let cancel = cancel_token.clone();
            tokio::spawn(async move {
                ipc_peer.run(cancel).await;
            });
            Some(state)
        }
        _ => None,
    };

    let state = Arc::new(AppState {
        profile: profile.clone(),
        client,
        socket_path: socket_path.clone(),
        my_user_id,
        event_bus: Arc::clone(&event_bus),
        subscriptions: Arc::new(Mutex::new(HashMap::new())),
        ws_state_holder: ws_state_holder.clone(),
        ipc_state,
        attention_state: Arc::new(Mutex::new(load_attention_state(&profile.name)?)),
    });
    let (shutdown_tx, mut shutdown_rx) = oneshot::channel::<()>();
    let shutdown_tx = Arc::new(Mutex::new(Some(shutdown_tx)));

    let (ws_shutdown_tx, ws_shutdown_rx) = tokio::sync::watch::channel(false);
    {
        let token_for_ws = load_token(&profile)?;
        let client_for_ws = MattermostClient::new(&profile, token_for_ws.clone())?;
        let event_bus = Arc::clone(&event_bus);
        let ws = Arc::new(MattermostWs::new(
            &profile,
            token_for_ws,
            client_for_ws,
            event_bus,
            state.my_user_id.clone(),
        ));
        let ws_state = ws.ws_state();
        let ws_ref = Arc::clone(&ws);
        tokio::spawn(async move {
            ws_ref.run(ws_shutdown_rx).await;
        });
        *ws_state_holder.lock().await = Some(ws_state);
    }

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
                cancel_token.cancel();
                let _ = ws_shutdown_tx.send(true);
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

pub async fn ping(profile_name: &str) -> Result<DaemonStatus, DaemonError> {
    daemon_client(profile_name).daemon_status().await
}

pub async fn stop(profile_name: &str) -> Result<(), DaemonError> {
    daemon_client(profile_name).shutdown().await?;
    Ok(())
}

pub async fn status(profile_name: &str) -> Result<DaemonStatus, DaemonError> {
    daemon_client(profile_name).daemon_status().await
}

async fn handle_client(
    stream: UnixStream,
    state: Arc<AppState>,
    shutdown_tx: Arc<Mutex<Option<oneshot::Sender<()>>>>,
) -> Result<(), DaemonError> {
    let (reader, mut writer) = stream.into_split();
    let mut reader = BufReader::new(reader);
    let mut line = String::new();
    let mut event_rx: Option<tokio::sync::broadcast::Receiver<Arc<DaemonEvent>>> = None;
    let mut client_sub_ids: Vec<String> = Vec::new();

    loop {
        tokio::select! {
            read_result = reader.read_line(&mut line) => {
                if read_result? == 0 {
                    break;
                }
                let request: JsonRpcRequest = serde_json::from_str(line.trim_end())?;
                let unsub_id = if request.method == "unsubscribe" {
                    request.params.get("subscription_id").and_then(|v| v.as_str()).map(|s| s.to_string())
                } else {
                    None
                };
                let response = dispatch_request(request, &state, &shutdown_tx).await;

                if let Some(sub_ack) = extract_subscription_id(&response.result) {
                    client_sub_ids.push(sub_ack);
                    if event_rx.is_none() {
                        event_rx = Some(state.event_bus.subscribe());
                    }
                }

                if let Some(removed_id) = unsub_id {
                    client_sub_ids.retain(|id| id != &removed_id);
                }

                writer
                    .write_all(serde_json::to_string(&response)?.as_bytes())
                    .await?;
                writer.write_all(b"\n").await?;
                line.clear();
            }
            recv_result = async {
                match event_rx.as_mut() {
                    Some(rx) => rx.recv().await,
                    None => std::future::pending().await,
                }
            } => {
                match recv_result {
                    Ok(event) => {
                        let subs = state.subscriptions.lock().await;
                        let matches_any = client_sub_ids.iter().any(|id| {
                            subs.get(id)
                                .is_some_and(|f| event_matches_filter(event.as_ref(), f))
                        });
                        if matches_any {
                            let notification = daemon_event_to_notification(event.as_ref());
                            let payload = serde_json::to_string(&notification)?;
                            writer.write_all(payload.as_bytes()).await?;
                            writer.write_all(b"\n").await?;
                        }
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                        let subs = state.subscriptions.lock().await;
                        let current_seq = state.event_bus.current_seq();
                        let missed_from = current_seq.saturating_sub(n);
                        for sub_id in &client_sub_ids {
                            if subs.contains_key(sub_id) {
                                let gap = JsonRpcNotification {
                                    jsonrpc: "2.0".to_string(),
                                    method: "push.gap".to_string(),
                                    params: serde_json::json!({
                                        "subscription_id": sub_id,
                                        "missed_from_seq": missed_from,
                                        "missed_to_seq": current_seq,
                                    }),
                                };
                                let payload = serde_json::to_string(&gap)?;
                                writer.write_all(payload.as_bytes()).await?;
                                writer.write_all(b"\n").await?;
                            }
                        }
                    }
                    _ => {}
                }
            }
        }
    }
    if !client_sub_ids.is_empty() {
        let mut subs = state.subscriptions.lock().await;
        for id in &client_sub_ids {
            subs.remove(id);
        }
    }
    Ok(())
}

fn extract_subscription_id(result: &Option<serde_json::Value>) -> Option<String> {
    result
        .as_ref()
        .and_then(|v| v.get("subscription_id"))
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
}

fn event_matches_filter(event: &DaemonEvent, filter: &SubscriptionFilter) -> bool {
    match filter {
        SubscriptionFilter::AllMonitored => matches!(
            event.kind,
            DaemonEventKind::InboundMessage
                | DaemonEventKind::InboundMention
                | DaemonEventKind::ConnectionStateChanged
        ),
        SubscriptionFilter::ChannelByName(name) => {
            let channel_matches = match &event.payload {
                DaemonEventPayloadInner::Inbound(p) => p.channel_name.eq_ignore_ascii_case(name),
                _ => true,
            };
            channel_matches
                && matches!(
                    event.kind,
                    DaemonEventKind::InboundMessage
                        | DaemonEventKind::InboundMention
                        | DaemonEventKind::ConnectionStateChanged
                )
        }
        SubscriptionFilter::MentionsOnly => matches!(
            event.kind,
            DaemonEventKind::InboundMention | DaemonEventKind::ConnectionStateChanged
        ),
        SubscriptionFilter::ConnectionState => {
            matches!(event.kind, DaemonEventKind::ConnectionStateChanged)
        }
    }
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
        "list_dms" => state
            .client
            .list_dms()
            .await
            .map(to_value)
            .map_err(DaemonError::from),
        "read_channel" => parse_and_call(&request.params, |params: ReadChannelParams| async move {
            if let Some(after_post_id) = params.after_post_id {
                state
                    .client
                    .read_channel_after(&params.channel, &after_post_id)
                    .await
            } else if params.since_last_mine {
                state
                    .client
                    .read_channel_since_last_mine(&params.channel)
                    .await
            } else {
                state
                    .client
                    .read_channel(&params.channel, params.since_minutes.unwrap_or(60))
                    .await
            }
        })
        .await
        .map(to_value),
        "check_channel" => {
            parse_and_call(&request.params, |params: CheckChannelParams| async move {
                check_channel(state, &params.channel, params.after_post_id.as_deref()).await
            })
            .await
            .map(to_value)
        }
        "post_message" => parse_and_call(&request.params, |params: PostMessageParams| async move {
            let receipt = state
                .client
                .post_message(&params.channel, &params.message)
                .await?;
            record_channel_cursor(state, &params.channel, &receipt.id).await?;
            Ok(receipt)
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
                if params.unread_only {
                    unread_notifications(state)
                        .await
                        .map(NotificationsResponse::Unread)
                } else {
                    let notifications = state
                        .client
                        .notifications(params.since_minutes.unwrap_or(1440))
                        .await?;
                    record_notifications_cursor(state, &notifications).await?;
                    Ok(NotificationsResponse::Messages(notifications))
                }
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
        "subscribe" => parse_and_call(&request.params, |params: SubscribeParams| async move {
            let sub_id = uuid::Uuid::new_v4().to_string();
            let start_seq = state.event_bus.current_seq();
            state
                .subscriptions
                .lock()
                .await
                .insert(sub_id.clone(), params.filter);
            Ok(SubscriptionAck {
                subscription_id: sub_id,
                start_sequence: start_seq,
            })
        })
        .await
        .map(to_value),
        "unsubscribe" => parse_and_call(&request.params, |params: UnsubscribeParams| async move {
            let mut subs = state.subscriptions.lock().await;
            Ok(subs.remove(&params.subscription_id).is_some())
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
        "daemon_status" => {
            use std::sync::atomic::Ordering;
            let ws_snapshot = {
                let ws_guard = state.ws_state_holder.lock().await;
                match ws_guard.as_ref() {
                    Some(ws) => {
                        let conn = *ws.connection_state.lock().await;
                        let last = ws.last_event_at.load(Ordering::Relaxed);
                        let err = ws.last_error.lock().await.clone();
                        let rc = ws.reconnect_count.load(Ordering::Relaxed);
                        let ldx = ws.last_disconnect_at.load(Ordering::Relaxed);
                        let lrx = ws.last_recovered_at.load(Ordering::Relaxed);
                        let gap = ws.suspected_gap.load(Ordering::Relaxed);
                        let ru = ws.recovering_until.load(Ordering::Relaxed);
                        chanvoy_core::WsStatusSnapshot {
                            connection_state: Some(conn),
                            last_event_at: if last > 0 { Some(last) } else { None },
                            last_error: err,
                            reconnect_count: Some(rc),
                            last_disconnect_at: if ldx > 0 { Some(ldx) } else { None },
                            last_recovered_at: if lrx > 0 { Some(lrx) } else { None },
                            suspected_gap: Some(gap),
                            recovering_until: ru,
                        }
                    }
                    None => chanvoy_core::WsStatusSnapshot {
                        connection_state: None,
                        last_event_at: None,
                        last_error: None,
                        reconnect_count: None,
                        last_disconnect_at: None,
                        last_recovered_at: None,
                        suspected_gap: None,
                        recovering_until: 0,
                    },
                }
            };
            let ipc_snapshot = match &state.ipc_state {
                Some(s) => {
                    let g = s.lock().await;
                    chanvoy_core::IpcStatusSnapshot {
                        connected: Some(g.connected),
                        peer_id: g.peer_id.clone(),
                        reconnect_count: Some(g.reconnect_count),
                    }
                }
                None => chanvoy_core::IpcStatusSnapshot {
                    connected: None,
                    peer_id: None,
                    reconnect_count: None,
                },
            };
            let whoami_result = match state.client.whoami().await {
                Ok(identity) => Ok(identity.username),
                Err(e) => Err(e.to_string()),
            };
            Ok(to_value(chanvoy_core::build_daemon_status(
                state.profile.name.clone(),
                state.socket_path.clone(),
                state.profile.bot_username.clone(),
                whoami_result,
                ws_snapshot,
                ipc_snapshot,
                now_unix_millis(),
            )))
        }
        "seed_cursors" => seed_cursors(state)
            .await
            .map(to_value)
            .map_err(DaemonError::from),
        "attention_list" => Ok(to_value(attention_list(state).await)),
        "attention_show" => {
            parse_and_call(&request.params, |params: AttentionShowParams| async move {
                Ok::<_, CoreError>(attention_show(state, &params.channel).await)
            })
            .await
            .map(to_value)
        }
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
        DaemonError::Core(CoreError::WaitTimeout(_)) => -32005,
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

    let initial = state
        .client
        .latest_channel_messages_by_id(&channel_id, 30)
        .await?;

    let cursor_id = initial.last().map(|m| m.id.clone()).unwrap_or_default();
    let cursor_create_at = initial.last().map(|m| m.create_at).unwrap_or(0);

    let is_monitored = state
        .profile
        .monitored_channels
        .iter()
        .any(|m| m.eq_ignore_ascii_case(channel));

    let limit = Duration::from_secs(timeout_minutes * 60);

    if is_monitored {
        wait_push_backed(
            state,
            channel,
            &channel_id,
            &cursor_id,
            cursor_create_at,
            limit,
        )
        .await
    } else {
        wait_rest_poll(
            state,
            channel,
            &channel_id,
            &cursor_id,
            cursor_create_at,
            limit,
        )
        .await
    }
}

async fn wait_push_backed(
    state: &AppState,
    channel: &str,
    channel_id: &str,
    cursor_id: &str,
    cursor_create_at: i64,
    limit: Duration,
) -> Result<WaitResult, CoreError> {
    let mut rx = state.event_bus.subscribe();

    let future = async {
        loop {
            match rx.recv().await {
                Ok(event) => match &event.payload {
                    DaemonEventPayloadInner::Inbound(p)
                        if p.channel_name.eq_ignore_ascii_case(channel) =>
                    {
                        if p.post_id != cursor_id
                            && p.create_at > cursor_create_at
                            && p.sender_id != state.my_user_id
                        {
                            return Ok(WaitResult {
                                channel: channel.to_string(),
                                messages: vec![chanvoy_core::Message {
                                    id: p.post_id.clone(),
                                    user_id: p.sender_id.clone(),
                                    username: p.sender_username.clone(),
                                    message: p.message.clone(),
                                    create_at: p.create_at,
                                }],
                            });
                        }
                    }
                    _ => {}
                },
                Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                    let messages = state
                        .client
                        .latest_channel_messages_by_id(channel_id, 30)
                        .await?;
                    let fresh: Vec<_> = messages
                        .into_iter()
                        .filter(|m| {
                            m.user_id != state.my_user_id
                                && m.id != cursor_id
                                && m.create_at > cursor_create_at
                        })
                        .collect();
                    if !fresh.is_empty() {
                        return Ok(WaitResult {
                            channel: channel.to_string(),
                            messages: fresh,
                        });
                    }
                }
                Err(_) => {}
            }
        }
    };

    timeout(limit, future)
        .await
        .map_err(|_| CoreError::WaitTimeout(channel.to_string()))?
}

async fn wait_rest_poll(
    state: &AppState,
    channel: &str,
    channel_id: &str,
    cursor_id: &str,
    cursor_create_at: i64,
    limit: Duration,
) -> Result<WaitResult, CoreError> {
    let my_user_id = state.my_user_id.clone();
    let channel_name = channel.to_string();
    let future: std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<WaitResult, CoreError>> + Send + '_>,
    > = Box::pin(async {
        loop {
            let messages = state
                .client
                .latest_channel_messages_by_id(channel_id, 10)
                .await?;
            let fresh: Vec<_> = messages
                .into_iter()
                .filter(|m| {
                    m.user_id != my_user_id && m.id != cursor_id && m.create_at > cursor_create_at
                })
                .collect();
            if !fresh.is_empty() {
                return Ok(WaitResult {
                    channel: channel_name.clone(),
                    messages: fresh,
                });
            }
            sleep(Duration::from_secs(2)).await;
        }
    });
    timeout(limit, future)
        .await
        .map_err(|_| CoreError::WaitTimeout(channel.to_string()))?
}

async fn check_channel(
    state: &AppState,
    channel: &str,
    explicit_after: Option<&str>,
) -> Result<CheckResult, CoreError> {
    let (anchor, anchor_source) = if let Some(after) = explicit_after {
        (Some(after.to_string()), "explicit_after".to_string())
    } else {
        let attention = state.attention_state.lock().await;
        let Some(cursor) = attention.channels.get(channel) else {
            return Ok(CheckResult {
                channel: channel.to_string(),
                anchor: None,
                anchor_source: "no_anchor".to_string(),
                has_new_messages: false,
                count: 0,
                newest_post_id: None,
            });
        };
        (
            cursor.last_seen_post_id.clone(),
            if cursor.last_seen_post_id.is_some() {
                "daemon_cursor".to_string()
            } else {
                "no_anchor".to_string()
            },
        )
    };

    let Some(anchor_post_id) = anchor.clone() else {
        return Ok(CheckResult {
            channel: channel.to_string(),
            anchor,
            anchor_source,
            has_new_messages: false,
            count: 0,
            newest_post_id: None,
        });
    };

    let messages = match state
        .client
        .read_channel_after(channel, &anchor_post_id)
        .await
    {
        Ok(messages) => {
            // Freshness verdict: cursor probed and anchor still present.
            // Persist `last_known_stale=false` + `last_checked_at=now` so
            // `attention list` / `show` can surface the verdict without
            // its own probe (PER-008B D1: cached staleness, cxotech's
            // `last_checked_at` refinement).
            if anchor_source == "daemon_cursor" {
                record_staleness_verdict(state, channel, false).await;
            }
            messages
        }
        Err(CoreError::AnchorNotFound(_)) | Err(CoreError::AnchorChannelMismatch { .. })
            if anchor_source == "daemon_cursor" =>
        {
            record_staleness_verdict(state, channel, true).await;
            return Ok(stale_cursor_check_result(channel));
        }
        Err(error) => return Err(error),
    };
    let fresh: Vec<_> = messages
        .into_iter()
        .filter(|message| message.user_id != state.my_user_id)
        .collect();
    let newest_post_id = fresh.last().map(|message| message.id.clone());
    Ok(CheckResult {
        channel: channel.to_string(),
        anchor,
        anchor_source,
        has_new_messages: !fresh.is_empty(),
        count: fresh.len(),
        newest_post_id,
    })
}

async fn record_channel_cursor(
    state: &AppState,
    channel: &str,
    post_id: &str,
) -> Result<(), CoreError> {
    let mut attention = state.attention_state.lock().await;
    // Every cursor-write path is a staleness-clearing event per the
    // PER-008B D1 guardrail: the new cursor value is fresh, by definition
    // not stale, and has not yet been checked.
    attention.channels.insert(
        channel.to_string(),
        chanvoy_core::ChannelCursorState {
            last_seen_post_id: Some(post_id.to_string()),
            updated_at: Some(chanvoy_core::now_unix_millis()),
            last_known_stale: false,
            last_checked_at: None,
        },
    );
    store_attention_state(&state.profile.name, &attention)?;
    Ok(())
}

async fn record_notifications_cursor(
    state: &AppState,
    notifications: &[chanvoy_core::Notification],
) -> Result<(), CoreError> {
    let Some(last) = notifications.last() else {
        return Ok(());
    };

    let mut attention = state.attention_state.lock().await;
    attention.mentions = chanvoy_core::MentionCursorState {
        last_seen_post_id: Some(last.message.id.clone()),
        updated_at: Some(chanvoy_core::now_unix_millis()),
    };
    store_attention_state(&state.profile.name, &attention)?;
    Ok(())
}

/// Record a channel cursor only if none already exists for that channel.
/// Returns true if the cursor was written (channel was unseeded). Monotonic
/// guard — PER-009 seed pass must never clobber an existing anchor.
async fn record_channel_cursor_if_absent(
    state: &AppState,
    channel: &str,
    post_id: &str,
) -> Result<bool, CoreError> {
    let mut attention = state.attention_state.lock().await;
    if attention.channels.contains_key(channel) {
        return Ok(false);
    }
    // A freshly-seeded cursor is by definition non-stale and unchecked.
    attention.channels.insert(
        channel.to_string(),
        chanvoy_core::ChannelCursorState {
            last_seen_post_id: Some(post_id.to_string()),
            updated_at: Some(chanvoy_core::now_unix_millis()),
            last_known_stale: false,
            last_checked_at: None,
        },
    );
    store_attention_state(&state.profile.name, &attention)?;
    Ok(true)
}

/// Persist the staleness verdict for a channel cursor. Updates both
/// `last_known_stale` and `last_checked_at` (Unix ms) on the existing
/// cursor entry, without touching `last_seen_post_id` / `updated_at`
/// (the cursor value itself is unchanged — only our knowledge of its
/// freshness).
///
/// No-op if the channel has no persisted cursor (staleness is a cursor
/// attribute, not a channel attribute). Errors on attention-state
/// persistence are logged but not returned — staleness cache is a
/// best-effort optimization for `attention list`'s fast path, and
/// failing a `check_channel` call because we couldn't persist the
/// verdict would be the wrong trade.
async fn record_staleness_verdict(state: &AppState, channel: &str, stale: bool) {
    let mut attention = state.attention_state.lock().await;
    let Some(cursor) = attention.channels.get_mut(channel) else {
        return;
    };
    cursor.last_known_stale = stale;
    cursor.last_checked_at = Some(chanvoy_core::now_unix_millis());
    if let Err(err) = store_attention_state(&state.profile.name, &attention) {
        tracing::warn!(
            profile = %state.profile.name,
            channel = %channel,
            %err,
            "failed to persist staleness verdict; attention list may show stale verdict"
        );
    }
}

/// Build `AttentionListResult` from the daemon's current attention state.
/// Pure-read; never mutates. Powers the `attention list` RPC.
///
/// Channel set is the union of:
/// - `state.profile.monitored_channels` — operator-declared tracked
///   channels, which may not yet have a persisted cursor (surface as
///   `no_anchor` so the operator can see "this channel is tracked but
///   uncursored"). Without this, AC #1 / #6 would miss the tracked-
///   but-uncursored state — exactly the operator view the brief's
///   example output illustrates (devrev finding, 2026-04-22).
/// - `attention.channels.keys()` — channels with at least one
///   persisted cursor. This includes post-established cursors for
///   channels that may or may not be in `monitored_channels`.
///
/// Emitting a `BTreeSet` union gives stable lexicographic ordering
/// across runs.
async fn attention_list(state: &AppState) -> chanvoy_core::AttentionListResult {
    let attention = state.attention_state.lock().await;
    let mut channel_names: std::collections::BTreeSet<String> =
        state.profile.monitored_channels.iter().cloned().collect();
    channel_names.extend(attention.channels.keys().cloned());
    let channels = channel_names
        .into_iter()
        .map(|name| match attention.channels.get(&name) {
            Some(cursor) => chanvoy_core::AttentionChannelEntry {
                channel: name,
                source: attention_source_for_channel(cursor),
                newest_seen: cursor.last_seen_post_id.clone(),
                updated_at: cursor.updated_at,
                last_checked_at: cursor.last_checked_at,
            },
            None => chanvoy_core::AttentionChannelEntry {
                channel: name,
                source: chanvoy_core::AttentionSource::NoAnchor,
                newest_seen: None,
                updated_at: None,
                last_checked_at: None,
            },
        })
        .collect();
    let mentions = chanvoy_core::AttentionMentionEntry {
        source: attention_source_for_mentions(&attention.mentions),
        newest_seen: attention.mentions.last_seen_post_id.clone(),
        updated_at: attention.mentions.updated_at,
    };
    chanvoy_core::AttentionListResult {
        profile: state.profile.name.clone(),
        channels,
        mentions,
    }
}

/// Build `AttentionShowResult` for a specific channel. Returns an entry
/// with `source = NoAnchor` when the channel is not tracked, rather than
/// erroring — operators asking about an untracked channel want that
/// confirmed, not a bare error.
async fn attention_show(state: &AppState, channel: &str) -> chanvoy_core::AttentionShowResult {
    let attention = state.attention_state.lock().await;
    let entry = match attention.channels.get(channel) {
        Some(cursor) => chanvoy_core::AttentionChannelEntry {
            channel: channel.to_string(),
            source: attention_source_for_channel(cursor),
            newest_seen: cursor.last_seen_post_id.clone(),
            updated_at: cursor.updated_at,
            last_checked_at: cursor.last_checked_at,
        },
        None => chanvoy_core::AttentionChannelEntry {
            channel: channel.to_string(),
            source: chanvoy_core::AttentionSource::NoAnchor,
            newest_seen: None,
            updated_at: None,
            last_checked_at: None,
        },
    };
    let mentions = chanvoy_core::AttentionMentionEntry {
        source: attention_source_for_mentions(&attention.mentions),
        newest_seen: attention.mentions.last_seen_post_id.clone(),
        updated_at: attention.mentions.updated_at,
    };
    chanvoy_core::AttentionShowResult {
        profile: state.profile.name.clone(),
        channel: entry,
        mentions,
    }
}

fn attention_source_for_channel(
    cursor: &chanvoy_core::ChannelCursorState,
) -> chanvoy_core::AttentionSource {
    if cursor.last_seen_post_id.is_none() {
        chanvoy_core::AttentionSource::NoAnchor
    } else if cursor.last_known_stale {
        chanvoy_core::AttentionSource::StaleCursor
    } else {
        chanvoy_core::AttentionSource::PostCursor
    }
}

fn attention_source_for_mentions(
    mentions: &chanvoy_core::MentionCursorState,
) -> chanvoy_core::AttentionSource {
    if mentions.last_seen_post_id.is_some() {
        chanvoy_core::AttentionSource::NotificationsCursor
    } else {
        chanvoy_core::AttentionSource::NoAnchor
    }
}

/// Seed cursors for bot-member channels that do not yet have a stored cursor.
/// Implements PER-009 option (b): seed only on explicit auto-setup, never clobber,
/// leave empty channels explicitly unseeded, surface per-channel failures.
async fn seed_cursors(state: &AppState) -> Result<chanvoy_core::SeedCursorsResult, CoreError> {
    let existing: std::collections::BTreeSet<String> = {
        let attention = state.attention_state.lock().await;
        attention.channels.keys().cloned().collect()
    };
    // Enumeration + HEAD fetch lives in chanvoy_core for wiremock-testable isolation.
    // This wrapper serializes writes through the attention-state mutex and filters
    // post_message races via the no-clobber helper.
    let outcomes = chanvoy_core::compute_seed_outcomes(&state.client, &existing).await?;
    let mut persisted: Vec<chanvoy_core::SeededChannelOutcome> = Vec::new();
    let mut newly_seeded: Vec<String> = Vec::new();
    for outcome in outcomes {
        match outcome {
            chanvoy_core::SeededChannelOutcome::Seeded { channel, post_id } => {
                match record_channel_cursor_if_absent(state, &channel, &post_id).await {
                    Ok(true) => {
                        newly_seeded.push(channel.clone());
                        persisted
                            .push(chanvoy_core::SeededChannelOutcome::Seeded { channel, post_id });
                    }
                    Ok(false) => {
                        // Lost a race with another writer (e.g., post_message between
                        // the pre-filter read and the record). Existing cursor wins
                        // under the monotonic rule — surface nothing.
                    }
                    Err(err) => {
                        persisted.push(chanvoy_core::SeededChannelOutcome::Failed {
                            channel,
                            reason: err.to_string(),
                        });
                    }
                }
            }
            other => persisted.push(other),
        }
    }
    if !newly_seeded.is_empty() {
        info!(
            profile = %state.profile.name,
            channels = ?newly_seeded,
            "chanvoy auto-setup seeded cursors"
        );
    }
    Ok(chanvoy_core::SeedCursorsResult {
        outcomes: persisted,
    })
}

async fn unread_notifications(state: &AppState) -> Result<UnreadNotifications, CoreError> {
    let anchor = {
        let attention = state.attention_state.lock().await;
        attention.mentions.last_seen_post_id.clone()
    };

    let mentions = state
        .client
        .unread_notification_mentions_since(anchor.as_deref())
        .await?;
    Ok(UnreadNotifications {
        count: mentions.len(),
    })
}

fn stale_cursor_check_result(channel: &str) -> CheckResult {
    CheckResult {
        channel: channel.to_string(),
        anchor: None,
        anchor_source: "stale_cursor".to_string(),
        has_new_messages: false,
        count: 0,
        newest_post_id: None,
    }
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

    pub async fn list_dms(&self) -> Result<Vec<DmConversation>, DaemonError> {
        self.call("list_dms", serde_json::json!({})).await
    }

    pub async fn read_channel(
        &self,
        channel: &str,
        since_minutes: Option<u64>,
        after_post_id: Option<String>,
        since_last_mine: bool,
    ) -> Result<Vec<chanvoy_core::Message>, DaemonError> {
        self.call(
            "read_channel",
            serde_json::to_value(ReadChannelParams {
                channel: channel.to_string(),
                since_minutes,
                after_post_id,
                since_last_mine,
            })?,
        )
        .await
    }

    pub async fn check_channel(
        &self,
        channel: &str,
        after_post_id: Option<String>,
    ) -> Result<CheckResult, DaemonError> {
        self.call(
            "check_channel",
            serde_json::to_value(CheckChannelParams {
                channel: channel.to_string(),
                after_post_id,
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
        unread_only: bool,
    ) -> Result<serde_json::Value, DaemonError> {
        self.call(
            "notifications",
            serde_json::to_value(NotificationsParams {
                since_minutes: Some(since_minutes),
                unread_only,
            })?,
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

    pub async fn daemon_status(&self) -> Result<DaemonStatus, DaemonError> {
        self.call("daemon_status", serde_json::json!({})).await
    }

    pub async fn attention_list(&self) -> Result<chanvoy_core::AttentionListResult, DaemonError> {
        self.call("attention_list", serde_json::json!({})).await
    }

    pub async fn attention_show(
        &self,
        channel: &str,
    ) -> Result<chanvoy_core::AttentionShowResult, DaemonError> {
        self.call(
            "attention_show",
            serde_json::to_value(AttentionShowParams {
                channel: channel.to_string(),
            })?,
        )
        .await
    }

    pub async fn seed_cursors(&self) -> Result<chanvoy_core::SeedCursorsResult, DaemonError> {
        self.call("seed_cursors", serde_json::json!({})).await
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stale_daemon_cursor_degrades_to_nonfatal_probe() {
        let result = stale_cursor_check_result("per-008");
        assert_eq!(result.channel, "per-008");
        assert_eq!(result.anchor_source, "stale_cursor");
        assert_eq!(result.anchor, None);
        assert!(!result.has_new_messages);
        assert_eq!(result.count, 0);
        assert_eq!(result.newest_post_id, None);
    }
    use chanvoy_core::{
        DaemonEvent, DaemonEventKind, DaemonEventPayloadInner, EventBus, InboundEventPayload,
        Provider, SubscriptionFilter,
    };
    use std::sync::Arc;

    fn inbound_event(channel_name: &str, mentioned: bool) -> DaemonEvent {
        DaemonEvent {
            seq: 0,
            kind: if mentioned {
                DaemonEventKind::InboundMention
            } else {
                DaemonEventKind::InboundMessage
            },
            payload: DaemonEventPayloadInner::Inbound(InboundEventPayload {
                profile: "test".to_string(),
                provider: Provider::Mattermost,
                channel_id: "ch1".to_string(),
                channel_name: channel_name.to_string(),
                post_id: "p1".to_string(),
                sender_id: "u1".to_string(),
                sender_username: "alice".to_string(),
                message: if mentioned {
                    "@agent-bravo-devlead hi".to_string()
                } else {
                    "hello".to_string()
                },
                create_at: 1000,
                received_at: 1001,
                mentioned,
            }),
        }
    }

    #[test]
    fn filter_all_monitored_matches_inbound_message() {
        let event = inbound_event("per-004", false);
        assert!(event_matches_filter(
            &event,
            &SubscriptionFilter::AllMonitored
        ));
    }

    #[test]
    fn filter_all_monitored_matches_mention() {
        let event = inbound_event("per-004", true);
        assert!(event_matches_filter(
            &event,
            &SubscriptionFilter::AllMonitored
        ));
    }

    #[test]
    fn filter_all_monitored_matches_connection_state() {
        let event = DaemonEvent {
            seq: 1,
            kind: DaemonEventKind::ConnectionStateChanged,
            payload: DaemonEventPayloadInner::ConnectionStateChanged(
                chanvoy_core::ConnectionStateChangedPayload {
                    profile: "test".to_string(),
                    provider: Provider::Mattermost,
                    state: chanvoy_core::WsConnectionState::Healthy,
                    message: "ok".to_string(),
                },
            ),
        };
        assert!(event_matches_filter(
            &event,
            &SubscriptionFilter::AllMonitored
        ));
    }

    #[test]
    fn filter_channel_by_name_matches_only_target_channel() {
        let event_per004 = inbound_event("per-004", false);
        let event_per003 = inbound_event("per-003", false);
        assert!(event_matches_filter(
            &event_per004,
            &SubscriptionFilter::ChannelByName("per-004".to_string())
        ));
        assert!(!event_matches_filter(
            &event_per003,
            &SubscriptionFilter::ChannelByName("per-004".to_string())
        ));
    }

    #[test]
    fn filter_mentions_only_rejects_plain_message() {
        let event = inbound_event("per-004", false);
        assert!(!event_matches_filter(
            &event,
            &SubscriptionFilter::MentionsOnly
        ));
    }

    #[test]
    fn filter_mentions_only_accepts_mention() {
        let event = inbound_event("per-004", true);
        assert!(event_matches_filter(
            &event,
            &SubscriptionFilter::MentionsOnly
        ));
    }

    #[test]
    fn filter_connection_state_rejects_inbound() {
        let event = inbound_event("per-004", false);
        assert!(!event_matches_filter(
            &event,
            &SubscriptionFilter::ConnectionState
        ));
    }

    #[tokio::test]
    async fn multiple_subscribers_isolated_filters() {
        let bus = Arc::new(EventBus::new(16));

        let sub_a_id = "sub-a".to_string();
        let sub_b_id = "sub-b".to_string();

        let mut subs: HashMap<String, SubscriptionFilter> = HashMap::new();
        subs.insert(
            sub_a_id.clone(),
            SubscriptionFilter::ChannelByName("per-004".to_string()),
        );
        subs.insert(sub_b_id.clone(), SubscriptionFilter::MentionsOnly);

        let mut rx = bus.subscribe();

        bus.emit(inbound_event("per-004", false));
        bus.emit(inbound_event("bravo-team", true));
        bus.emit(inbound_event("per-003", false));

        let e1 = rx.recv().await.unwrap();
        let e2 = rx.recv().await.unwrap();
        let e3 = rx.recv().await.unwrap();

        let filter_a = subs.get(&sub_a_id).unwrap();
        let filter_b = subs.get(&sub_b_id).unwrap();

        assert!(event_matches_filter(&e1, filter_a));
        assert!(!event_matches_filter(&e1, filter_b));

        assert!(event_matches_filter(&e2, filter_b));
        assert!(!event_matches_filter(&e2, filter_a));

        assert!(!event_matches_filter(&e3, filter_a));
        assert!(!event_matches_filter(&e3, filter_b));
    }

    #[tokio::test]
    async fn unsubscribe_removes_filter_and_stops_matching() {
        let bus = Arc::new(EventBus::new(16));
        let mut subs: HashMap<String, SubscriptionFilter> = HashMap::new();

        let sub_id = "sub-test".to_string();
        subs.insert(
            sub_id.clone(),
            SubscriptionFilter::ChannelByName("per-004".to_string()),
        );

        let mut rx = bus.subscribe();

        bus.emit(inbound_event("per-004", false));
        let e1 = rx.recv().await.unwrap();
        assert!(event_matches_filter(&e1, subs.get(&sub_id).unwrap()));

        subs.remove(&sub_id);
        assert!(!subs.contains_key(&sub_id));

        bus.emit(inbound_event("per-004", false));
        let e2 = rx.recv().await.unwrap();
        assert!(!subs.contains_key(&sub_id));
        assert!(!subs.values().any(|f| event_matches_filter(&e2, f)));
    }

    #[test]
    fn client_sub_ids_prune_on_unsubscribe() {
        let mut client_sub_ids: Vec<String> = vec![
            "sub-a".to_string(),
            "sub-b".to_string(),
            "sub-c".to_string(),
        ];
        let removed_id = "sub-b".to_string();
        client_sub_ids.retain(|id| id != &removed_id);
        assert_eq!(client_sub_ids, vec!["sub-a", "sub-c"]);
    }
}
