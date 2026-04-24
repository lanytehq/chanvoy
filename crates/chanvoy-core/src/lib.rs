use std::collections::BTreeMap;
use std::env;
use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use futures_util::{SinkExt, StreamExt};
use reqwest::{Client, StatusCode};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;
use tokio::sync::{broadcast, Mutex};
use tokio::time::{sleep, MissedTickBehavior};
use tokio_tungstenite::{connect_async, tungstenite::Message as WsMessage};
use tracing::{info, warn};
use uuid::Uuid;

pub const DEFAULT_TEAM: &str = "org-lanytehq";
pub const DEFAULT_NOTIFICATIONS_CHANNEL: &str = "agent-notifications";
pub const WAIT_POLL_SECONDS: u64 = 2;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Provider {
    Mattermost,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CredentialMode {
    EnvName,
    EnvFile,
    SeclusorRun,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CapabilityClass {
    Standard,
    Elevated,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Profile {
    pub name: String,
    pub role: String,
    pub scope: String,
    pub provider: Provider,
    pub bot_username: String,
    #[serde(default = "default_team_name")]
    pub team_name: String,
    pub server_url: String,
    #[serde(alias = "token_env")]
    pub env_name: String,
    #[serde(default)]
    pub env_file: Option<PathBuf>,
    #[serde(default = "default_credential_mode")]
    pub credential_mode: CredentialMode,
    #[serde(default = "default_capability_class")]
    pub capability_class: CapabilityClass,
    #[serde(default)]
    pub monitored_channels: Vec<String>,
    #[serde(default)]
    pub ipc: Option<IpcConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct IpcConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub gateway_socket: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProfileStatus {
    pub profile_name: String,
    pub role: String,
    pub scope: String,
    pub provider: Provider,
    pub bot_username: String,
    pub server_url: String,
    pub socket_path: PathBuf,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Identity {
    pub id: String,
    pub username: String,
    #[serde(default)]
    pub is_bot: bool,
    #[serde(default)]
    pub nickname: Option<String>,
    #[serde(default)]
    pub email: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Channel {
    pub id: String,
    pub name: String,
    pub display_name: String,
    #[serde(rename = "type")]
    pub channel_type: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Message {
    pub id: String,
    pub user_id: String,
    pub username: String,
    pub message: String,
    pub create_at: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PostReceipt {
    pub id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct WaitResult {
    pub channel: String,
    pub messages: Vec<Message>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Notification {
    pub from_channel: String,
    pub message: Message,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MentionSummary {
    pub post_id: String,
    pub create_at: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DmConversation {
    pub id: String,
    pub name: String,
    pub last_post_at: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ErrorDetail {
    pub code: i64,
    pub message: String,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum WsConnectionState {
    Disconnected,
    Connecting,
    Healthy,
    Degraded,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum DaemonHealthState {
    Disconnected,
    Connecting,
    Healthy,
    Degraded,
    Recovering,
}

pub const RECOVERY_GRACE_MS: i64 = 10_000;

/// Upper bound for the Mattermost reachability probe used by
/// `daemon_status`. Short enough that the status surface stays
/// responsive when REST is stalled (the same outage class PER-010 is
/// about), long enough to tolerate normal network latency on a
/// healthy path. PER-010, entarch.
pub const STATUS_PROBE_TIMEOUT_MS: u64 = 2_000;

/// Time-bound a Mattermost identity probe. On success returns the
/// username; on remote error or local timeout returns a printable
/// error string, ready to feed `build_daemon_status` as
/// `whoami_result: Err(...)`.
///
/// `daemon_status` must return promptly even when the reachability
/// probe does not complete — a stalled REST call would otherwise wedge
/// the RPC exactly when the operator needs the reconnect-health
/// surface to diagnose. PER-010, entarch.
pub async fn probe_whoami(client: &MattermostClient, timeout_ms: u64) -> Result<String, String> {
    match tokio::time::timeout(Duration::from_millis(timeout_ms), client.whoami()).await {
        Ok(Ok(identity)) => Ok(identity.username),
        Ok(Err(e)) => Err(e.to_string()),
        Err(_) => Err(format!(
            "reachability probe timed out after {}ms",
            timeout_ms
        )),
    }
}

/// Snapshot of websocket state for building a `DaemonStatus` without
/// holding any locks. All fields reflect a single atomic read moment.
pub struct WsStatusSnapshot {
    pub connection_state: Option<WsConnectionState>,
    pub last_event_at: Option<i64>,
    pub last_error: Option<String>,
    pub reconnect_count: Option<u64>,
    pub last_disconnect_at: Option<i64>,
    pub last_recovered_at: Option<i64>,
    pub suspected_gap: Option<bool>,
    pub recovering_until: i64,
}

/// Snapshot of IPC state for building a `DaemonStatus` without holding
/// any locks.
pub struct IpcStatusSnapshot {
    pub connected: Option<bool>,
    pub peer_id: Option<String>,
    pub reconnect_count: Option<u64>,
}

/// Build a `DaemonStatus` from pre-computed snapshots.
///
/// Pure function — no mutation, no I/O. Intended for `daemon_status` to
/// remain a local read that does not fail when Mattermost reachability
/// is lost: `whoami` errors surface as data (`mattermost_ok=false`,
/// `mattermost_last_error=Some`, `mattermost_username` falls back to
/// the profile's configured bot username) rather than failing the RPC.
///
/// This matters in exactly the outage class PER-010 addresses — a
/// sleep/wake or transient network loss can take down the REST path
/// alongside the WS, and that is when the operator most needs to see
/// the new reconnect-health fields. Making status pure-local keeps the
/// surface available during the failure window. PER-010, entarch.
#[allow(clippy::too_many_arguments)]
pub fn build_daemon_status(
    profile_name: String,
    socket_path: PathBuf,
    configured_bot_username: String,
    whoami_result: Result<String, String>,
    ws: WsStatusSnapshot,
    ipc: IpcStatusSnapshot,
    now_millis: i64,
) -> DaemonStatus {
    let (mattermost_username, mattermost_ok, mattermost_last_error) = match whoami_result {
        Ok(username) => (username, true, None),
        Err(msg) => (configured_bot_username, false, Some(msg)),
    };
    let health = derive_daemon_health(
        now_millis,
        ws.connection_state,
        ws.suspected_gap.unwrap_or(false),
        ws.recovering_until,
    );
    DaemonStatus {
        profile_name,
        socket_path,
        mattermost_username,
        mattermost_ok,
        ws_connection_state: ws.connection_state,
        ws_last_event_at: ws.last_event_at,
        ws_last_error: ws.last_error,
        ws_reconnect_count: ws.reconnect_count,
        ipc_connected: ipc.connected,
        ipc_peer_id: ipc.peer_id,
        ipc_reconnect_count: ipc.reconnect_count,
        health,
        ws_last_disconnect_at: ws.last_disconnect_at,
        ws_last_recovered_at: ws.last_recovered_at,
        ws_suspected_gap: ws.suspected_gap,
        mattermost_last_error,
    }
}

pub fn derive_daemon_health(
    now_millis: i64,
    connection_state: Option<WsConnectionState>,
    suspected_gap: bool,
    recovering_until_millis: i64,
) -> Option<DaemonHealthState> {
    let state = connection_state?;
    Some(match state {
        WsConnectionState::Disconnected => DaemonHealthState::Disconnected,
        WsConnectionState::Connecting => DaemonHealthState::Connecting,
        WsConnectionState::Degraded => DaemonHealthState::Degraded,
        WsConnectionState::Healthy => {
            if now_millis < recovering_until_millis || suspected_gap {
                DaemonHealthState::Recovering
            } else {
                DaemonHealthState::Healthy
            }
        }
    })
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum DaemonEventKind {
    InboundMessage,
    InboundMention,
    ConnectionStateChanged,
    Gap,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct InboundEventPayload {
    pub profile: String,
    pub provider: Provider,
    pub channel_id: String,
    pub channel_name: String,
    pub post_id: String,
    pub sender_id: String,
    pub sender_username: String,
    pub message: String,
    pub create_at: i64,
    pub received_at: i64,
    pub mentioned: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ConnectionStateChangedPayload {
    pub profile: String,
    pub provider: Provider,
    pub state: WsConnectionState,
    pub message: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct GapPayload {
    pub subscription_id: String,
    pub missed_from_seq: u64,
    pub missed_to_seq: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DaemonEvent {
    pub seq: u64,
    pub kind: DaemonEventKind,
    #[serde(flatten)]
    pub payload: DaemonEventPayloadInner,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(untagged)]
pub enum DaemonEventPayloadInner {
    Inbound(InboundEventPayload),
    ConnectionStateChanged(ConnectionStateChangedPayload),
    Gap(GapPayload),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SubscriptionFilter {
    AllMonitored,
    ChannelByName(String),
    MentionsOnly,
    ConnectionState,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SubscribeParams {
    pub filter: SubscriptionFilter,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct UnsubscribeParams {
    pub subscription_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SubscriptionAck {
    pub subscription_id: String,
    pub start_sequence: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JsonRpcNotification {
    pub jsonrpc: String,
    pub method: String,
    pub params: Value,
}

pub fn daemon_event_to_notification(event: &DaemonEvent) -> JsonRpcNotification {
    let method = match event.kind {
        DaemonEventKind::InboundMessage => "push.inbound_message",
        DaemonEventKind::InboundMention => "push.inbound_mention",
        DaemonEventKind::ConnectionStateChanged => "push.connection_state_changed",
        DaemonEventKind::Gap => "push.gap",
    };
    JsonRpcNotification {
        jsonrpc: "2.0".to_string(),
        method: method.to_string(),
        params: serde_json::to_value(event).unwrap_or_default(),
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct JsonRpcRequest {
    pub jsonrpc: String,
    pub id: Uuid,
    pub method: String,
    #[serde(default)]
    pub params: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct JsonRpcResponse {
    pub jsonrpc: String,
    pub id: Uuid,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<ErrorDetail>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReadChannelParams {
    pub channel: String,
    pub since_minutes: Option<u64>,
    #[serde(default)]
    pub after_post_id: Option<String>,
    #[serde(default)]
    pub since_last_mine: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PostMessageParams {
    pub channel: String,
    pub message: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DirectMessageParams {
    pub username: String,
    pub message: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReadDirectMessageParams {
    pub username: String,
    pub since_minutes: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct NotificationsParams {
    pub since_minutes: Option<u64>,
    #[serde(default)]
    pub unread_only: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CheckChannelParams {
    pub channel: String,
    #[serde(default)]
    pub after_post_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AttentionShowParams {
    pub channel: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CheckResult {
    pub channel: String,
    #[serde(default)]
    pub anchor: Option<String>,
    pub anchor_source: String,
    pub has_new_messages: bool,
    pub count: usize,
    #[serde(default)]
    pub newest_post_id: Option<String>,
}

/// Per-channel outcome of a seed-cursors pass. See PER-009 / #per-009 for the contract.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum SeededChannelOutcome {
    Seeded { channel: String, post_id: String },
    UnseededEmptyChannel { channel: String },
    Failed { channel: String, reason: String },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct SeedCursorsResult {
    pub outcomes: Vec<SeededChannelOutcome>,
}

/// Discriminator for the source of a channel's attention anchor, surfaced
/// by `attention list` / `attention show`. Matches the vocabulary used by
/// `CheckResult.anchor_source` so operator-parsing agents don't need a
/// second vocabulary.
///
/// - `NoAnchor`: channel is tracked but no cursor value — first-use or
///   freshly cleared state.
/// - `PostCursor`: cursor established by a successful `post_message`.
/// - `NotificationsCursor`: only valid on the mentions sibling structure;
///   kept here for symmetry so wire shape is stable across list / show.
/// - `StaleCursor`: daemon's last `check_channel` pass on this cursor
///   observed `AnchorNotFound` / `AnchorChannelMismatch`. Persisted
///   via `ChannelCursorState::last_known_stale`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum AttentionSource {
    #[default]
    NoAnchor,
    PostCursor,
    NotificationsCursor,
    StaleCursor,
}

/// One row in `attention list`'s channels table.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AttentionChannelEntry {
    pub channel: String,
    pub source: AttentionSource,
    #[serde(default)]
    pub newest_seen: Option<String>,
    #[serde(default)]
    pub updated_at: Option<i64>,
    #[serde(default)]
    pub last_checked_at: Option<i64>,
}

/// Mention-cursor entry for `attention list`'s `mentions` sibling.
/// Mentions are profile-scoped, not channel-scoped, so they surface
/// outside the channels table.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct AttentionMentionEntry {
    pub source: AttentionSource,
    #[serde(default)]
    pub newest_seen: Option<String>,
    #[serde(default)]
    pub updated_at: Option<i64>,
}

/// `attention list` wire shape — channels table + mentions sibling.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct AttentionListResult {
    pub profile: String,
    pub channels: Vec<AttentionChannelEntry>,
    pub mentions: AttentionMentionEntry,
}

/// `attention show <channel>` wire shape. Includes the channel entry
/// plus the profile-scoped mention state (cheap to surface alongside).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AttentionShowResult {
    pub profile: String,
    pub channel: AttentionChannelEntry,
    pub mentions: AttentionMentionEntry,
}

/// Enumerate bot memberships and compute the per-channel seed outcome for PER-009
/// option (b). Writes are the caller's responsibility — typically the daemon, which
/// serializes them under its attention-state mutex via a no-clobber helper.
///
/// - Returns `Seeded{channel, post_id}` for bot-member channels that had no entry in
///   `existing_cursors` and whose latest post fetch succeeded.
/// - Returns `UnseededEmptyChannel{channel}` for joined channels with zero posts
///   (intentional absence, not a failure — does not degrade readiness).
/// - Returns `Failed{channel, reason}` for per-channel HEAD-fetch errors. Does not
///   abort the pass; degrades overall readiness when aggregated.
/// - Skips DM (`D`) and group-DM (`G`) pseudo-channels; those are addressed via the
///   mentions cursor.
pub async fn compute_seed_outcomes(
    client: &MattermostClient,
    existing_cursors: &std::collections::BTreeSet<String>,
) -> Result<Vec<SeededChannelOutcome>, CoreError> {
    let channels = client.list_channels().await?;
    let mut outcomes = Vec::new();
    for channel in channels {
        if channel.channel_type != "O" && channel.channel_type != "P" {
            continue;
        }
        if existing_cursors.contains(&channel.name) {
            continue;
        }
        let head = match client.latest_channel_messages_by_id(&channel.id, 1).await {
            Ok(posts) => posts,
            Err(err) => {
                outcomes.push(SeededChannelOutcome::Failed {
                    channel: channel.name,
                    reason: err.to_string(),
                });
                continue;
            }
        };
        let Some(latest) = head.last() else {
            outcomes.push(SeededChannelOutcome::UnseededEmptyChannel {
                channel: channel.name,
            });
            continue;
        };
        outcomes.push(SeededChannelOutcome::Seeded {
            channel: channel.name,
            post_id: latest.id.clone(),
        });
    }
    Ok(outcomes)
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct UnreadNotifications {
    pub count: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct AttentionState {
    #[serde(default)]
    pub channels: BTreeMap<String, ChannelCursorState>,
    #[serde(default)]
    pub mentions: MentionCursorState,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct ChannelCursorState {
    #[serde(default)]
    pub last_seen_post_id: Option<String>,
    #[serde(default)]
    pub updated_at: Option<i64>,
    /// Last-known staleness verdict for this cursor. Set true when
    /// `check_channel` detects the anchor post has been deleted
    /// (`AnchorNotFound`) or moved channels (`AnchorChannelMismatch`).
    /// Cleared on any successful cursor write. Powers `attention list`'s
    /// `stale_cursor` discriminator without per-call Mattermost probes —
    /// consistent with the strict-read-only contract on the `attention`
    /// prefix (see PER-008B amendment).
    ///
    /// `#[serde(default)]` preserves on-disk back-compat for state files
    /// written by earlier daemon versions.
    #[serde(default)]
    pub last_known_stale: bool,
    /// Unix-millis timestamp of the last `check_channel` pass that
    /// probed this cursor's anchor (success or stale). Distinct from
    /// `updated_at` (cursor-value update time). Surfaced by
    /// `attention list` / `show` so operators can tell "freshly verified"
    /// from "never checked since establishment."
    #[serde(default)]
    pub last_checked_at: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct MentionCursorState {
    #[serde(default)]
    pub last_seen_post_id: Option<String>,
    #[serde(default)]
    pub updated_at: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct NotifyParams {
    pub bot_username: String,
    pub message: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct WaitChannelParams {
    pub channel: String,
    pub timeout_minutes: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CreateChannelParams {
    pub name: String,
    pub display_name: String,
    #[serde(default)]
    pub purpose: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ArchiveChannelParams {
    pub name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AddMemberParams {
    pub channel: String,
    pub username: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ShutdownResult {
    pub stopping: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DaemonHealth {
    pub profile: String,
    pub socket_path: PathBuf,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DaemonStatus {
    pub profile_name: String,
    pub socket_path: PathBuf,
    pub mattermost_username: String,
    pub mattermost_ok: bool,
    #[serde(default)]
    pub ws_connection_state: Option<WsConnectionState>,
    #[serde(default)]
    pub ws_last_event_at: Option<i64>,
    #[serde(default)]
    pub ws_last_error: Option<String>,
    #[serde(default)]
    pub ws_reconnect_count: Option<u64>,
    #[serde(default)]
    pub ipc_connected: Option<bool>,
    #[serde(default)]
    pub ipc_peer_id: Option<String>,
    #[serde(default)]
    pub ipc_reconnect_count: Option<u64>,
    #[serde(default)]
    pub health: Option<DaemonHealthState>,
    #[serde(default)]
    pub ws_last_disconnect_at: Option<i64>,
    #[serde(default)]
    pub ws_last_recovered_at: Option<i64>,
    #[serde(default)]
    pub ws_suspected_gap: Option<bool>,
    #[serde(default)]
    pub mattermost_last_error: Option<String>,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct ConfigFile {
    pub mattermost: Option<MattermostConfig>,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct MattermostConfig {
    pub server_url: Option<String>,
    pub team_name: Option<String>,
    pub bot_token: Option<String>,
}

#[derive(Debug, Error)]
pub enum CoreError {
    #[error("missing credential source {0}")]
    MissingCredential(String),
    #[error("credential mode env_file requires --env-file in the profile")]
    MissingEnvFile,
    #[error("profile bot username mismatch: expected {expected}, got {actual}")]
    ProfileIdentityMismatch { expected: String, actual: String },
    #[error("anchor post {0} not found")]
    AnchorNotFound(String),
    #[error("anchor post {post_id} is not in channel {channel}")]
    AnchorChannelMismatch { post_id: String, channel: String },
    #[error("no prior authored post found in channel {channel} for {username}")]
    NoPriorAuthoredPost { channel: String, username: String },
    #[error("no stored cursor exists for channel {channel}")]
    NoStoredCursor { channel: String },
    #[error("operation requires elevated capability")]
    RequiresElevatedCapability,
    #[error("timeout waiting for channel {0}")]
    WaitTimeout(String),
    #[error("profile {0} not found")]
    ProfileNotFound(String),
    #[error("unknown provider in profile")]
    UnsupportedProvider,
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("toml parse error: {0}")]
    TomlDe(#[from] toml::de::Error),
    #[error("toml serialize error: {0}")]
    TomlSer(#[from] toml::ser::Error),
    #[error("http error: {0}")]
    Http(#[from] reqwest::Error),
    #[error("serialization error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("api error {status}: {message}")]
    Api { status: StatusCode, message: String },
}

pub fn default_profile_dir() -> PathBuf {
    default_chanvoy_config_dir().join("profiles")
}

pub fn default_chanvoy_config_dir() -> PathBuf {
    // `CHANVOY_CONFIG_DIR` is an explicit override used by the integration test
    // harness for cross-platform isolation (the `dirs` crate does not honor
    // XDG_CONFIG_HOME on macOS), and available to operators on non-standard
    // systems. When unset, fall through to the platform-conventional location.
    if let Some(dir) = env::var_os("CHANVOY_CONFIG_DIR") {
        return PathBuf::from(dir);
    }
    dirs::config_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join("lanytehq/chanvoy")
}

pub fn default_runtime_dir() -> PathBuf {
    // `CHANVOY_RUNTIME_DIR` mirrors the config-dir override for symmetry.
    // Otherwise prefer XDG_RUNTIME_DIR, then dirs::runtime_dir, then temp_dir.
    if let Some(dir) = env::var_os("CHANVOY_RUNTIME_DIR") {
        return PathBuf::from(dir);
    }
    env::var_os("XDG_RUNTIME_DIR")
        .map(PathBuf::from)
        .or_else(dirs::runtime_dir)
        .unwrap_or_else(env::temp_dir)
        .join("chanvoy")
}

pub fn socket_path_for_profile(profile: &str) -> PathBuf {
    default_runtime_dir().join(format!("{profile}.sock"))
}

pub fn pid_path_for_profile(profile: &str) -> PathBuf {
    default_runtime_dir().join(format!("{profile}.pid"))
}

pub fn active_profile_path() -> PathBuf {
    default_chanvoy_config_dir().join("active_profile")
}

pub fn attention_state_path(profile_name: &str) -> PathBuf {
    default_chanvoy_config_dir().join(format!("state-{profile_name}.json"))
}

pub fn parse_env_file(path: &Path) -> Result<BTreeMap<String, String>, CoreError> {
    let contents = fs::read_to_string(path)?;
    let mut values = BTreeMap::new();
    for line in contents.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }
        let normalized = trimmed.strip_prefix("export ").unwrap_or(trimmed);
        let Some((key, value)) = normalized.split_once('=') else {
            continue;
        };
        values.insert(
            key.trim().to_string(),
            strip_quotes(value.trim()).to_string(),
        );
    }
    Ok(values)
}

fn strip_quotes(input: &str) -> &str {
    input
        .strip_prefix('"')
        .and_then(|s| s.strip_suffix('"'))
        .unwrap_or(input)
}

pub fn load_profile(name: &str) -> Result<Profile, CoreError> {
    let path = default_profile_dir().join(format!("{name}.toml"));
    let contents = fs::read_to_string(&path).map_err(|err| {
        if err.kind() == std::io::ErrorKind::NotFound {
            CoreError::ProfileNotFound(name.to_string())
        } else {
            CoreError::Io(err)
        }
    })?;
    let mut profile: Profile = toml::from_str(&contents)?;
    if profile.name.is_empty() {
        profile.name = name.to_string();
    }
    Ok(profile)
}

pub fn load_active_profile() -> Result<Option<String>, CoreError> {
    let path = active_profile_path();
    match fs::read_to_string(path) {
        Ok(contents) => {
            let trimmed = contents.trim();
            if trimmed.is_empty() {
                Ok(None)
            } else {
                Ok(Some(trimmed.to_string()))
            }
        }
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(err) => Err(CoreError::Io(err)),
    }
}

pub fn store_active_profile(name: &str) -> Result<PathBuf, CoreError> {
    let dir = default_chanvoy_config_dir();
    fs::create_dir_all(&dir)?;
    fs::set_permissions(&dir, fs::Permissions::from_mode(0o700))?;
    let path = active_profile_path();
    fs::write(&path, format!("{name}\n"))?;
    fs::set_permissions(&path, fs::Permissions::from_mode(0o600))?;
    Ok(path)
}

pub fn list_profiles() -> Result<Vec<Profile>, CoreError> {
    let mut profiles = Vec::new();
    let profile_dir = default_profile_dir();
    if !profile_dir.exists() {
        return Ok(profiles);
    }
    for entry in fs::read_dir(profile_dir)? {
        let path = entry?.path();
        if path.extension().and_then(|ext| ext.to_str()) != Some("toml") {
            continue;
        }
        let contents = fs::read_to_string(&path)?;
        let profile: Profile = toml::from_str(&contents)?;
        profiles.push(profile);
    }
    profiles.sort_by(|left, right| left.name.cmp(&right.name));
    Ok(profiles)
}

pub fn store_profile(profile: &Profile) -> Result<PathBuf, CoreError> {
    let dir = default_profile_dir();
    fs::create_dir_all(&dir)?;
    fs::set_permissions(&dir, fs::Permissions::from_mode(0o700))?;
    let path = dir.join(format!("{}.toml", profile.name));
    fs::write(&path, toml::to_string_pretty(profile)?)?;
    fs::set_permissions(&path, fs::Permissions::from_mode(0o600))?;
    Ok(path)
}

pub fn load_attention_state(profile_name: &str) -> Result<AttentionState, CoreError> {
    let path = attention_state_path(profile_name);
    match fs::read_to_string(path) {
        Ok(contents) => Ok(serde_json::from_str(&contents)?),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(AttentionState::default()),
        Err(err) => Err(CoreError::Io(err)),
    }
}

pub fn store_attention_state(
    profile_name: &str,
    state: &AttentionState,
) -> Result<PathBuf, CoreError> {
    let dir = default_chanvoy_config_dir();
    fs::create_dir_all(&dir)?;
    fs::set_permissions(&dir, fs::Permissions::from_mode(0o700))?;
    let path = attention_state_path(profile_name);
    let tmp_path = path.with_extension("json.tmp");
    fs::write(&tmp_path, serde_json::to_string_pretty(state)?)?;
    fs::set_permissions(&tmp_path, fs::Permissions::from_mode(0o600))?;
    fs::rename(&tmp_path, &path)?;
    Ok(path)
}

pub fn load_token(profile: &Profile) -> Result<String, CoreError> {
    match profile.credential_mode {
        CredentialMode::EnvName | CredentialMode::SeclusorRun => {
            if let Ok(value) = env::var(&profile.env_name) {
                if !value.is_empty() {
                    return Ok(value);
                }
            }
            Err(CoreError::MissingCredential(profile.env_name.clone()))
        }
        CredentialMode::EnvFile => {
            let path = profile.env_file.as_ref().ok_or(CoreError::MissingEnvFile)?;
            let env_values = parse_env_file(path)?;
            if let Some(value) = env_values.get(&profile.env_name) {
                if !value.is_empty() {
                    return Ok(value.clone());
                }
            }
            Err(CoreError::MissingCredential(profile.env_name.clone()))
        }
    }
}

pub fn now_unix_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

pub fn minutes_ago_millis(minutes: u64) -> i64 {
    now_unix_millis() - Duration::from_secs(minutes * 60).as_millis() as i64
}

#[derive(Clone)]
pub struct MattermostClient {
    base_url: String,
    team_name: String,
    token: String,
    client: Client,
}

impl MattermostClient {
    pub fn new(profile: &Profile, token: String) -> Result<Self, CoreError> {
        let client = Client::builder().user_agent("chanvoy/0.1.0").build()?;
        Ok(Self {
            base_url: profile.server_url.trim_end_matches('/').to_string(),
            team_name: profile.team_name.clone(),
            token,
            client,
        })
    }

    pub async fn whoami(&self) -> Result<Identity, CoreError> {
        #[derive(Deserialize)]
        struct RawUser {
            id: String,
            username: String,
            #[serde(default)]
            is_bot: bool,
            nickname: Option<String>,
            email: Option<String>,
        }
        let user: RawUser = self.request("GET", "/users/me", None::<Value>).await?;
        Ok(Identity {
            id: user.id,
            username: user.username,
            is_bot: user.is_bot,
            nickname: user.nickname,
            email: user.email,
        })
    }

    pub async fn list_channels(&self) -> Result<Vec<Channel>, CoreError> {
        let team_id = self.team_id().await?;
        #[derive(Deserialize)]
        struct RawChannel {
            id: String,
            name: String,
            display_name: String,
            #[serde(rename = "type")]
            channel_type: String,
        }
        let channels: Vec<RawChannel> = self
            .request(
                "GET",
                &format!("/users/me/teams/{team_id}/channels"),
                None::<Value>,
            )
            .await?;
        Ok(channels
            .into_iter()
            .map(|channel| Channel {
                id: channel.id,
                name: channel.name,
                display_name: channel.display_name,
                channel_type: channel.channel_type,
            })
            .collect())
    }

    pub async fn read_channel(
        &self,
        channel_name: &str,
        since_minutes: u64,
    ) -> Result<Vec<Message>, CoreError> {
        let channel_id = self.channel_id(channel_name).await?;
        let since = minutes_ago_millis(since_minutes);
        #[derive(Deserialize)]
        struct RawPost {
            id: String,
            user_id: String,
            message: String,
            create_at: i64,
            username: Option<String>,
        }
        #[derive(Deserialize)]
        struct PostsResponse {
            posts: BTreeMap<String, RawPost>,
        }
        let response: PostsResponse = self
            .request(
                "GET",
                &format!("/channels/{channel_id}/posts?since={since}&per_page=30"),
                None::<Value>,
            )
            .await?;
        let mut posts: Vec<Message> = response
            .posts
            .into_values()
            .map(|post| Message {
                id: post.id,
                user_id: post.user_id,
                username: post.username.unwrap_or_else(|| "unknown".to_string()),
                message: post.message,
                create_at: post.create_at,
            })
            .collect();
        posts.sort_by_key(|message| message.create_at);
        Ok(posts)
    }

    pub async fn read_channel_after(
        &self,
        channel_name: &str,
        after_post_id: &str,
    ) -> Result<Vec<Message>, CoreError> {
        let channel_id = self.channel_id(channel_name).await?;
        self.assert_post_in_channel(&channel_id, channel_name, after_post_id)
            .await?;

        #[derive(Deserialize)]
        struct RawPost {
            id: String,
            user_id: String,
            message: String,
            create_at: i64,
            username: Option<String>,
        }

        #[derive(Deserialize)]
        struct PostsResponse {
            posts: BTreeMap<String, RawPost>,
        }

        let mut page = 0;
        let mut messages = Vec::new();

        loop {
            let response: PostsResponse = self
                .request(
                    "GET",
                    &format!(
                        "/channels/{channel_id}/posts?after={after_post_id}&page={page}&per_page=200"
                    ),
                    None::<Value>,
                )
                .await?;

            let mut page_messages: Vec<Message> = response
                .posts
                .into_values()
                .map(|post| Message {
                    id: post.id,
                    user_id: post.user_id,
                    username: post.username.unwrap_or_else(|| "unknown".to_string()),
                    message: post.message,
                    create_at: post.create_at,
                })
                .collect();

            if page_messages.is_empty() {
                break;
            }

            page_messages.sort_by_key(|message| message.create_at);
            let page_len = page_messages.len();
            messages.extend(page_messages);

            if page_len < 200 {
                break;
            }

            page += 1;
        }

        messages.sort_by(|left, right| {
            left.create_at
                .cmp(&right.create_at)
                .then_with(|| left.id.cmp(&right.id))
        });

        Ok(messages)
    }

    pub async fn read_channel_since_last_mine(
        &self,
        channel_name: &str,
    ) -> Result<Vec<Message>, CoreError> {
        let my_username = self.whoami().await?.username;
        let after_post_id = self
            .latest_authored_post_id(channel_name, &my_username)
            .await?
            .ok_or_else(|| CoreError::NoPriorAuthoredPost {
                channel: channel_name.to_string(),
                username: my_username.clone(),
            })?;

        self.read_channel_after(channel_name, &after_post_id).await
    }

    pub async fn post_message(
        &self,
        channel_name: &str,
        message: &str,
    ) -> Result<PostReceipt, CoreError> {
        let channel_id = self.channel_id(channel_name).await?;
        #[derive(Serialize)]
        struct Payload<'a> {
            channel_id: &'a str,
            message: &'a str,
        }
        #[derive(Deserialize)]
        struct RawPostReceipt {
            id: String,
        }
        let receipt: RawPostReceipt = self
            .request(
                "POST",
                "/posts",
                Some(Payload {
                    channel_id: &channel_id,
                    message,
                }),
            )
            .await?;
        Ok(PostReceipt { id: receipt.id })
    }

    pub async fn direct_message(
        &self,
        username: &str,
        message: &str,
    ) -> Result<PostReceipt, CoreError> {
        let user_id = self.user_id(username).await?;
        let my_id = self.whoami().await?.id;
        let channel_id: ChannelIdResponse = self
            .request("POST", "/channels/direct", Some(vec![my_id, user_id]))
            .await?;
        self.post_message_by_id(&channel_id.id, message).await
    }

    pub async fn read_dm(
        &self,
        username: &str,
        since_minutes: u64,
    ) -> Result<Vec<Message>, CoreError> {
        let user_id = self.user_id(username).await?;
        let my_id = self.whoami().await?.id;
        let channel_id: ChannelIdResponse = self
            .request("POST", "/channels/direct", Some(vec![my_id, user_id]))
            .await?;
        self.read_channel_by_id(&channel_id.id, since_minutes).await
    }

    pub async fn notify(
        &self,
        bot_username: &str,
        message: &str,
    ) -> Result<PostReceipt, CoreError> {
        self.post_message(
            DEFAULT_NOTIFICATIONS_CHANNEL,
            &format!("@{bot_username} **[notify]** {message}"),
        )
        .await
    }

    pub async fn notifications(&self, since_minutes: u64) -> Result<Vec<Notification>, CoreError> {
        let my_username = self.whoami().await?.username;
        let messages = self
            .read_channel(DEFAULT_NOTIFICATIONS_CHANNEL, since_minutes)
            .await?;
        let notifications = messages
            .into_iter()
            .filter(|message| message_mentions_username(&message.message, &my_username))
            .map(|message| Notification {
                from_channel: DEFAULT_NOTIFICATIONS_CHANNEL.to_string(),
                message,
            })
            .collect();
        Ok(notifications)
    }

    pub async fn unread_notification_mentions_since(
        &self,
        after_post_id: Option<&str>,
    ) -> Result<Vec<MentionSummary>, CoreError> {
        let my_username = self.whoami().await?.username;
        let messages = match after_post_id {
            Some(post_id) => {
                self.read_channel_after(DEFAULT_NOTIFICATIONS_CHANNEL, post_id)
                    .await?
            }
            None => {
                self.latest_channel_messages(DEFAULT_NOTIFICATIONS_CHANNEL, 200)
                    .await?
            }
        };

        Ok(messages
            .into_iter()
            .filter(|message| message_mentions_username(&message.message, &my_username))
            .map(|message| MentionSummary {
                post_id: message.id,
                create_at: message.create_at,
            })
            .collect())
    }

    pub async fn validate_team_access(&self) -> Result<(), CoreError> {
        let _ = self.team_id().await?;
        Ok(())
    }

    pub async fn list_dms(&self) -> Result<Vec<DmConversation>, CoreError> {
        let my_id = self.whoami().await?.id;

        #[derive(Deserialize)]
        struct RawDmChannel {
            id: String,
            name: String,
            #[serde(rename = "type")]
            channel_type: String,
            last_post_at: i64,
        }

        let raw_channels: Vec<RawDmChannel> = self
            .request(
                "GET",
                &format!("/users/{my_id}/channels?per_page=100"),
                None::<Value>,
            )
            .await?;

        let mut channels: Vec<DmConversation> = raw_channels
            .into_iter()
            .filter(|channel: &RawDmChannel| channel.channel_type == "D")
            .map(|channel| DmConversation {
                id: channel.id,
                name: channel.name,
                last_post_at: channel.last_post_at,
            })
            .collect();

        channels.sort_by(|left, right| right.last_post_at.cmp(&left.last_post_at));
        channels.truncate(20);
        Ok(channels)
    }

    pub async fn create_channel(
        &self,
        name: &str,
        display_name: &str,
        purpose: Option<String>,
    ) -> Result<Channel, CoreError> {
        let team_id = self.team_id().await?;
        #[derive(Serialize)]
        struct Payload<'a> {
            team_id: &'a str,
            name: &'a str,
            display_name: &'a str,
            #[serde(rename = "type")]
            channel_type: &'static str,
            #[serde(skip_serializing_if = "Option::is_none")]
            purpose: Option<String>,
        }
        #[derive(Deserialize)]
        struct RawChannel {
            id: String,
            name: String,
            display_name: String,
            #[serde(rename = "type")]
            channel_type: String,
        }
        let channel: RawChannel = self
            .request(
                "POST",
                "/channels",
                Some(Payload {
                    team_id: &team_id,
                    name,
                    display_name,
                    channel_type: "O",
                    purpose,
                }),
            )
            .await?;
        Ok(Channel {
            id: channel.id,
            name: channel.name,
            display_name: channel.display_name,
            channel_type: channel.channel_type,
        })
    }

    pub async fn archive_channel(&self, channel_name: &str) -> Result<(), CoreError> {
        let channel_id = self.channel_id(channel_name).await?;
        let _: Value = self
            .request("DELETE", &format!("/channels/{channel_id}"), None::<Value>)
            .await?;
        Ok(())
    }

    pub async fn restore_channel(&self, channel_name: &str) -> Result<(), CoreError> {
        let channel_id = self.channel_id(channel_name).await?;
        let _: Value = self
            .request(
                "POST",
                &format!("/channels/{channel_id}/restore"),
                None::<Value>,
            )
            .await?;
        Ok(())
    }

    pub async fn add_member(&self, channel_name: &str, username: &str) -> Result<(), CoreError> {
        let channel_id = self.channel_id(channel_name).await?;
        let user_id = self.user_id(username).await?;
        #[derive(Serialize)]
        struct Payload<'a> {
            user_id: &'a str,
        }
        let _: Value = self
            .request(
                "POST",
                &format!("/channels/{channel_id}/members"),
                Some(Payload { user_id: &user_id }),
            )
            .await?;
        Ok(())
    }

    async fn read_channel_by_id(
        &self,
        channel_id: &str,
        since_minutes: u64,
    ) -> Result<Vec<Message>, CoreError> {
        let since = minutes_ago_millis(since_minutes);
        self.read_channel_by_id_since_millis(channel_id, since)
            .await
    }

    pub async fn read_channel_by_id_since_millis(
        &self,
        channel_id: &str,
        since_millis: i64,
    ) -> Result<Vec<Message>, CoreError> {
        #[derive(Deserialize)]
        struct RawPost {
            id: String,
            user_id: String,
            message: String,
            create_at: i64,
            username: Option<String>,
        }
        #[derive(Deserialize)]
        struct PostsResponse {
            posts: BTreeMap<String, RawPost>,
        }
        let response: PostsResponse = self
            .request(
                "GET",
                &format!("/channels/{channel_id}/posts?since={since_millis}&per_page=30"),
                None::<Value>,
            )
            .await?;
        let mut posts: Vec<Message> = response
            .posts
            .into_values()
            .map(|post| Message {
                id: post.id,
                user_id: post.user_id,
                username: post.username.unwrap_or_else(|| "unknown".to_string()),
                message: post.message,
                create_at: post.create_at,
            })
            .collect();
        posts.sort_by_key(|message| message.create_at);
        Ok(posts)
    }

    pub async fn post_message_by_id(
        &self,
        channel_id: &str,
        message: &str,
    ) -> Result<PostReceipt, CoreError> {
        #[derive(Serialize)]
        struct Payload<'a> {
            channel_id: &'a str,
            message: &'a str,
        }
        #[derive(Deserialize)]
        struct RawPostReceipt {
            id: String,
        }
        let receipt: RawPostReceipt = self
            .request(
                "POST",
                "/posts",
                Some(Payload {
                    channel_id,
                    message,
                }),
            )
            .await?;
        Ok(PostReceipt { id: receipt.id })
    }

    pub async fn post_threaded_reply(
        &self,
        channel_id: &str,
        root_id: &str,
        message: &str,
    ) -> Result<PostReceipt, CoreError> {
        #[derive(Serialize)]
        struct Payload<'a> {
            channel_id: &'a str,
            root_id: &'a str,
            message: &'a str,
        }
        #[derive(Deserialize)]
        struct RawPostReceipt {
            id: String,
        }
        let receipt: RawPostReceipt = self
            .request(
                "POST",
                "/posts",
                Some(Payload {
                    channel_id,
                    root_id,
                    message,
                }),
            )
            .await?;
        Ok(PostReceipt { id: receipt.id })
    }

    pub async fn read_thread(&self, root_post_id: &str) -> Result<Vec<Message>, CoreError> {
        #[derive(Deserialize)]
        struct RawPost {
            id: String,
            user_id: String,
            message: String,
            create_at: i64,
            username: Option<String>,
        }
        #[derive(Deserialize)]
        struct ThreadResponse {
            posts: BTreeMap<String, RawPost>,
        }
        let response: ThreadResponse = self
            .request(
                "GET",
                &format!("/posts/{root_post_id}/thread"),
                None::<Value>,
            )
            .await?;
        let mut posts: Vec<Message> = response
            .posts
            .into_values()
            .filter_map(|p| {
                p.username.map(|username| Message {
                    id: p.id,
                    user_id: p.user_id,
                    username,
                    message: p.message,
                    create_at: p.create_at,
                })
            })
            .collect();
        posts.sort_by_key(|m| m.create_at);
        Ok(posts)
    }

    async fn team_id(&self) -> Result<String, CoreError> {
        #[derive(Deserialize)]
        struct TeamResponse {
            id: String,
        }
        let team: TeamResponse = self
            .request(
                "GET",
                &format!("/teams/name/{}", self.team_name),
                None::<Value>,
            )
            .await?;
        Ok(team.id)
    }

    pub async fn channel_id_for_name(&self, channel_name: &str) -> Result<String, CoreError> {
        self.channel_id(channel_name).await
    }

    pub async fn latest_channel_messages(
        &self,
        channel_name: &str,
        per_page: u64,
    ) -> Result<Vec<Message>, CoreError> {
        let channel_id = self.channel_id(channel_name).await?;
        self.latest_channel_messages_by_id(&channel_id, per_page)
            .await
    }

    pub async fn latest_channel_messages_by_id(
        &self,
        channel_id: &str,
        per_page: u64,
    ) -> Result<Vec<Message>, CoreError> {
        #[derive(Deserialize)]
        struct RawPost {
            id: String,
            user_id: String,
            message: String,
            create_at: i64,
            username: Option<String>,
        }
        #[derive(Deserialize)]
        struct PostsResponse {
            posts: BTreeMap<String, RawPost>,
        }
        let response: PostsResponse = self
            .request(
                "GET",
                &format!("/channels/{channel_id}/posts?per_page={per_page}"),
                None::<Value>,
            )
            .await?;
        let mut posts: Vec<Message> = response
            .posts
            .into_values()
            .map(|post| Message {
                id: post.id,
                user_id: post.user_id,
                username: post.username.unwrap_or_else(|| "unknown".to_string()),
                message: post.message,
                create_at: post.create_at,
            })
            .collect();
        posts.sort_by_key(|message| message.create_at);
        Ok(posts)
    }

    async fn latest_authored_post_id(
        &self,
        channel_name: &str,
        username: &str,
    ) -> Result<Option<String>, CoreError> {
        let team_id = self.team_id().await?;

        #[derive(Serialize)]
        struct SearchPayload {
            terms: String,
            is_or_search: bool,
            page: u64,
            per_page: u64,
        }

        #[derive(Deserialize)]
        struct RawPost {
            id: String,
            create_at: i64,
        }

        #[derive(Deserialize)]
        struct SearchResponse {
            posts: BTreeMap<String, RawPost>,
        }

        let response: SearchResponse = self
            .request(
                "POST",
                &format!("/teams/{team_id}/posts/search"),
                Some(SearchPayload {
                    terms: format!("from:{username} in:{channel_name}"),
                    is_or_search: false,
                    page: 0,
                    per_page: 1,
                }),
            )
            .await?;

        Ok(response
            .posts
            .into_values()
            .max_by_key(|post| post.create_at)
            .map(|post| post.id))
    }

    async fn channel_id(&self, channel_name: &str) -> Result<String, CoreError> {
        let team_id = self.team_id().await?;
        #[derive(Deserialize)]
        struct ChannelResponse {
            id: String,
        }
        let channel: ChannelResponse = self
            .request(
                "GET",
                &format!("/teams/{team_id}/channels/name/{channel_name}"),
                None::<Value>,
            )
            .await?;
        Ok(channel.id)
    }

    async fn user_id(&self, username: &str) -> Result<String, CoreError> {
        #[derive(Deserialize)]
        struct UserResponse {
            id: String,
        }
        let user: UserResponse = self
            .request("GET", &format!("/users/username/{username}"), None::<Value>)
            .await?;
        Ok(user.id)
    }

    async fn assert_post_in_channel(
        &self,
        expected_channel_id: &str,
        channel_name: &str,
        post_id: &str,
    ) -> Result<(), CoreError> {
        #[derive(Deserialize)]
        struct PostResponse {
            channel_id: String,
        }

        let post: PostResponse = match self
            .request("GET", &format!("/posts/{post_id}"), None::<Value>)
            .await
        {
            Ok(post) => post,
            Err(CoreError::Api {
                status: StatusCode::NOT_FOUND,
                ..
            }) => return Err(CoreError::AnchorNotFound(post_id.to_string())),
            Err(error) => return Err(error),
        };

        if post.channel_id != expected_channel_id {
            return Err(CoreError::AnchorChannelMismatch {
                post_id: post_id.to_string(),
                channel: channel_name.to_string(),
            });
        }

        Ok(())
    }

    async fn request<T, B>(
        &self,
        method: &str,
        endpoint: &str,
        body: Option<B>,
    ) -> Result<T, CoreError>
    where
        T: for<'de> Deserialize<'de>,
        B: Serialize,
    {
        self.request_raw(method, endpoint, body).await
    }

    pub async fn request_raw<T, B>(
        &self,
        method: &str,
        endpoint: &str,
        body: Option<B>,
    ) -> Result<T, CoreError>
    where
        T: for<'de> Deserialize<'de>,
        B: Serialize,
    {
        let url = format!("{}/api/v4{endpoint}", self.base_url);
        let request = match method {
            "GET" => self.client.get(&url),
            "POST" => self.client.post(&url),
            "DELETE" => self.client.delete(&url),
            other => panic!("unsupported method {other}"),
        }
        .bearer_auth(&self.token);

        let request = if let Some(body) = body {
            request.json(&body)
        } else {
            request
        };
        let response = request.send().await?;
        let status = response.status();
        let bytes = response.bytes().await?;
        if !status.is_success() {
            let message = String::from_utf8_lossy(&bytes).to_string();
            return Err(CoreError::Api { status, message });
        }
        Ok(serde_json::from_slice(&bytes)?)
    }
}

#[derive(Debug, Clone, Deserialize)]
struct ChannelIdResponse {
    id: String,
}

pub struct EventBus {
    sender: broadcast::Sender<Arc<DaemonEvent>>,
    seq_counter: AtomicU64,
}

impl EventBus {
    pub fn new(capacity: usize) -> Self {
        let (sender, _) = broadcast::channel(capacity);
        Self {
            sender,
            seq_counter: AtomicU64::new(0),
        }
    }

    pub fn sender(&self) -> broadcast::Sender<Arc<DaemonEvent>> {
        self.sender.clone()
    }

    pub fn subscribe(&self) -> broadcast::Receiver<Arc<DaemonEvent>> {
        self.sender.subscribe()
    }

    pub fn current_seq(&self) -> u64 {
        self.seq_counter.load(Ordering::Relaxed)
    }

    pub fn emit(&self, mut event: DaemonEvent) -> u64 {
        let seq = self.seq_counter.fetch_add(1, Ordering::Relaxed) + 1;
        event.seq = seq;
        let _ = self.sender.send(Arc::new(event));
        seq
    }
}

pub struct WsState {
    pub connection_state: Arc<Mutex<WsConnectionState>>,
    pub last_event_at: Arc<AtomicI64>,
    pub last_error: Arc<Mutex<Option<String>>>,
    pub reconnect_count: Arc<AtomicU64>,
    pub last_disconnect_at: Arc<AtomicI64>,
    pub last_disconnect_seq: AtomicU64,
    pub suspected_gap: Arc<AtomicBool>,
    pub recovering_until: Arc<AtomicI64>,
    pub last_recovered_at: Arc<AtomicI64>,
}

impl Default for WsState {
    fn default() -> Self {
        Self::new()
    }
}

impl WsState {
    pub fn new() -> Self {
        Self {
            connection_state: Arc::new(Mutex::new(WsConnectionState::Disconnected)),
            last_event_at: Arc::new(AtomicI64::new(0)),
            last_error: Arc::new(Mutex::new(None)),
            reconnect_count: Arc::new(AtomicU64::new(0)),
            last_disconnect_at: Arc::new(AtomicI64::new(0)),
            last_disconnect_seq: AtomicU64::new(0),
            suspected_gap: Arc::new(AtomicBool::new(false)),
            recovering_until: Arc::new(AtomicI64::new(0)),
            last_recovered_at: Arc::new(AtomicI64::new(0)),
        }
    }

    pub async fn set_state(&self, state: WsConnectionState) {
        if matches!(state, WsConnectionState::Disconnected) {
            self.last_disconnect_at
                .store(now_unix_millis(), Ordering::Relaxed);
        }
        *self.connection_state.lock().await = state;
    }

    pub fn record_disconnect_seq(&self, seq: u64) {
        self.last_disconnect_seq.store(seq, Ordering::Relaxed);
    }

    pub async fn set_error(&self, msg: impl Into<String>) {
        *self.last_error.lock().await = Some(msg.into());
    }

    pub fn touch_event(&self) {
        self.last_event_at
            .store(now_unix_millis(), Ordering::Relaxed);
    }

    pub fn bump_reconnect(&self) {
        self.reconnect_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Stamp `recovering_until = now + RECOVERY_GRACE_MS` and spawn an
    /// idempotent one-shot task that, at grace-window completion,
    /// stamps `last_recovered_at` and clears `suspected_gap` — provided
    /// no later reconnect has restamped the target and the transport
    /// is still Healthy.
    ///
    /// Grace-window completion is the "recovery confirmed" moment: a
    /// gap that was flagged during this cycle's `reconnect_catchup`
    /// resolves when the grace elapses cleanly. Without clearing here,
    /// a daemon could report `recovering` / `suspected_gap=true` for
    /// hours after one sleep-wake, since the only other clear is at
    /// the entry of a *later* reconnect cycle. PER-010, secrev.
    ///
    /// No-op on cold start (`last_disconnect_at == 0`): `Recovering` is
    /// a reconnect-health signal, not a normal-startup state. Callers
    /// may invoke this on every auth-success; gating lives here so all
    /// paths share one rule.
    pub fn arm_recovery_window(self: &Arc<Self>) {
        if self.last_disconnect_at.load(Ordering::Relaxed) == 0 {
            return;
        }
        let target = now_unix_millis() + RECOVERY_GRACE_MS;
        self.recovering_until.store(target, Ordering::Relaxed);
        let ws = Arc::clone(self);
        tokio::spawn(async move {
            sleep(Duration::from_millis((RECOVERY_GRACE_MS + 10) as u64)).await;
            if ws.recovering_until.load(Ordering::Relaxed) != target {
                return;
            }
            let conn = *ws.connection_state.lock().await;
            if matches!(conn, WsConnectionState::Healthy) {
                ws.last_recovered_at
                    .store(now_unix_millis(), Ordering::Relaxed);
                ws.suspected_gap.store(false, Ordering::Relaxed);
            }
        });
    }
}

pub struct MattermostWs {
    ws_url: String,
    token: String,
    event_bus: Arc<EventBus>,
    ws_state: Arc<WsState>,
    profile_name: String,
    client: MattermostClient,
    monitored_channels: Vec<String>,
    bot_username: String,
    my_user_id: String,
    seen_posts: Arc<Mutex<std::collections::HashSet<String>>>,
}

impl MattermostWs {
    pub fn new(
        profile: &Profile,
        token: String,
        client: MattermostClient,
        event_bus: Arc<EventBus>,
        my_user_id: String,
    ) -> Self {
        let ws_url = profile
            .server_url
            .trim_end_matches('/')
            .replace("https://", "wss://")
            .replace("http://", "ws://");
        let ws_url = format!("{ws_url}/api/v4/websocket");
        Self {
            ws_url,
            token,
            event_bus,
            ws_state: Arc::new(WsState::new()),
            profile_name: profile.name.clone(),
            client,
            monitored_channels: profile.monitored_channels.clone(),
            bot_username: profile.bot_username.clone(),
            my_user_id,
            seen_posts: Arc::new(Mutex::new(std::collections::HashSet::new())),
        }
    }

    pub fn ws_state(&self) -> Arc<WsState> {
        Arc::clone(&self.ws_state)
    }

    pub async fn run(self: Arc<Self>, mut shutdown: tokio::sync::watch::Receiver<bool>) {
        let mut attempt: u64 = 0;
        loop {
            if *shutdown.borrow() {
                break;
            }
            attempt += 1;
            self.ws_state.set_state(WsConnectionState::Connecting).await;
            self.event_bus.emit(DaemonEvent {
                seq: 0,
                kind: DaemonEventKind::ConnectionStateChanged,
                payload: DaemonEventPayloadInner::ConnectionStateChanged(
                    ConnectionStateChangedPayload {
                        profile: self.profile_name.clone(),
                        provider: Provider::Mattermost,
                        state: WsConnectionState::Connecting,
                        message: format!("attempt {attempt}"),
                    },
                ),
            });

            match self.connect_and_listen().await {
                Ok(()) => {
                    info!("websocket session ended cleanly");
                }
                Err(e) => {
                    warn!(%e, "websocket session error");
                    self.ws_state.set_error(e.to_string()).await;
                }
            }

            if *shutdown.borrow() {
                break;
            }

            self.ws_state
                .set_state(WsConnectionState::Disconnected)
                .await;
            self.ws_state
                .record_disconnect_seq(self.event_bus.current_seq());
            self.ws_state.bump_reconnect();

            self.event_bus.emit(DaemonEvent {
                seq: 0,
                kind: DaemonEventKind::ConnectionStateChanged,
                payload: DaemonEventPayloadInner::ConnectionStateChanged(
                    ConnectionStateChangedPayload {
                        profile: self.profile_name.clone(),
                        provider: Provider::Mattermost,
                        state: WsConnectionState::Disconnected,
                        message: format!("disconnected, will reconnect (attempt {attempt})"),
                    },
                ),
            });

            let delay = if attempt <= 3 {
                Duration::from_secs(1 << attempt.min(3))
            } else {
                self.ws_state.set_state(WsConnectionState::Degraded).await;
                self.event_bus.emit(DaemonEvent {
                    seq: 0,
                    kind: DaemonEventKind::ConnectionStateChanged,
                    payload: DaemonEventPayloadInner::ConnectionStateChanged(
                        ConnectionStateChangedPayload {
                            profile: self.profile_name.clone(),
                            provider: Provider::Mattermost,
                            state: WsConnectionState::Degraded,
                            message: format!("degraded after {attempt} attempts"),
                        },
                    ),
                });
                Duration::from_secs(30)
            };

            tokio::select! {
                _ = sleep(delay) => {}
                _ = shutdown.changed() => {
                    break;
                }
            }
        }

        self.ws_state
            .set_state(WsConnectionState::Disconnected)
            .await;
    }

    async fn connect_and_listen(&self) -> Result<(), CoreError> {
        let (ws_stream, _) = connect_async(&self.ws_url).await.map_err(|e| {
            CoreError::Io(std::io::Error::new(
                std::io::ErrorKind::ConnectionRefused,
                e.to_string(),
            ))
        })?;

        let (mut write, mut read) = ws_stream.split();

        let auth = serde_json::json!({
            "seq": 1,
            "action": "authentication_challenge",
            "data": { "token": self.token }
        });
        write
            .send(WsMessage::Text(auth.to_string().into()))
            .await
            .map_err(|e| {
                CoreError::Io(std::io::Error::new(
                    std::io::ErrorKind::BrokenPipe,
                    e.to_string(),
                ))
            })?;

        let mut authenticated = false;
        let mut heartbeat = tokio::time::interval(Duration::from_secs(30));
        heartbeat.set_missed_tick_behavior(MissedTickBehavior::Delay);
        let auth_deadline = tokio::time::Instant::now() + Duration::from_secs(10);

        loop {
            let remaining = if !authenticated {
                auth_deadline.saturating_duration_since(tokio::time::Instant::now())
            } else {
                Duration::from_secs(3600)
            };

            tokio::select! {
                msg = read.next() => {
                    match msg {
                        Some(Ok(WsMessage::Text(text))) => {
                            if !authenticated {
                                if is_auth_success(&text) {
                                    authenticated = true;
                                    self.ws_state
                                        .set_state(WsConnectionState::Healthy)
                                        .await;
                                    info!("websocket authenticated and healthy");
                                    self.ws_state.arm_recovery_window();
                                    self.event_bus.emit(DaemonEvent {
                                        seq: 0,
                                        kind: DaemonEventKind::ConnectionStateChanged,
                                        payload: DaemonEventPayloadInner::ConnectionStateChanged(
                                            ConnectionStateChangedPayload {
                                                profile: self.profile_name.clone(),
                                                provider: Provider::Mattermost,
                                                state: WsConnectionState::Healthy,
                                                message: "connected and authenticated".to_string(),
                                            },
                                        ),
                                    });
                                    self.reconnect_catchup().await;
                                } else if is_auth_error(&text) {
                                    return Err(CoreError::Io(std::io::Error::new(
                                        std::io::ErrorKind::PermissionDenied,
                                        format!("websocket auth rejected: {}", text),
                                    )));
                                }
                            } else {
                                self.handle_ws_message(&text).await;
                            }
                        }
                        Some(Ok(WsMessage::Ping(data))) => {
                            let _ = write.send(WsMessage::Pong(data)).await;
                        }
                        Some(Ok(WsMessage::Close(_))) | None => {
                            return Ok(());
                        }
                        Some(Err(e)) => {
                            return Err(CoreError::Io(std::io::Error::new(
                                std::io::ErrorKind::ConnectionReset,
                                e.to_string(),
                            )));
                        }
                        _ => {}
                    }
                }
                _ = heartbeat.tick() => {
                    if authenticated {
                        let ping = serde_json::json!({
                            "seq": 0,
                            "action": "user_activity"
                        });
                        if write.send(WsMessage::Text(ping.to_string().into())).await.is_err() {
                            return Ok(());
                        }
                    }
                }
                _ = tokio::time::sleep(remaining) => {
                    if !authenticated {
                        return Err(CoreError::Io(std::io::Error::new(
                            std::io::ErrorKind::TimedOut,
                            "websocket auth timed out waiting for server ack",
                        )));
                    }
                }
            }
        }
    }

    async fn handle_ws_message(&self, text: &str) {
        let Ok(value) = serde_json::from_str::<Value>(text) else {
            return;
        };

        let event = value.get("event").and_then(|v| v.as_str()).unwrap_or("");
        let data = value.get("data").cloned().unwrap_or(Value::Null);

        match event {
            "posted" | "post_edited" => {
                self.handle_post_event(&data).await;
            }
            "status_change" | "hello" => {
                self.ws_state.touch_event();
            }
            _ => {}
        }
    }

    async fn handle_post_event(&self, data: &Value) {
        self.ws_state.touch_event();

        let Some(post_str) = data.get("post").and_then(|v| v.as_str()) else {
            return;
        };
        let Ok(post): Result<Value, _> = serde_json::from_str(post_str) else {
            return;
        };

        let post_id = post
            .get("id")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let channel_id = post
            .get("channel_id")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let sender_id = post
            .get("user_id")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let message = post
            .get("message")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let create_at = post.get("create_at").and_then(|v| v.as_i64()).unwrap_or(0);

        if sender_id == self.my_user_id {
            return;
        }

        if post_id.is_empty() || channel_id.is_empty() {
            return;
        }

        {
            let mut seen = self.seen_posts.lock().await;
            if !seen.insert(post_id.clone()) {
                return;
            }
            if seen.len() > 10000 {
                let to_remove: Vec<String> = seen.iter().take(5000).cloned().collect();
                for id in to_remove {
                    seen.remove(&id);
                }
            }
        }

        let channel_name = self.resolve_channel_name(&channel_id).await;
        let sender_username = self.resolve_username(&sender_id).await;
        let mentioned = message_mentions_username(&message, &self.bot_username);

        let is_monitored = self
            .monitored_channels
            .iter()
            .any(|m| m.eq_ignore_ascii_case(&channel_name));

        if is_monitored {
            let event = DaemonEvent {
                seq: 0,
                kind: DaemonEventKind::InboundMessage,
                payload: DaemonEventPayloadInner::Inbound(InboundEventPayload {
                    profile: self.profile_name.clone(),
                    provider: Provider::Mattermost,
                    channel_id,
                    channel_name,
                    post_id,
                    sender_id,
                    sender_username,
                    message,
                    create_at,
                    received_at: now_unix_millis(),
                    mentioned,
                }),
            };
            self.event_bus.emit(event);
        } else if mentioned {
            let event = DaemonEvent {
                seq: 0,
                kind: DaemonEventKind::InboundMention,
                payload: DaemonEventPayloadInner::Inbound(InboundEventPayload {
                    profile: self.profile_name.clone(),
                    provider: Provider::Mattermost,
                    channel_id,
                    channel_name,
                    post_id,
                    sender_id,
                    sender_username,
                    message,
                    create_at,
                    received_at: now_unix_millis(),
                    mentioned: true,
                }),
            };
            self.event_bus.emit(event);
        }
    }

    async fn reconnect_catchup(&self) {
        // Each reconnect cycle starts with a clean slate. If this cycle's
        // outage exceeded the 5-min window and emits a Gap below, we'll
        // re-flag suspected_gap — sticky across the healthy window until
        // the next reconnect cycle proves it clean. PER-010.
        self.ws_state.suspected_gap.store(false, Ordering::Relaxed);
        let five_min_ago = now_unix_millis() - (5 * 60 * 1000);
        let disconnect_at = self.ws_state.last_disconnect_at.load(Ordering::Relaxed);
        let outage_exceeded_window = disconnect_at > 0 && disconnect_at < five_min_ago;

        for channel_name in &self.monitored_channels {
            let Ok(channel_id) = self.client.channel_id_for_name(channel_name).await else {
                continue;
            };
            let Ok(messages) = self
                .client
                .read_channel_by_id_since_millis(&channel_id, five_min_ago)
                .await
            else {
                continue;
            };

            let mut seen = self.seen_posts.lock().await;

            let new_messages: Vec<_> = messages
                .into_iter()
                .filter(|m| m.user_id != self.my_user_id && seen.insert(m.id.clone()))
                .collect();

            for msg in new_messages {
                let mentioned = message_mentions_username(&msg.message, &self.bot_username);
                let event = DaemonEvent {
                    seq: 0,
                    kind: DaemonEventKind::InboundMessage,
                    payload: DaemonEventPayloadInner::Inbound(InboundEventPayload {
                        profile: self.profile_name.clone(),
                        provider: Provider::Mattermost,
                        channel_id: channel_id.clone(),
                        channel_name: channel_name.clone(),
                        post_id: msg.id,
                        sender_id: msg.user_id,
                        sender_username: msg.username,
                        message: msg.message,
                        create_at: msg.create_at,
                        received_at: now_unix_millis(),
                        mentioned,
                    }),
                };
                self.event_bus.emit(event);
            }

            if seen.len() > 10000 {
                let to_remove: Vec<String> = seen.iter().take(5000).cloned().collect();
                for id in to_remove {
                    seen.remove(&id);
                }
            }
        }

        if outage_exceeded_window {
            self.ws_state.suspected_gap.store(true, Ordering::Relaxed);
            let missed_from = self.ws_state.last_disconnect_seq.load(Ordering::Relaxed);
            let missed_to = self.event_bus.current_seq();
            self.event_bus.emit(DaemonEvent {
                seq: 0,
                kind: DaemonEventKind::Gap,
                payload: DaemonEventPayloadInner::Gap(GapPayload {
                    subscription_id: format!("__reconnect__{}", self.profile_name),
                    missed_from_seq: missed_from,
                    missed_to_seq: missed_to,
                }),
            });
        }
    }

    async fn resolve_channel_name(&self, channel_id: &str) -> String {
        let channels = self.client.list_channels().await.unwrap_or_default();
        channels
            .iter()
            .find(|c| c.id == channel_id)
            .map(|c| c.name.clone())
            .unwrap_or_else(|| channel_id.to_string())
    }

    async fn resolve_username(&self, user_id: &str) -> String {
        #[derive(Deserialize)]
        struct UserResp {
            username: String,
        }
        let result: Result<UserResp, _> = self
            .client
            .request_raw("GET", &format!("/users/{user_id}"), None::<Value>)
            .await;
        result
            .map(|u| u.username)
            .unwrap_or_else(|_| "unknown".to_string())
    }
}

fn is_auth_success(text: &str) -> bool {
    let Ok(value) = serde_json::from_str::<Value>(text) else {
        return false;
    };
    let event = value.get("event").and_then(|v| v.as_str()).unwrap_or("");
    if event == "hello" {
        let status = value
            .get("data")
            .and_then(|d| d.get("status"))
            .and_then(|s| s.as_str())
            .unwrap_or("");
        return status == "OK";
    }
    false
}

fn is_auth_error(text: &str) -> bool {
    let Ok(value) = serde_json::from_str::<Value>(text) else {
        return false;
    };
    let seq = value.get("seq").and_then(|v| v.as_u64()).unwrap_or(0);
    let has_error = value.get("error").is_some();
    if seq == 0 && has_error {
        return true;
    }
    let status = value
        .get("data")
        .and_then(|d| d.get("status"))
        .and_then(|s| s.as_str())
        .unwrap_or("");
    if status == "FAIL" || status == "INVALID_TOKEN" {
        return true;
    }
    false
}

pub fn rpc_request(method: impl Into<String>, params: Value) -> JsonRpcRequest {
    JsonRpcRequest {
        jsonrpc: "2.0".to_string(),
        id: Uuid::new_v4(),
        method: method.into(),
        params,
    }
}

pub fn rpc_result(id: Uuid, result: Value) -> JsonRpcResponse {
    JsonRpcResponse {
        jsonrpc: "2.0".to_string(),
        id,
        result: Some(result),
        error: None,
    }
}

pub fn rpc_error(id: Uuid, code: i64, message: impl Into<String>) -> JsonRpcResponse {
    JsonRpcResponse {
        jsonrpc: "2.0".to_string(),
        id,
        result: None,
        error: Some(ErrorDetail {
            code,
            message: message.into(),
        }),
    }
}

fn default_team_name() -> String {
    DEFAULT_TEAM.to_string()
}

fn default_credential_mode() -> CredentialMode {
    CredentialMode::EnvName
}

fn default_capability_class() -> CapabilityClass {
    CapabilityClass::Standard
}

fn message_mentions_username(message: &str, username: &str) -> bool {
    let needle = format!("@{username}");
    let mut search_start = 0;
    while let Some(index) = message[search_start..].find(&needle) {
        let absolute = search_start + index;
        let boundary_index = absolute + needle.len();
        let boundary_ok = message
            .as_bytes()
            .get(boundary_index)
            .map(|byte| !byte.is_ascii_alphanumeric() && *byte != b'_' && *byte != b'-')
            .unwrap_or(true);
        if boundary_ok {
            return true;
        }
        search_start = boundary_index;
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    static CONFIG_ENV_LOCK: Mutex<()> = Mutex::new(());

    #[test]
    fn parses_env_file_lines() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("mm.env");
        fs::write(
            &path,
            "export LANYTE_MM_TOKEN=\"secret\"\n# comment\nOTHER=value\n",
        )
        .unwrap();
        let values = parse_env_file(&path).unwrap();
        assert_eq!(values.get("LANYTE_MM_TOKEN"), Some(&"secret".to_string()));
        assert_eq!(values.get("OTHER"), Some(&"value".to_string()));
    }

    #[test]
    fn stores_and_loads_attention_state() {
        let _guard = CONFIG_ENV_LOCK.lock().unwrap();
        let dir = tempfile::tempdir().unwrap();
        unsafe { env::set_var("XDG_CONFIG_HOME", dir.path()) };

        let state = AttentionState {
            channels: BTreeMap::from([(
                "per-008".to_string(),
                ChannelCursorState {
                    last_seen_post_id: Some("post-123".to_string()),
                    updated_at: Some(1_776_000_000_000),
                    last_known_stale: false,
                    last_checked_at: None,
                },
            )]),
            mentions: MentionCursorState {
                last_seen_post_id: Some("mention-456".to_string()),
                updated_at: Some(1_776_000_000_001),
            },
        };

        let path = store_attention_state("bravo-devlead", &state).unwrap();
        let loaded = load_attention_state("bravo-devlead").unwrap();

        assert_eq!(loaded, state);
        assert!(path.ends_with("lanytehq/chanvoy/state-bravo-devlead.json"));

        unsafe { env::remove_var("XDG_CONFIG_HOME") };
    }

    #[test]
    fn serializes_rpc_request() {
        let request = rpc_request(
            "whoami",
            serde_json::json!({
                "profile": "bravo-devlead"
            }),
        );
        let encoded = serde_json::to_value(&request).unwrap();
        assert_eq!(encoded["jsonrpc"], "2.0");
        assert_eq!(encoded["method"], "whoami");
        assert_eq!(encoded["params"]["profile"], "bravo-devlead");
    }

    #[test]
    fn uses_token_env_alias_for_profile() {
        let profile: Profile = toml::from_str(
            r#"
name = "bravo-devlead"
role = "devlead"
scope = "bravo"
provider = "mattermost"
bot_username = "agent-bravo-devlead"
server_url = "https://mm.example.com"
token_env = "LANYTE_MM_TOKEN"
credential_mode = "env_name"
"#,
        )
        .unwrap();
        assert_eq!(profile.env_name, "LANYTE_MM_TOKEN");
        assert_eq!(profile.credential_mode, CredentialMode::EnvName);
    }

    #[test]
    fn matches_only_targeted_notifications() {
        assert!(message_mentions_username(
            "@agent-bravo-devlead please review",
            "agent-bravo-devlead"
        ));
        assert!(!message_mentions_username(
            "@agent-bravo-devrev please review",
            "agent-bravo-devlead"
        ));
        assert!(!message_mentions_username(
            "@agent-bravo-devlead-extra please review",
            "agent-bravo-devlead"
        ));
    }

    #[test]
    fn parses_monitored_channels_from_profile() {
        let profile: Profile = toml::from_str(
            r#"
name = "bravo-devlead"
role = "devlead"
scope = "bravo"
provider = "mattermost"
bot_username = "agent-bravo-devlead"
server_url = "https://mm.example.com"
env_name = "LANYTE_MM_TOKEN"
monitored_channels = ["per-003", "per-004"]
"#,
        )
        .unwrap();
        assert_eq!(profile.monitored_channels, vec!["per-003", "per-004"]);
    }

    #[test]
    fn daemon_event_serializes_as_notification() {
        let event = DaemonEvent {
            seq: 42,
            kind: DaemonEventKind::InboundMessage,
            payload: DaemonEventPayloadInner::Inbound(InboundEventPayload {
                profile: "bravo-devlead".to_string(),
                provider: Provider::Mattermost,
                channel_id: "ch123".to_string(),
                channel_name: "per-004".to_string(),
                post_id: "post456".to_string(),
                sender_id: "user789".to_string(),
                sender_username: "agent-dispatch".to_string(),
                message: "hello".to_string(),
                create_at: 1000,
                received_at: 1001,
                mentioned: false,
            }),
        };
        let notification = daemon_event_to_notification(&event);
        assert_eq!(notification.jsonrpc, "2.0");
        assert_eq!(notification.method, "push.inbound_message");
        let params = &notification.params;
        assert_eq!(params["seq"], 42);
        assert_eq!(params["kind"], "inbound_message");
        assert_eq!(params["channel_name"], "per-004");
    }

    #[tokio::test]
    async fn event_bus_seq_monotonic() {
        let bus = Arc::new(EventBus::new(16));
        let mut rx = bus.subscribe();

        bus.emit(DaemonEvent {
            seq: 0,
            kind: DaemonEventKind::ConnectionStateChanged,
            payload: DaemonEventPayloadInner::ConnectionStateChanged(
                ConnectionStateChangedPayload {
                    profile: "test".to_string(),
                    provider: Provider::Mattermost,
                    state: WsConnectionState::Healthy,
                    message: "ok".to_string(),
                },
            ),
        });
        bus.emit(DaemonEvent {
            seq: 0,
            kind: DaemonEventKind::ConnectionStateChanged,
            payload: DaemonEventPayloadInner::ConnectionStateChanged(
                ConnectionStateChangedPayload {
                    profile: "test".to_string(),
                    provider: Provider::Mattermost,
                    state: WsConnectionState::Disconnected,
                    message: "gone".to_string(),
                },
            ),
        });

        let e1 = rx.recv().await.unwrap();
        let e2 = rx.recv().await.unwrap();
        assert_eq!(e1.seq, 1);
        assert_eq!(e2.seq, 2);
        assert!(e2.seq > e1.seq);
    }

    #[tokio::test]
    async fn event_bus_gap_detection() {
        let bus = Arc::new(EventBus::new(2));
        let mut rx = bus.subscribe();

        for i in 0..5 {
            bus.emit(DaemonEvent {
                seq: 0,
                kind: DaemonEventKind::ConnectionStateChanged,
                payload: DaemonEventPayloadInner::ConnectionStateChanged(
                    ConnectionStateChangedPayload {
                        profile: "test".to_string(),
                        provider: Provider::Mattermost,
                        state: WsConnectionState::Healthy,
                        message: format!("event {i}"),
                    },
                ),
            });
        }

        let result = rx.try_recv();
        match result {
            Ok(event) => {
                assert!(event.seq >= 1);
            }
            Err(tokio::sync::broadcast::error::TryRecvError::Lagged(n)) => {
                assert!(n > 0, "should report lagged count");
            }
            _ => {}
        }

        assert_eq!(bus.current_seq(), 5);
    }

    #[tokio::test]
    async fn event_bus_multi_subscriber_isolation() {
        let bus = Arc::new(EventBus::new(16));
        let mut rx1 = bus.subscribe();
        let mut rx2 = bus.subscribe();

        bus.emit(DaemonEvent {
            seq: 0,
            kind: DaemonEventKind::InboundMessage,
            payload: DaemonEventPayloadInner::Inbound(InboundEventPayload {
                profile: "test".to_string(),
                provider: Provider::Mattermost,
                channel_id: "ch1".to_string(),
                channel_name: "per-004".to_string(),
                post_id: "p1".to_string(),
                sender_id: "u1".to_string(),
                sender_username: "alice".to_string(),
                message: "hello".to_string(),
                create_at: 1000,
                received_at: 1001,
                mentioned: false,
            }),
        });

        let e1 = rx1.recv().await.unwrap();
        let e2 = rx2.recv().await.unwrap();
        assert_eq!(e1.seq, e2.seq);
        assert_eq!(e1.kind, DaemonEventKind::InboundMessage);

        drop(rx1);

        bus.emit(DaemonEvent {
            seq: 0,
            kind: DaemonEventKind::InboundMention,
            payload: DaemonEventPayloadInner::Inbound(InboundEventPayload {
                profile: "test".to_string(),
                provider: Provider::Mattermost,
                channel_id: "ch2".to_string(),
                channel_name: "bravo-team".to_string(),
                post_id: "p2".to_string(),
                sender_id: "u2".to_string(),
                sender_username: "bob".to_string(),
                message: "@agent-bravo-devlead review".to_string(),
                create_at: 2000,
                received_at: 2001,
                mentioned: true,
            }),
        });

        let e3 = rx2.recv().await.unwrap();
        assert_eq!(e3.kind, DaemonEventKind::InboundMention);
    }

    #[test]
    fn is_auth_success_accepts_hello_ok() {
        let frame = r#"{"event":"hello","data":{"status":"OK","server_version":"10.5.0"}}"#;
        assert!(is_auth_success(frame));
    }

    #[test]
    fn is_auth_success_rejects_non_hello() {
        let frame = r#"{"event":"posted","data":{"post":"{}"}}"#;
        assert!(!is_auth_success(frame));
    }

    #[test]
    fn is_auth_success_rejects_hello_fail() {
        let frame = r#"{"event":"hello","data":{"status":"FAIL"}}"#;
        assert!(!is_auth_success(frame));
    }

    #[test]
    fn is_auth_error_detects_fail_status() {
        let frame = r#"{"data":{"status":"FAIL"}}"#;
        assert!(is_auth_error(frame));
    }

    #[test]
    fn is_auth_error_detects_invalid_token() {
        let frame = r#"{"data":{"status":"INVALID_TOKEN"}}"#;
        assert!(is_auth_error(frame));
    }

    #[test]
    fn is_auth_error_rejects_normal_event() {
        let frame = r#"{"event":"posted","data":{"post":"{}"}}"#;
        assert!(!is_auth_error(frame));
    }

    // ---- PER-009 wiremock integration tests ----
    //
    // Cover reviewer-named merge-gate paths at the MattermostClient + compute_seed_outcomes
    // layer: invalid-token (401), team-missing (404), bot-not-in-team (403), preflight
    // happy path, partial-seed (per-channel fetch failure), empty-channel-seed, monotonic
    // skip of already-cursored channels, DM-channel skip.
    //
    // End-to-end coverage (daemon spawn, actual auto-setup exit codes) is intentionally
    // deferred to a follow-up lifecycle harness (PER-008C expansion) — it requires
    // binary-spawn infrastructure outside this PR's scope.

    mod per_009_seed {
        use super::*;
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        fn test_profile(server_url: &str) -> Profile {
            Profile {
                name: "bravo-devlead".to_string(),
                role: "bravo-devlead".to_string(),
                scope: "lanytehq".to_string(),
                provider: Provider::Mattermost,
                bot_username: "agent-bravo-devlead".to_string(),
                team_name: "org-lanytehq".to_string(),
                server_url: server_url.to_string(),
                env_name: "LANYTE_MM_TOKEN".to_string(),
                env_file: None,
                credential_mode: CredentialMode::EnvName,
                capability_class: CapabilityClass::Standard,
                monitored_channels: Vec::new(),
                ipc: None,
            }
        }

        async fn mock_whoami(server: &MockServer, status: u16) {
            let response = if status == 200 {
                ResponseTemplate::new(200).set_body_json(serde_json::json!({
                    "id": "bot-id-123",
                    "username": "agent-bravo-devlead",
                    "is_bot": true,
                    "nickname": null,
                    "email": null,
                }))
            } else {
                ResponseTemplate::new(status).set_body_json(serde_json::json!({
                    "status_code": status,
                    "message": "mock error",
                }))
            };
            Mock::given(method("GET"))
                .and(path("/api/v4/users/me"))
                .respond_with(response)
                .mount(server)
                .await;
        }

        async fn mock_team(server: &MockServer, team_name: &str, status: u16) {
            let response = if status == 200 {
                ResponseTemplate::new(200).set_body_json(serde_json::json!({
                    "id": "team-id-456",
                    "name": team_name,
                }))
            } else {
                ResponseTemplate::new(status).set_body_json(serde_json::json!({
                    "status_code": status,
                    "message": "mock team error",
                }))
            };
            Mock::given(method("GET"))
                .and(path(format!("/api/v4/teams/name/{team_name}")))
                .respond_with(response)
                .mount(server)
                .await;
        }

        #[tokio::test]
        async fn preflight_401_surfaces_as_api_unauthorized() {
            let server = MockServer::start().await;
            mock_whoami(&server, 401).await;

            let client =
                MattermostClient::new(&test_profile(&server.uri()), "bad-token".into()).unwrap();
            let err = client.whoami().await.expect_err("whoami must fail on 401");
            match err {
                CoreError::Api { status, .. } => assert_eq!(status.as_u16(), 401),
                other => panic!("expected Api{{401}}, got {other:?}"),
            }
        }

        #[tokio::test]
        async fn preflight_team_404_surfaces_as_api_not_found() {
            let server = MockServer::start().await;
            mock_whoami(&server, 200).await;
            mock_team(&server, "org-lanytehq", 404).await;

            let client =
                MattermostClient::new(&test_profile(&server.uri()), "token".into()).unwrap();
            let err = client
                .validate_team_access()
                .await
                .expect_err("validate_team_access must fail when team missing");
            match err {
                CoreError::Api { status, .. } => assert_eq!(status.as_u16(), 404),
                other => panic!("expected Api{{404}}, got {other:?}"),
            }
        }

        #[tokio::test]
        async fn preflight_team_403_surfaces_as_api_forbidden() {
            let server = MockServer::start().await;
            mock_whoami(&server, 200).await;
            mock_team(&server, "org-lanytehq", 403).await;

            let client =
                MattermostClient::new(&test_profile(&server.uri()), "token".into()).unwrap();
            let err = client
                .validate_team_access()
                .await
                .expect_err("validate_team_access must fail when bot not in team");
            match err {
                CoreError::Api { status, .. } => assert_eq!(status.as_u16(), 403),
                other => panic!("expected Api{{403}}, got {other:?}"),
            }
        }

        #[tokio::test]
        async fn preflight_happy_path_validates_team_access() {
            let server = MockServer::start().await;
            mock_whoami(&server, 200).await;
            mock_team(&server, "org-lanytehq", 200).await;

            let client =
                MattermostClient::new(&test_profile(&server.uri()), "token".into()).unwrap();
            client.whoami().await.unwrap();
            client.validate_team_access().await.unwrap();
        }

        #[tokio::test]
        async fn compute_seed_outcomes_mixed_channels() {
            let server = MockServer::start().await;
            mock_whoami(&server, 200).await;
            mock_team(&server, "org-lanytehq", 200).await;

            // /users/me/teams/{team_id}/channels -> three team channels plus one DM
            Mock::given(method("GET"))
                .and(path("/api/v4/users/me/teams/team-id-456/channels"))
                .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!([
                    {"id": "ch-public-seeded", "name": "bravo-team", "display_name": "Bravo", "type": "O"},
                    {"id": "ch-private-empty", "name": "per-009-private", "display_name": "PER-009", "type": "P"},
                    {"id": "ch-public-failed", "name": "flaky-channel", "display_name": "Flaky", "type": "O"},
                    {"id": "ch-dm", "name": "dm-channel", "display_name": "", "type": "D"},
                ])))
                .mount(&server)
                .await;
            // Seeded channel returns a head post.
            Mock::given(method("GET"))
                .and(path("/api/v4/channels/ch-public-seeded/posts"))
                .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                    "posts": {
                        "post-head-xyz": {
                            "id": "post-head-xyz",
                            "user_id": "user-1",
                            "message": "hi",
                            "create_at": 1_776_000_000_000_i64,
                            "username": "user-1",
                        }
                    }
                })))
                .mount(&server)
                .await;
            // Empty channel returns zero posts.
            Mock::given(method("GET"))
                .and(path("/api/v4/channels/ch-private-empty/posts"))
                .respond_with(
                    ResponseTemplate::new(200).set_body_json(serde_json::json!({ "posts": {} })),
                )
                .mount(&server)
                .await;
            // Flaky channel fails HEAD fetch.
            Mock::given(method("GET"))
                .and(path("/api/v4/channels/ch-public-failed/posts"))
                .respond_with(ResponseTemplate::new(500).set_body_json(serde_json::json!({
                    "status_code": 500,
                    "message": "upstream error",
                })))
                .mount(&server)
                .await;

            let client =
                MattermostClient::new(&test_profile(&server.uri()), "token".into()).unwrap();
            let existing = std::collections::BTreeSet::new();
            let outcomes = compute_seed_outcomes(&client, &existing).await.unwrap();

            // DM channel must be skipped entirely — not present in outcomes.
            assert!(!outcomes
                .iter()
                .any(|o| matches!(o, SeededChannelOutcome::Seeded { channel, .. } if channel == "dm-channel")));

            let seeded: Vec<_> = outcomes
                .iter()
                .filter_map(|o| match o {
                    SeededChannelOutcome::Seeded { channel, post_id } => {
                        Some((channel.as_str(), post_id.as_str()))
                    }
                    _ => None,
                })
                .collect();
            assert_eq!(seeded, vec![("bravo-team", "post-head-xyz")]);

            let empty: Vec<_> = outcomes
                .iter()
                .filter_map(|o| match o {
                    SeededChannelOutcome::UnseededEmptyChannel { channel } => {
                        Some(channel.as_str())
                    }
                    _ => None,
                })
                .collect();
            assert_eq!(empty, vec!["per-009-private"]);

            let failed: Vec<_> = outcomes
                .iter()
                .filter_map(|o| match o {
                    SeededChannelOutcome::Failed { channel, .. } => Some(channel.as_str()),
                    _ => None,
                })
                .collect();
            assert_eq!(failed, vec!["flaky-channel"]);
        }

        #[tokio::test]
        async fn compute_seed_outcomes_skips_already_cursored_channels() {
            // Monotonic guarantee at the enumeration layer: a channel that already has a
            // stored cursor must not appear in outcomes (the daemon's write-side
            // if-absent guard is defense-in-depth, not the primary mechanism).
            let server = MockServer::start().await;
            mock_whoami(&server, 200).await;
            mock_team(&server, "org-lanytehq", 200).await;

            Mock::given(method("GET"))
                .and(path("/api/v4/users/me/teams/team-id-456/channels"))
                .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!([
                    {"id": "ch-existing", "name": "bravo-team", "display_name": "Bravo", "type": "O"},
                    {"id": "ch-new", "name": "per-009", "display_name": "PER-009", "type": "O"},
                ])))
                .mount(&server)
                .await;
            Mock::given(method("GET"))
                .and(path("/api/v4/channels/ch-new/posts"))
                .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                    "posts": {
                        "post-new": {
                            "id": "post-new",
                            "user_id": "u",
                            "message": "x",
                            "create_at": 1_776_000_000_000_i64,
                            "username": "u",
                        }
                    }
                })))
                .mount(&server)
                .await;
            // Intentionally no mock for ch-existing/posts — if seeder fetched HEAD there,
            // the test fails (wiremock returns 404 for unmocked paths, surfacing as Failed).

            let client =
                MattermostClient::new(&test_profile(&server.uri()), "token".into()).unwrap();
            let mut existing = std::collections::BTreeSet::new();
            existing.insert("bravo-team".to_string());
            let outcomes = compute_seed_outcomes(&client, &existing).await.unwrap();

            let channels: Vec<_> = outcomes
                .iter()
                .map(|o| match o {
                    SeededChannelOutcome::Seeded { channel, .. }
                    | SeededChannelOutcome::UnseededEmptyChannel { channel }
                    | SeededChannelOutcome::Failed { channel, .. } => channel.as_str(),
                })
                .collect();
            assert_eq!(channels, vec!["per-009"]);
        }
    }

    mod reconnect_health {
        use super::*;
        use std::sync::atomic::Ordering;

        #[test]
        fn derive_none_when_no_transport() {
            assert_eq!(derive_daemon_health(1000, None, false, 0), None);
        }

        #[test]
        fn derive_maps_non_healthy_transport_one_to_one() {
            let now = 1_000_000;
            assert_eq!(
                derive_daemon_health(now, Some(WsConnectionState::Disconnected), false, 0),
                Some(DaemonHealthState::Disconnected)
            );
            assert_eq!(
                derive_daemon_health(now, Some(WsConnectionState::Connecting), false, 0),
                Some(DaemonHealthState::Connecting)
            );
            assert_eq!(
                derive_daemon_health(now, Some(WsConnectionState::Degraded), false, 0),
                Some(DaemonHealthState::Degraded)
            );
        }

        #[test]
        fn derive_healthy_when_transport_healthy_and_no_grace_no_gap() {
            let now = 1_000_000;
            assert_eq!(
                derive_daemon_health(now, Some(WsConnectionState::Healthy), false, 0),
                Some(DaemonHealthState::Healthy)
            );
        }

        #[test]
        fn derive_recovering_during_grace_window() {
            let now = 1_000_000;
            let recovering_until = now + 5_000;
            assert_eq!(
                derive_daemon_health(
                    now,
                    Some(WsConnectionState::Healthy),
                    false,
                    recovering_until
                ),
                Some(DaemonHealthState::Recovering)
            );
        }

        #[test]
        fn derive_recovering_when_suspected_gap_even_after_grace() {
            let now = 1_000_000;
            assert_eq!(
                derive_daemon_health(now, Some(WsConnectionState::Healthy), true, 0),
                Some(DaemonHealthState::Recovering)
            );
        }

        #[test]
        fn derive_healthy_after_grace_elapsed_no_gap() {
            let now = 1_000_000;
            let recovering_until = now - 1;
            assert_eq!(
                derive_daemon_health(
                    now,
                    Some(WsConnectionState::Healthy),
                    false,
                    recovering_until
                ),
                Some(DaemonHealthState::Healthy)
            );
        }

        // AC #6 state-machine coverage operating directly on WsState.
        // WS-lifecycle helpers (`arm_recovery_window`) and `reconnect_catchup`
        // are exercised at the WsState level because the full transport loop
        // requires a live Mattermost server. These tests pin the
        // agent-gating contract: given a specific WsState snapshot, the
        // derived health is correct.

        fn snapshot(ws: &WsState) -> (WsConnectionState, bool, i64) {
            // connection_state is a Mutex<T>; tests read it via try_lock
            // since no other task holds it here.
            let conn = ws.connection_state.try_lock().map(|g| *g).unwrap();
            let gap = ws.suspected_gap.load(Ordering::Relaxed);
            let ru = ws.recovering_until.load(Ordering::Relaxed);
            (conn, gap, ru)
        }

        #[tokio::test]
        async fn healthy_steady_state_reads_healthy() {
            let ws = WsState::new();
            ws.set_state(WsConnectionState::Healthy).await;
            let (conn, gap, ru) = snapshot(&ws);
            let health = derive_daemon_health(now_unix_millis(), Some(conn), gap, ru);
            assert_eq!(health, Some(DaemonHealthState::Healthy));
        }

        #[tokio::test]
        async fn disconnect_stamps_last_disconnect_and_reads_disconnected() {
            let ws = WsState::new();
            ws.set_state(WsConnectionState::Healthy).await;
            ws.set_state(WsConnectionState::Disconnected).await;
            let (conn, gap, ru) = snapshot(&ws);
            assert!(ws.last_disconnect_at.load(Ordering::Relaxed) > 0);
            let health = derive_daemon_health(now_unix_millis(), Some(conn), gap, ru);
            assert_eq!(health, Some(DaemonHealthState::Disconnected));
        }

        #[tokio::test]
        async fn reconnect_within_grace_reads_recovering() {
            let ws = Arc::new(WsState::new());
            ws.set_state(WsConnectionState::Disconnected).await;
            ws.set_state(WsConnectionState::Healthy).await;
            ws.arm_recovery_window();
            // Within the grace window the derived health must be Recovering.
            let (conn, gap, ru) = snapshot(&ws);
            let health = derive_daemon_health(now_unix_millis(), Some(conn), gap, ru);
            assert_eq!(health, Some(DaemonHealthState::Recovering));
            assert!(ru > now_unix_millis());
        }

        #[tokio::test]
        async fn suspected_gap_keeps_recovering_past_grace() {
            let ws = Arc::new(WsState::new());
            ws.set_state(WsConnectionState::Healthy).await;
            ws.suspected_gap.store(true, Ordering::Relaxed);
            // recovering_until is 0 (grace window elapsed in the past), but
            // suspected_gap alone should hold Recovering.
            let (conn, gap, ru) = snapshot(&ws);
            let health = derive_daemon_health(now_unix_millis(), Some(conn), gap, ru);
            assert_eq!(health, Some(DaemonHealthState::Recovering));
        }

        #[tokio::test]
        async fn grace_window_task_stamps_last_recovered_at_when_still_healthy() {
            // Shorten the wait by arming with a tiny recovering_until; the
            // task sleeps RECOVERY_GRACE_MS + 10, so we rely on the real
            // constant here for a small-but-real wait.
            let ws = Arc::new(WsState::new());
            ws.set_state(WsConnectionState::Disconnected).await;
            ws.set_state(WsConnectionState::Healthy).await;
            ws.arm_recovery_window();
            tokio::time::sleep(Duration::from_millis((RECOVERY_GRACE_MS as u64) + 200)).await;
            assert!(ws.last_recovered_at.load(Ordering::Relaxed) > 0);
        }

        #[tokio::test]
        async fn grace_window_task_is_idempotent_against_later_reconnect() {
            let ws = Arc::new(WsState::new());
            ws.set_state(WsConnectionState::Disconnected).await;
            ws.set_state(WsConnectionState::Healthy).await;
            ws.arm_recovery_window();
            // Restamp immediately — later reconnect — which invalidates the
            // first task's target equality check.
            let first_target = ws.recovering_until.load(Ordering::Relaxed);
            // Advance enough for the first task to wake up, but NOT enough
            // for the second target's task to wake up.
            tokio::time::sleep(Duration::from_millis(5)).await;
            ws.arm_recovery_window();
            let second_target = ws.recovering_until.load(Ordering::Relaxed);
            assert!(
                second_target > first_target,
                "restamp must advance the target"
            );
            // Let both task timers fire; only the second should stamp.
            tokio::time::sleep(Duration::from_millis((RECOVERY_GRACE_MS as u64) + 200)).await;
            assert!(ws.last_recovered_at.load(Ordering::Relaxed) > 0);
        }

        fn ws_snapshot_for(
            connection_state: Option<WsConnectionState>,
            suspected_gap: Option<bool>,
            last_disconnect_at: Option<i64>,
            reconnect_count: Option<u64>,
            recovering_until: i64,
        ) -> WsStatusSnapshot {
            WsStatusSnapshot {
                connection_state,
                last_event_at: None,
                last_error: None,
                reconnect_count,
                last_disconnect_at,
                last_recovered_at: None,
                suspected_gap,
                recovering_until,
            }
        }

        fn ipc_absent() -> IpcStatusSnapshot {
            IpcStatusSnapshot {
                connected: None,
                peer_id: None,
                reconnect_count: None,
            }
        }

        #[test]
        fn build_status_surfaces_reconnect_fields_when_whoami_fails() {
            // PER-010 entarch finding: `daemon_status` must remain a
            // local read when Mattermost reachability is lost. In the
            // sleep/wake / transient-network outage class this PR is
            // about, REST `whoami` can fail alongside the WS — and the
            // operator needs the new reconnect-health fields precisely
            // then. The RPC must not error out; whoami failure becomes
            // data.
            let now = 1_777_050_000_000;
            let ws = ws_snapshot_for(
                Some(WsConnectionState::Disconnected),
                Some(true),
                Some(now - 30_000),
                Some(4),
                0,
            );
            let status = build_daemon_status(
                "bravo-devlead".to_string(),
                PathBuf::from("/tmp/chanvoy/bravo-devlead.sock"),
                "agent-bravo-devlead".to_string(),
                Err("io error: connection refused".to_string()),
                ws,
                ipc_absent(),
                now,
            );
            assert!(!status.mattermost_ok);
            assert_eq!(status.mattermost_username, "agent-bravo-devlead");
            assert_eq!(
                status.mattermost_last_error.as_deref(),
                Some("io error: connection refused")
            );
            // The reconnect-health surface must be populated regardless.
            assert_eq!(
                status.ws_connection_state,
                Some(WsConnectionState::Disconnected)
            );
            assert_eq!(status.health, Some(DaemonHealthState::Disconnected));
            assert_eq!(status.ws_suspected_gap, Some(true));
            assert_eq!(status.ws_last_disconnect_at, Some(now - 30_000));
            assert_eq!(status.ws_reconnect_count, Some(4));
        }

        #[test]
        fn build_status_uses_whoami_username_when_ok() {
            let now = 1_777_050_000_000;
            let ws = ws_snapshot_for(
                Some(WsConnectionState::Healthy),
                Some(false),
                None,
                Some(0),
                0,
            );
            let status = build_daemon_status(
                "bravo-devlead".to_string(),
                PathBuf::from("/tmp/chanvoy/bravo-devlead.sock"),
                "agent-bravo-devlead".to_string(),
                Ok("agent-bravo-devlead".to_string()),
                ws,
                ipc_absent(),
                now,
            );
            assert!(status.mattermost_ok);
            assert_eq!(status.mattermost_username, "agent-bravo-devlead");
            assert_eq!(status.mattermost_last_error, None);
            assert_eq!(status.health, Some(DaemonHealthState::Healthy));
        }

        #[tokio::test]
        async fn gap_triggered_recovery_clears_suspected_gap_after_grace() {
            // secrev blocker: a >5min outage flags suspected_gap=true,
            // and derive_daemon_health maps Healthy + suspected_gap to
            // Recovering. Without clearing at grace-window completion,
            // the daemon would report recovering / suspected_gap=true
            // for hours after one sleep-wake event, until a *later*
            // reconnect cycle happened to clear it. The clear must
            // happen when the current reconnect's grace window
            // completes cleanly — no additional reconnect required.
            let ws = Arc::new(WsState::new());
            ws.set_state(WsConnectionState::Disconnected).await;
            ws.set_state(WsConnectionState::Healthy).await;
            ws.arm_recovery_window();
            // A gap was detected during this reconnect's catchup path.
            ws.suspected_gap.store(true, Ordering::Relaxed);

            // Within the grace window: derived health is Recovering
            // because suspected_gap=true (AND we're inside the grace
            // window — either condition is sufficient).
            let now = now_unix_millis();
            let (conn, gap, ru) = snapshot(&ws);
            assert!(gap, "precondition: suspected_gap should be set");
            assert_eq!(
                derive_daemon_health(now, Some(conn), gap, ru),
                Some(DaemonHealthState::Recovering)
            );

            // After the grace window elapses without a new disconnect:
            // the task stamps last_recovered_at AND clears
            // suspected_gap. Derived health returns to Healthy without
            // a second reconnect cycle.
            tokio::time::sleep(Duration::from_millis((RECOVERY_GRACE_MS as u64) + 200)).await;

            assert!(
                ws.last_recovered_at.load(Ordering::Relaxed) > 0,
                "last_recovered_at must be stamped at grace-window completion"
            );
            assert!(
                !ws.suspected_gap.load(Ordering::Relaxed),
                "suspected_gap must clear at grace-window completion"
            );
            let now2 = now_unix_millis();
            let (conn2, gap2, ru2) = snapshot(&ws);
            assert_eq!(
                derive_daemon_health(now2, Some(conn2), gap2, ru2),
                Some(DaemonHealthState::Healthy)
            );
        }

        #[tokio::test]
        async fn probe_whoami_returns_promptly_when_remote_is_stalled() {
            // entarch follow-up blocker: `daemon_status` must return
            // promptly even when the Mattermost reachability probe does
            // not complete. A stalled REST call (TCP black-hole, DNS
            // wedge, slow-loris) previously hung the whole RPC, making
            // the reconnect-health surface disappear in the same outage
            // class PER-010 is meant to diagnose. Prove the timeout
            // wrapper fires well before the server's configured delay.
            use wiremock::matchers::{method, path};
            use wiremock::{Mock, MockServer, ResponseTemplate};

            let server = MockServer::start().await;
            // Simulate a stalled REST call: whoami would eventually
            // return 200 after 5s, but the probe must not wait.
            Mock::given(method("GET"))
                .and(path("/api/v4/users/me"))
                .respond_with(
                    ResponseTemplate::new(200)
                        .set_delay(Duration::from_secs(5))
                        .set_body_json(serde_json::json!({
                            "id": "bot-id-123",
                            "username": "agent-bravo-devlead",
                            "is_bot": true,
                            "nickname": null,
                            "email": null,
                        })),
                )
                .mount(&server)
                .await;

            let profile = Profile {
                name: "bravo-devlead".to_string(),
                role: "bravo-devlead".to_string(),
                scope: "lanytehq".to_string(),
                provider: Provider::Mattermost,
                bot_username: "agent-bravo-devlead".to_string(),
                team_name: "org-lanytehq".to_string(),
                server_url: server.uri(),
                env_name: "LANYTE_MM_TOKEN".to_string(),
                env_file: None,
                credential_mode: CredentialMode::EnvName,
                capability_class: CapabilityClass::Standard,
                monitored_channels: Vec::new(),
                ipc: None,
            };
            let client = MattermostClient::new(&profile, "token".into()).unwrap();

            let t0 = std::time::Instant::now();
            let result = probe_whoami(&client, 200).await;
            let elapsed = t0.elapsed();

            assert!(
                elapsed < Duration::from_millis(1_500),
                "probe_whoami must return before server's 5s delay; elapsed={:?}",
                elapsed
            );
            match result {
                Err(msg) => assert!(
                    msg.contains("timed out"),
                    "expected timeout error, got {msg:?}"
                ),
                Ok(_) => panic!("probe must not return Ok when the remote stalls past timeout"),
            }
        }

        #[tokio::test]
        async fn cold_start_auth_does_not_arm_recovery() {
            // First successful auth on a fresh daemon: no prior disconnect,
            // so `arm_recovery_window` must be a no-op and derived health
            // must be Healthy — not Recovering. Recovering is a reconnect
            // signal, not a startup state.
            let ws = Arc::new(WsState::new());
            ws.set_state(WsConnectionState::Healthy).await;
            ws.arm_recovery_window();
            assert_eq!(ws.recovering_until.load(Ordering::Relaxed), 0);
            let (conn, gap, ru) = snapshot(&ws);
            let health = derive_daemon_health(now_unix_millis(), Some(conn), gap, ru);
            assert_eq!(health, Some(DaemonHealthState::Healthy));
        }
    }
}
