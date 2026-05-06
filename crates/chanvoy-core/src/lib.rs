pub mod bootstrap;

pub use bootstrap::{
    bootstrap_path_for_profile, build_bootstrap_state, compute_profile_fingerprint,
    consume_bootstrap_state, generate_nonce, read_bootstrap_state, resolve_startup_identity,
    validate_bootstrap_state, write_bootstrap_state, BootstrapError, BootstrapResolution,
    BootstrapState, BOOTSTRAP_MAX_AGE_SECS, BOOTSTRAP_NONCE_ENV,
};

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
    /// PER-025 AC #6: MM `last_post_at` (Unix epoch ms). `None` is
    /// rendered as `null` in `--json` (deterministic-null shape per
    /// productbook PR #49 cleanup); the field is **not**
    /// `skip_serializing_if`'d on purpose so consumers see a
    /// deterministic shape rather than absent-field. The
    /// `--primary-team --json` legacy path uses a separate
    /// `LegacyChannel` serialization shape that omits this field
    /// entirely (see `Channel::to_legacy`).
    #[serde(default)]
    pub last_post_at: Option<i64>,
}

impl Channel {
    /// PER-025 AC #6a: render the legacy `--primary-team --json` shape
    /// — the pre-PER-019 / pre-PER-025 single-team JSON field set, no
    /// `last_post_at` field at all. Per @agent-entarch-lanytehq's
    /// 2026-05-03 #brief-per-025 pin #5: the same in-memory `Channel`
    /// struct cannot serialize into both the activity-bearing default
    /// shape AND the legacy shape; the explicit projection avoids
    /// accidental field leakage into the legacy contract.
    pub fn to_legacy(&self) -> LegacyChannel {
        LegacyChannel {
            id: self.id.clone(),
            name: self.name.clone(),
            display_name: self.display_name.clone(),
            channel_type: self.channel_type.clone(),
        }
    }
}

/// PER-025 AC #6a: legacy `--primary-team --json` channel shape (no
/// `last_post_at`). Constructed via `Channel::to_legacy` at the CLI
/// rendering layer when `--primary-team` is set.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct LegacyChannel {
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
    /// PER-024: thread root post id when the post was created as a
    /// thread reply via `chanvoy post --reply-to`. **Additive** field
    /// per AC #3a — non-threaded posts omit this entirely (not `null`)
    /// so the existing `{ "id": "<post_id>" }` JSON shape is preserved
    /// byte-for-byte for callers who don't opt into threading.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_id: Option<String>,
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
    let (mattermost_username, mattermost_ok, mattermost_last_error, mattermost_identity_drift) =
        match whoami_result {
            Ok(username) => {
                let drift = if configured_bot_username.is_empty() {
                    None
                } else {
                    Some(username != configured_bot_username)
                };
                (username, true, None, drift)
            }
            Err(msg) => (configured_bot_username.clone(), false, Some(msg), None),
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
        mattermost_identity_drift,
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
    /// PER-023: time window in seconds (resolution upgrade for `30s`/`5m`/
    /// `4h`/`2d` suffix support). Daemon prefers `since_secs` over
    /// `since_minutes` when both are present; the CLI emits seconds for
    /// new invocations and `since_minutes` is retained only so a freshly
    /// upgraded daemon can still decode requests from a not-yet-upgraded
    /// CLI peer in flight on the same machine.
    #[serde(default)]
    pub since_secs: Option<u64>,
    #[serde(default)]
    pub after_post_id: Option<String>,
    #[serde(default)]
    pub since_last_mine: bool,
    /// PER-023 Scope §2 (settled in productbook PR #47): bounded most-recent-N
    /// posts (default N=50; `--limit` overrides). Mode-independent of the
    /// `--since`/`--after`/`--since-last-mine` chain; mutually exclusive
    /// with them at the CLI layer.
    #[serde(default)]
    pub since_bootstrap: bool,
    /// PER-023 Scope §2 + AC #2a: hard cap on the existing read-mode
    /// result set. Daemon truncates the post list returned by the chosen
    /// read mode to at most `limit` entries; PER-023 explicitly does NOT
    /// add full-window pagination semantics. CLI rejects bare
    /// `read --limit N` (no read-mode flag) before reaching the daemon.
    #[serde(default)]
    pub limit: Option<u32>,
    /// PER-023 Scope §4 + AC #4: when set, daemon advances the channel
    /// attention cursor to the latest post **returned** by this read
    /// (mode-independent rule). No-op when zero posts are returned.
    #[serde(default)]
    pub advance: bool,
    /// PER-019: optional `--team <slug>` override for cross-team
    /// disambiguation. Per-invocation only (no profile-level toggle).
    #[serde(default)]
    pub team: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PostMessageParams {
    pub channel: String,
    pub message: String,
    /// PER-019: optional `--team <slug>` override.
    #[serde(default)]
    pub team: Option<String>,
    /// PER-024 primitive 1: when set, the post is created as a
    /// thread reply under the named parent (MM `root_id`). Validation
    /// order: resolve channel → verify parent exists on resolved
    /// channel → write. Refuse wrong-channel parents with a clear
    /// diagnostic before the write is attempted.
    #[serde(default)]
    pub thread_root_id: Option<String>,
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
    /// PER-023: time window in seconds. Daemon prefers `since_secs` over
    /// `since_minutes` when both are present.
    #[serde(default)]
    pub since_secs: Option<u64>,
    #[serde(default)]
    pub unread_only: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CheckChannelParams {
    pub channel: String,
    #[serde(default)]
    pub after_post_id: Option<String>,
    /// PER-019: optional `--team <slug>` override.
    #[serde(default)]
    pub team: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AttentionShowParams {
    pub channel: String,
    /// PER-019: optional `--team <slug>` override.
    #[serde(default)]
    pub team: Option<String>,
}

/// PER-023 primitive 1: parameters for `chanvoy pinned <channel>`. Pure
/// read; daemon does not advance any cursor regardless of result.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PinnedChannelParams {
    pub channel: String,
    #[serde(default)]
    pub team: Option<String>,
}

/// PER-024 primitive 2: parameters for `chanvoy react <channel>
/// <post_id> <emoji>`. Channel context is positional (required) for
/// multi-provider portability — Slack's reactions API needs the
/// channel-id tuple even though Mattermost can key by post-id alone.
/// See PER-024 Multi-Provider Portability Note for the rationale.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReactParams {
    pub channel: String,
    pub post_id: String,
    pub emoji: String,
    #[serde(default)]
    pub team: Option<String>,
}

/// PER-024 primitive 2: parameters for `chanvoy unreact <channel>
/// <post_id> <emoji>`. Same shape as `ReactParams`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct UnreactParams {
    pub channel: String,
    pub post_id: String,
    pub emoji: String,
    #[serde(default)]
    pub team: Option<String>,
}

/// PER-024 primitive 2: outcome of a `react` / `unreact` call. The
/// resolved `team` and `channel` are surfaced so JSON consumers can
/// disambiguate cross-team channel duplicates (PER-019). The `emoji`
/// field reflects the **normalized** value sent to the MM API
/// (colon-stripped if the input was `:emoji:` form). `ok` is always
/// `true` when this struct surfaces — failures bubble as
/// `CoreError`s. Per @agent-bravo-devrev's PER-024 pre-impl pin #2
/// (2026-05-04): includes both `team` and `channel` mirroring
/// `AckResult`'s pattern.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReactionResult {
    pub team: String,
    pub channel: String,
    pub post_id: String,
    pub emoji: String,
    pub ok: bool,
}

/// PER-025 primitive 1: parameters for `chanvoy search <channel>
/// <query>`. Cross-channel / team-wide search is explicitly deferred
/// from v1 per cross-reviewer alignment (see brief §Out of scope) —
/// channel arg is always required.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SearchParams {
    pub channel: String,
    pub query: String,
    /// Default 20 if omitted; CLI populates via `--limit N`.
    #[serde(default)]
    pub limit: Option<u32>,
    /// MM `from:<author>` operator narrowing; CLI flag value.
    #[serde(default)]
    pub from: Option<String>,
    /// PER-023 time-window suffix-parsed seconds; CLI flag value.
    /// Daemon converts to MM `after:<computed-iso-date>` operator.
    #[serde(default)]
    pub since_secs: Option<u64>,
    /// PER-019 explicit team override.
    #[serde(default)]
    pub team: Option<String>,
}

/// PER-025 primitive 1: outcome of a `chanvoy search` call. The
/// resolved `team` and `channel` are surfaced so JSON consumers can
/// disambiguate cross-team channel duplicates (mirrors PER-024's
/// `ReactionResult` pattern). `posts` are the matching messages
/// in MM's returned order (newest-first per MM's search ranking,
/// preserved verbatim — chanvoy doesn't re-sort).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SearchResult {
    pub team: String,
    pub channel: String,
    pub posts: Vec<Message>,
}

/// PER-024: strip surrounding ASCII colons from an emoji name if the
/// operator typed the MM-UI form `:emoji:`. Pass-through when bare.
/// Unmatched single colons are preserved verbatim (the chanvoy CLI
/// is not an emoji validator — MM rejects unknown names with a
/// clear error if needed). Per @agent-bravo-devrev's PER-024
/// pre-impl pin #4 (2026-05-04): the **stripped** value is what
/// gets sent to the MM API and surfaces in `--json`.
pub fn normalize_emoji_name(input: &str) -> String {
    let trimmed = input.trim();
    if trimmed.len() >= 2 && trimmed.starts_with(':') && trimmed.ends_with(':') {
        trimmed[1..trimmed.len() - 1].to_string()
    } else {
        trimmed.to_string()
    }
}

#[cfg(test)]
mod emoji_tests {
    use super::*;

    #[test]
    fn bare_emoji_unchanged() {
        assert_eq!(normalize_emoji_name("+1"), "+1");
        assert_eq!(normalize_emoji_name("thumbsup"), "thumbsup");
        assert_eq!(normalize_emoji_name("heavy_check_mark"), "heavy_check_mark");
    }

    #[test]
    fn colon_wrapped_stripped() {
        assert_eq!(normalize_emoji_name(":+1:"), "+1");
        assert_eq!(normalize_emoji_name(":eyes:"), "eyes");
    }

    #[test]
    fn unmatched_colon_preserved() {
        // Single trailing or leading colon isn't the MM-UI form;
        // pass through verbatim so MM can reject it cleanly.
        assert_eq!(normalize_emoji_name(":+1"), ":+1");
        assert_eq!(normalize_emoji_name("+1:"), "+1:");
    }

    #[test]
    fn whitespace_trimmed() {
        assert_eq!(normalize_emoji_name("  +1  "), "+1");
        assert_eq!(normalize_emoji_name("  :+1:  "), "+1");
    }

    #[test]
    fn empty_or_only_colons() {
        // ":" by itself is one char so the colon-pair check fails
        // (len >= 2 required); pass through.
        assert_eq!(normalize_emoji_name(":"), ":");
        // "::" is two colons — passes the colon-pair check, strips
        // to empty string. MM will reject empty; that's the right
        // place for the error.
        assert_eq!(normalize_emoji_name("::"), "");
    }
}

/// PER-025 primitive 1: chanvoy-owned scopes that can conflict with
/// inline MM search operators. Caller passes flags into
/// `check_search_operator_conflicts` so the scan knows which
/// chanvoy-owned scope is active and which inline operators would
/// therefore conflict.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ChanvoyScopes {
    /// `<channel>` positional arg is set on `chanvoy search` —
    /// chanvoy auto-scopes via `in:<resolved-channel>` so an inline
    /// `in:` from the operator's query conflicts.
    pub channel_arg: bool,
    /// `--from <author>` flag is set — inline `from:<user>` conflicts.
    pub from_flag: bool,
    /// `--since <time>` flag is set — inline `before:`/`after:`
    /// conflicts (both are date-window operators that overlap with
    /// `--since`'s computed window).
    pub since_flag: bool,
}

/// PER-025 AC #4a (cleared via productbook PR #49): scan an MM search
/// query for inline operators that conflict with chanvoy-owned scopes.
/// Returns `Ok(())` if no conflict; `Err(diagnostic)` on the first
/// conflict found.
///
/// Per @agent-entarch-lanytehq's 2026-05-03 #brief-per-025 pin #3:
/// chanvoy refuses with a clear diagnostic naming the conflicting
/// flag/arg explicitly so operators can fix. Non-conflicting MM
/// operators (anything chanvoy doesn't claim ownership of, plus
/// `before:`/`after:` when `--since` is unset, etc.) pass through
/// verbatim — chanvoy doesn't parse them; MM handles.
///
/// Quoted-region handling per @agent-bravo-devlead's 2026-05-05
/// preread implementor-call disposition #2: tokens inside balanced
/// double-quoted regions are treated as literal search text, NOT
/// operators. So `chanvoy search per-019 "in: the brief"` does NOT
/// conflict with the channel arg even though the literal substring
/// `in:` appears, because it's quoted-as-text.
pub fn check_search_operator_conflicts(query: &str, scopes: &ChanvoyScopes) -> Result<(), String> {
    for token in iter_unquoted_tokens(query) {
        let lower = token.to_ascii_lowercase();
        if lower.starts_with("in:") && scopes.channel_arg {
            return Err(format!(
                "inline `in:` operator in search query conflicts with the \
                 channel argument; channel argument defines search scope; \
                 remove inline `in:` or run a future team-wide search mode \
                 (offending token: {token:?})"
            ));
        }
        if lower.starts_with("from:") && scopes.from_flag {
            return Err(format!(
                "inline `from:` operator in search query conflicts with the \
                 `--from` flag; pick one (offending token: {token:?})"
            ));
        }
        if (lower.starts_with("before:") || lower.starts_with("after:")) && scopes.since_flag {
            return Err(format!(
                "inline `{op}` operator in search query conflicts with the \
                 `--since` flag (both define the search time window); pick \
                 one (offending token: {token:?})",
                op = if lower.starts_with("before:") {
                    "before:"
                } else {
                    "after:"
                }
            ));
        }
    }
    Ok(())
}

/// Yield successive whitespace-delimited tokens of `input` that lie
/// **outside** balanced double-quoted regions. Quoted regions are
/// skipped entirely (their contents are search literals, not
/// operators). Unmatched closing quotes are tolerated — the rest of
/// the string is treated as quoted text up to end-of-input.
fn iter_unquoted_tokens(input: &str) -> Vec<&str> {
    let mut out = Vec::new();
    let bytes = input.as_bytes();
    let mut i = 0;
    let mut in_quote = false;
    let mut token_start: Option<usize> = None;
    while i < bytes.len() {
        let c = bytes[i] as char;
        if c == '"' {
            // Flush any pending unquoted token before flipping the
            // quote state.
            if let Some(start) = token_start.take() {
                if start < i {
                    out.push(&input[start..i]);
                }
            }
            in_quote = !in_quote;
            i += 1;
            continue;
        }
        if in_quote {
            i += 1;
            continue;
        }
        if c.is_ascii_whitespace() {
            if let Some(start) = token_start.take() {
                if start < i {
                    out.push(&input[start..i]);
                }
            }
        } else if token_start.is_none() {
            token_start = Some(i);
        }
        i += 1;
    }
    if let Some(start) = token_start {
        if start < bytes.len() {
            out.push(&input[start..]);
        }
    }
    out
}

#[cfg(test)]
mod search_operator_tests {
    use super::*;

    fn all_scopes() -> ChanvoyScopes {
        ChanvoyScopes {
            channel_arg: true,
            from_flag: true,
            since_flag: true,
        }
    }

    #[test]
    fn no_inline_operators_passes() {
        let r = check_search_operator_conflicts("parent_pid validation", &all_scopes());
        assert!(r.is_ok(), "plain text query should pass: {r:?}");
    }

    #[test]
    fn inline_in_conflicts_with_channel_arg() {
        let r = check_search_operator_conflicts(
            "parent_pid in:other-channel",
            &ChanvoyScopes {
                channel_arg: true,
                ..Default::default()
            },
        );
        let err = r.unwrap_err();
        assert!(
            err.contains("`in:`"),
            "diagnostic should name `in:`; got: {err}"
        );
        assert!(
            err.contains("channel argument"),
            "diagnostic should name the conflict; got: {err}"
        );
        assert!(
            !err.contains("--team-wide"),
            "diagnostic must NOT name nonexistent --team-wide flag; got: {err}"
        );
    }

    #[test]
    fn inline_from_conflicts_with_from_flag() {
        let r = check_search_operator_conflicts(
            "validation from:entarch",
            &ChanvoyScopes {
                from_flag: true,
                ..Default::default()
            },
        );
        let err = r.unwrap_err();
        assert!(err.contains("`from:`"), "got: {err}");
        assert!(err.contains("--from"), "got: {err}");
    }

    #[test]
    fn inline_before_conflicts_with_since_flag() {
        let r = check_search_operator_conflicts(
            "x before:2026-05-01",
            &ChanvoyScopes {
                since_flag: true,
                ..Default::default()
            },
        );
        let err = r.unwrap_err();
        assert!(err.contains("`before:`"), "got: {err}");
        assert!(err.contains("--since"), "got: {err}");
    }

    #[test]
    fn inline_after_conflicts_with_since_flag() {
        let r = check_search_operator_conflicts(
            "x after:2026-05-01",
            &ChanvoyScopes {
                since_flag: true,
                ..Default::default()
            },
        );
        let err = r.unwrap_err();
        assert!(err.contains("`after:`"), "got: {err}");
    }

    #[test]
    fn non_conflicting_inline_passes() {
        // `from:` without --from flag set should pass through.
        let r =
            check_search_operator_conflicts("validation from:entarch", &ChanvoyScopes::default());
        assert!(r.is_ok(), "from: without flag should pass: {r:?}");
        // Same for before: when --since unset.
        let r = check_search_operator_conflicts("x before:2026-05-01", &ChanvoyScopes::default());
        assert!(r.is_ok());
    }

    #[test]
    fn unknown_inline_operator_passes_through() {
        // chanvoy doesn't claim ownership of arbitrary MM operators;
        // they pass through to MM regardless of which flags are set.
        let r = check_search_operator_conflicts("validation has:link", &all_scopes());
        assert!(r.is_ok(), "unknown operator should pass: {r:?}");
    }

    #[test]
    fn quoted_in_is_search_text_not_operator() {
        // `"in: the brief"` is searchable text — operator-conflict
        // scanner must NOT flag it.
        let r = check_search_operator_conflicts(
            "\"in: the brief\" parent_pid",
            &ChanvoyScopes {
                channel_arg: true,
                ..Default::default()
            },
        );
        assert!(
            r.is_ok(),
            "quoted in: should be search text, not operator: {r:?}"
        );
    }

    #[test]
    fn quoted_from_is_search_text() {
        let r = check_search_operator_conflicts(
            "\"from: anywhere\" validation",
            &ChanvoyScopes {
                from_flag: true,
                ..Default::default()
            },
        );
        assert!(r.is_ok(), "quoted from: should pass: {r:?}");
    }

    #[test]
    fn case_insensitive_operator_match() {
        // MM accepts `In:`, `IN:` etc. The scan should catch all
        // case variants so a typo doesn't sneak past.
        for variant in ["IN:other", "In:other", "iN:other"] {
            let r = check_search_operator_conflicts(
                variant,
                &ChanvoyScopes {
                    channel_arg: true,
                    ..Default::default()
                },
            );
            assert!(r.is_err(), "variant {variant} should conflict");
        }
    }

    #[test]
    fn empty_query_passes() {
        let r = check_search_operator_conflicts("", &all_scopes());
        assert!(r.is_ok());
    }

    #[test]
    fn quoted_region_with_unclosed_trailing_quote_treated_as_quoted() {
        // Unclosed trailing quote: rest of string is quoted-text up
        // to end-of-input. So `parent_pid "in: stuff` has only
        // "parent_pid" as an unquoted token — the rest is text.
        let r = check_search_operator_conflicts(
            "parent_pid \"in: stuff",
            &ChanvoyScopes {
                channel_arg: true,
                ..Default::default()
            },
        );
        assert!(
            r.is_ok(),
            "unclosed quote: rest is text, not operator: {r:?}"
        );
    }

    #[test]
    fn first_conflict_wins_diagnostic_is_specific() {
        // Multiple inline conflicts: first-found is reported. This
        // matches PER-025 brief intent — operator fixes one issue,
        // re-runs, sees the next.
        let r = check_search_operator_conflicts("in:chan from:user", &all_scopes());
        let err = r.unwrap_err();
        // `in:` comes first in the query, so it's the diagnostic.
        assert!(err.contains("`in:`"), "got: {err}");
    }
}

/// PER-023 primitive 4: parameters for `chanvoy ack <channel>`. Daemon
/// fetches the channel's current latest post id (without surfacing
/// content) and advances the attention cursor.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AckChannelParams {
    pub channel: String,
    #[serde(default)]
    pub team: Option<String>,
}

/// PER-023 primitive 4: outcome of an `ack` call. Carries the resolved
/// channel + the cursor target so JSON consumers can confirm what was
/// ack'd. `cursor_post_id == None` means the channel had no posts —
/// cursor is unchanged.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AckResult {
    pub channel: String,
    pub team: String,
    pub cursor_post_id: Option<String>,
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
    /// PER-019 (secrev PR #17 finding #2): legacy cursor records that
    /// the qualified-key migration could not bind cleanly because the
    /// channel name resolved on multiple member teams. Surfaced here
    /// so operators can see them and disambiguate; the next read/post
    /// on the channel via `--team` or `<team>/<channel>` syntax
    /// re-establishes a fresh cursor under the correct qualified key.
    /// `#[serde(default)]` keeps wire-format back-compat.
    #[serde(default)]
    pub quarantined: Vec<QuarantinedCursor>,
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
///
/// PER-019 (entarch PR #17 finding + secrev residual): seed pre-filter
/// must understand the qualified-key cursor format. Pre-PER-019 the
/// `existing_cursors` set was bare names; post-migration the daemon
/// passes qualified `<team>/<channel>` keys. The helper enumerates
/// only primary-team channels, so qualify each enumerated name with
/// the primary team before checking the existing-cursors set.
/// Bare-name fallback is retained for any callers that haven't migrated
/// yet (or pre-migration state).
pub async fn compute_seed_outcomes(
    client: &MattermostClient,
    existing_cursors: &std::collections::BTreeSet<String>,
) -> Result<Vec<SeededChannelOutcome>, CoreError> {
    let channels = client.list_channels().await?;
    let primary_team = client.primary_team_name();
    let mut outcomes = Vec::new();
    for channel in channels {
        if channel.channel_type != "O" && channel.channel_type != "P" {
            continue;
        }
        let qualified = attention_key_for(primary_team, &channel.name);
        if existing_cursors.contains(&qualified) || existing_cursors.contains(&channel.name) {
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
    /// PER-019: keyed by qualified `<team_name>/<channel_name>` so
    /// same-named channels on different teams maintain independent
    /// cursors (AC #6). Pre-PER-019 entries with bare-name keys are
    /// migrated at daemon `start()` (see `migrate_attention_state`).
    #[serde(default)]
    pub channels: BTreeMap<String, ChannelCursorState>,
    #[serde(default)]
    pub mentions: MentionCursorState,
    /// PER-019: cursor records that couldn't be migrated cleanly because
    /// the channel name resolves on multiple member teams. Held aside
    /// rather than silently bound to one team — operator must reissue
    /// reads/posts via `--team <slug>` or `<team>/<channel>` syntax to
    /// re-establish cursors per-team. Surfaced via `attention list` so
    /// the situation is visible.
    #[serde(default)]
    pub quarantined: Vec<QuarantinedCursor>,
}

/// PER-019: build a qualified attention-state key from a resolved
/// channel. Mirrors the explicit `<team>/<channel>` syntax operators
/// already type, so state-file inspection stays human-readable.
pub fn attention_key_for(team_name: &str, channel_name: &str) -> String {
    format!("{team_name}/{channel_name}")
}

/// PER-019: a cursor record that couldn't be migrated because the
/// channel name was ambiguous across the bot's member teams at
/// migration time. Preserved verbatim for diagnostic display; the
/// operator's next read/post/check on the channel re-establishes a
/// fresh cursor under the qualified key for whichever team they
/// disambiguated to.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct QuarantinedCursor {
    /// Original bare channel name (the pre-PER-019 key).
    pub legacy_channel_name: String,
    /// Names of the teams the channel resolved on at migration time.
    pub ambiguous_teams: Vec<String>,
    /// The original cursor record, preserved as-is.
    pub state: ChannelCursorState,
    /// Unix-millis timestamp the migration ran.
    pub quarantined_at: i64,
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
    /// PER-019 denormalized metadata. Populated on cursor writes after
    /// the resolver runs; pre-PER-019 records may have empty strings
    /// until migration touches them. Channel-id is the canonical
    /// Mattermost identifier; the team/channel name pair is the
    /// human-readable form preserved for state-file inspection.
    #[serde(default)]
    pub channel_id: String,
    #[serde(default)]
    pub team_id: String,
    #[serde(default)]
    pub team_name: String,
    #[serde(default)]
    pub channel_name: String,
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
    /// PER-023: timeout in seconds. Daemon prefers `timeout_secs` over
    /// `timeout_minutes` when set.
    #[serde(default)]
    pub timeout_secs: Option<u64>,
    /// PER-019: optional `--team <slug>` override.
    #[serde(default)]
    pub team: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CreateChannelParams {
    pub name: String,
    pub display_name: String,
    #[serde(default)]
    pub purpose: Option<String>,
    /// Optional team override. When `None`, the channel is created on
    /// the profile's primary team (pre-PER-019 / pre-v0.2.1 default).
    /// When `Some(slug)`, the channel is created on the named team
    /// (must be a team the bot is a member of). Closes the cross-team
    /// admin-verb gap that the PER-019 γ resolver left on
    /// `channel create` — every other PER-025-era verb routes
    /// cross-team correctly via the resolver chain, but
    /// `create_channel` historically went straight through
    /// `team_id()` (primary). v0.2.1 release-prep addition.
    #[serde(default)]
    pub team: Option<String>,
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
    /// PER-014: post-bind drift signal — `true` when a successful
    /// `whoami()` returned a username that does NOT match the configured
    /// `bot_username`. `None` when the probe has not yet completed
    /// successfully or has not been called. The local socket remains
    /// bound on drift; network-backed RPCs surface a clear diagnostic.
    #[serde(default)]
    pub mattermost_identity_drift: Option<bool>,
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
    /// PER-014: parent-side auto-setup advertised a bootstrap handoff via
    /// `CHANVOY_BOOTSTRAP_NONCE` but the daemon child could not find the
    /// per-profile bootstrap-state file. This is a failed handoff (likely
    /// causes: runtime-dir drift between parent and child, sandbox /tmp
    /// cleanup, or a consume race), not a legacy manual `daemon serve`
    /// invocation. Refuse with a clear diagnostic so operators can
    /// distinguish from the legacy path. Per agent-bravo-devrev's PR #16
    /// finding (2026-04-27).
    #[error(
        "PER-014 bootstrap handoff failed for profile {profile}: \
         {nonce_env} is set but {path:?} is missing. \
         Likely runtime-dir drift between auto-setup and daemon, \
         sandbox temp-dir cleanup, or a consume race. \
         Re-run `chanvoy auto-setup` to re-validate identity."
    )]
    BootstrapHandoffFailed {
        profile: String,
        nonce_env: &'static str,
        path: PathBuf,
    },
    /// PER-019: channel name not found on any team the bot is a member of.
    /// Lists the searched teams in the diagnostic so operators can verify
    /// the spelling and the membership coverage at the same time.
    #[error(
        "channel {channel:?} not found on any team you are a member of. \
         Teams searched: {teams:?}. \
         If the channel exists on a different team, ask dispatch to add \
         the bot, or use the `<team>/<channel>` syntax with a team you are \
         a member of."
    )]
    ChannelNotFoundInAnyTeam { channel: String, teams: Vec<String> },
    /// PER-019: explicit `<team>/<channel>` requested a team the bot is
    /// not a member of. Distinct from `ChannelNotFoundInAnyTeam` so
    /// operators can distinguish "I typed the wrong team" from "the bot
    /// is not in that team yet".
    #[error(
        "team {team:?} requested via <team>/<channel> syntax, but you are \
         not a member of it. Teams you are a member of: {teams:?}."
    )]
    NotAMemberOfTeam { team: String, teams: Vec<String> },
    /// PER-019: channel name resolves on multiple teams the bot is a
    /// member of. Refuse with the team list so the operator can pick
    /// via `--team <slug>` or `<team>/<channel>` syntax.
    #[error(
        "channel {channel:?} is ambiguous — found on multiple teams: \
         {teams:?}. Use `--team <slug>` or `<team>/<channel>` syntax to \
         disambiguate."
    )]
    AmbiguousChannel { channel: String, teams: Vec<String> },
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

/// Fallback policy for chanvoy CLI default profile resolution.
///
/// PER-012 / crucible spec §"Chanvoy Profile Naming". Side-effecting
/// verbs whose target uncertainty could disrupt another operator's
/// state on a shared dev machine MUST resolve via explicit sources
/// only. Read/inspect/post verbs may consult the broader fallback
/// chain (single running daemon, then `active_profile` marker).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FallbackPolicy {
    /// Resolve only via explicit sources: `--profile` flag,
    /// `CHANVOY_PROFILE` env var, or `LANYTE_AGENT_ROLE` +
    /// `LANYTE_AGENT_SCOPE` exact-name match. Refuse on any fallback.
    /// Used by daemon lifecycle verbs (`daemon stop` etc.) where a
    /// stale fallback could affect another operator's daemon.
    ExplicitOnly,
    /// In addition to explicit sources, allow single-running-daemon
    /// and `active_profile` fallbacks. Used by read/inspect/post verbs
    /// where mis-attribution risk is bounded by membership/permissions.
    AllowReadFallbacks,
}

/// Inputs to the pure profile resolver. All fields are I/O snapshots —
/// the caller is responsible for gathering them; the resolver itself
/// is side-effect-free for testability.
pub struct ResolverInputs<'a> {
    pub profiles: &'a [String],
    pub running_daemon_profiles: &'a [String],
    pub active_profile: Option<&'a str>,
    pub env_role: Option<&'a str>,
    pub env_scope: Option<&'a str>,
    pub env_chanvoy_profile: Option<&'a str>,
}

/// Reasons the resolver may refuse. Each variant is a distinct
/// operator-visible failure mode with a tailored remediation hint.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum ResolverError {
    #[error(
        "CHANVOY_PROFILE is set to '{name}' but no such profile exists; \
         create it or unset the env var. Available profiles: {available:?}"
    )]
    EnvProfileNotFound {
        name: String,
        available: Vec<String>,
    },
    #[error(
        "env identity is {role}/{scope} but no profile named '{expected}' exists; \
         run `chanvoy auto-setup` to materialize it. Available profiles: {available:?}"
    )]
    EnvExactMatchNotFound {
        role: String,
        scope: String,
        expected: String,
        available: Vec<String>,
    },
    #[error(
        "multiple chanvoy daemons are running ({running:?}); \
         pass --profile to disambiguate"
    )]
    AmbiguousMultiDaemon { running: Vec<String> },
    #[error(
        "destructive verb requires explicit profile selection; \
         pass --profile, set CHANVOY_PROFILE, or source an identity script. \
         Available profiles: {available:?}"
    )]
    DestructiveRequiresExplicit { available: Vec<String> },
    #[error(
        "the persistent active_profile marker points at '{name}' but no such profile exists \
         (likely renamed or deleted); pass --profile, set LANYTE_AGENT_ROLE+LANYTE_AGENT_SCOPE, \
         or run `chanvoy auto-setup` to refresh the marker. Available profiles: {available:?}"
    )]
    ActiveProfileNotFound {
        name: String,
        available: Vec<String>,
    },
    #[error(
        "unable to resolve a chanvoy profile; \
         pass --profile or set LANYTE_AGENT_ROLE+LANYTE_AGENT_SCOPE. \
         Available profiles: {available:?}"
    )]
    CannotResolve { available: Vec<String> },
}

/// Resolve the chanvoy profile a CLI invocation should target.
///
/// Implements the canonical rule from
/// `lanyte-crucible/docs/specs/agent-chat-conventions.md`
/// §"Chanvoy Profile Naming":
///
/// 1. Explicit `--profile` flag — always wins, no validation.
/// 2. `CHANVOY_PROFILE` env — refuse if the named profile does not exist.
/// 3. `${LANYTE_AGENT_ROLE}-${LANYTE_AGENT_SCOPE}` exact-name match —
///    refuse hard when the env is set but no canonical-name profile
///    exists, rather than fall through to a different identity (this
///    is the silent mis-attribution class PER-012 closes).
/// 4. Single running daemon (only with `AllowReadFallbacks`).
/// 5. `active_profile` marker file (only with `AllowReadFallbacks`).
/// 6. Refuse with the live-profile list.
///
/// Pure function: no I/O. The caller must provide a snapshot of the
/// relevant filesystem and environment state via `ResolverInputs`.
pub fn resolve_profile_name(
    profile_flag: Option<&str>,
    policy: FallbackPolicy,
    inputs: &ResolverInputs<'_>,
) -> Result<String, ResolverError> {
    // Rule 1: explicit --profile flag — operator's stated intent.
    if let Some(name) = profile_flag {
        return Ok(name.to_string());
    }

    let available = || inputs.profiles.to_vec();

    // Rule 2: CHANVOY_PROFILE env. Must point at an existing profile.
    if let Some(name) = inputs.env_chanvoy_profile {
        if inputs.profiles.iter().any(|p| p == name) {
            return Ok(name.to_string());
        }
        return Err(ResolverError::EnvProfileNotFound {
            name: name.to_string(),
            available: available(),
        });
    }

    // Rule 3: ${ROLE}-${SCOPE} exact-name. Hard-refuse on env-set-no-match.
    if let (Some(role), Some(scope)) = (inputs.env_role, inputs.env_scope) {
        let expected = format!("{role}-{scope}");
        if inputs.profiles.iter().any(|p| p == &expected) {
            return Ok(expected);
        }
        return Err(ResolverError::EnvExactMatchNotFound {
            role: role.to_string(),
            scope: scope.to_string(),
            expected,
            available: available(),
        });
    }

    // Below this point is fallback territory; explicit-only verbs refuse.
    if matches!(policy, FallbackPolicy::ExplicitOnly) {
        return Err(ResolverError::DestructiveRequiresExplicit {
            available: available(),
        });
    }

    // Rule 4: single running daemon.
    match inputs.running_daemon_profiles.len() {
        0 => {} // continue to rule 5
        1 => return Ok(inputs.running_daemon_profiles[0].clone()),
        _ => {
            return Err(ResolverError::AmbiguousMultiDaemon {
                running: inputs.running_daemon_profiles.to_vec(),
            });
        }
    }

    // Rule 5: active_profile marker file. Below env + daemon — never
    // overrides env-derived resolution. Demoted in PER-012, Option A.
    // Validate membership the same way rule 2 validates
    // CHANVOY_PROFILE: a marker pointing at a deleted/renamed profile
    // is dead state, not a valid resolution. Surface the stale-marker
    // diagnosis with its own variant rather than silent fall-through
    // so the operator's remediation path is clear (refresh marker via
    // auto-setup, or delete it). PER-012, entarch follow-up.
    if let Some(active) = inputs.active_profile {
        if inputs.profiles.iter().any(|p| p == active) {
            return Ok(active.to_string());
        }
        return Err(ResolverError::ActiveProfileNotFound {
            name: active.to_string(),
            available: available(),
        });
    }

    // Rule 6: refuse.
    Err(ResolverError::CannotResolve {
        available: available(),
    })
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

/// PER-019: walk an `AttentionState` and migrate pre-PER-019 cursor
/// records (keyed by bare channel name) to the new qualified
/// `<team_name>/<channel_name>` keying with denormalized metadata.
///
/// Migration rule (per devrev's PR #40 pin):
///
/// - Unique resolution (one match across the bot's member teams) →
///   migrate cleanly, preserving `last_seen_post_id` /
///   `last_checked_at` / `last_known_stale` / `updated_at`.
/// - Ambiguous resolution (multiple member teams have a channel with
///   this name) → quarantine the record, do not silently bind to one
///   team. Operator's next read/post on that channel re-establishes a
///   fresh cursor under the qualified key for the team they pick.
/// - No resolution (channel not found on any team) → leave the entry
///   alone. Could be a deleted channel or a bot that lost team
///   membership; operator visibility comes through `attention list`.
///
/// Idempotent: an already-migrated entry (key contains `/`, metadata
/// populated) walks but rewrites nothing.
///
/// Returns the count of entries migrated, quarantined, and skipped so
/// the caller can log a summary line.
pub async fn migrate_attention_state(
    state: &mut AttentionState,
    client: &MattermostClient,
) -> Result<MigrationOutcome, CoreError> {
    let mut migrated = 0usize;
    let mut quarantined = 0usize;
    let mut skipped = 0usize;
    let now = now_unix_millis();
    let primary_team = client.primary_team_name().to_string();

    let legacy_keys: Vec<String> = state
        .channels
        .keys()
        .filter(|k| !k.contains('/'))
        .cloned()
        .collect();

    for legacy_name in legacy_keys {
        let Some(legacy_state) = state.channels.remove(&legacy_name) else {
            continue;
        };

        // Try primary first — if hit there, migrate cleanly even if
        // fallback teams also have a same-named channel (devrev's pin:
        // primary-team-first lookup wins).
        match client
            .resolve_channel(&legacy_name, Some(&primary_team))
            .await
        {
            Ok(resolved) => {
                state.channels.insert(
                    attention_key_for(&resolved.team_name, &resolved.channel_name),
                    ChannelCursorState {
                        last_seen_post_id: legacy_state.last_seen_post_id,
                        updated_at: legacy_state.updated_at,
                        last_known_stale: legacy_state.last_known_stale,
                        last_checked_at: legacy_state.last_checked_at,
                        channel_id: resolved.channel_id,
                        team_id: resolved.team_id,
                        team_name: resolved.team_name,
                        channel_name: resolved.channel_name,
                    },
                );
                migrated += 1;
            }
            Err(_) => {
                // Primary missed; try the fallback path with strict
                // resolution. Single match → migrate; multiple matches →
                // quarantine; none → skip.
                match client.resolve_channel(&legacy_name, None).await {
                    Ok(resolved) => {
                        state.channels.insert(
                            attention_key_for(&resolved.team_name, &resolved.channel_name),
                            ChannelCursorState {
                                last_seen_post_id: legacy_state.last_seen_post_id,
                                updated_at: legacy_state.updated_at,
                                last_known_stale: legacy_state.last_known_stale,
                                last_checked_at: legacy_state.last_checked_at,
                                channel_id: resolved.channel_id,
                                team_id: resolved.team_id,
                                team_name: resolved.team_name,
                                channel_name: resolved.channel_name,
                            },
                        );
                        migrated += 1;
                    }
                    Err(CoreError::AmbiguousChannel { teams, .. }) => {
                        state.quarantined.push(QuarantinedCursor {
                            legacy_channel_name: legacy_name.clone(),
                            ambiguous_teams: teams,
                            state: legacy_state,
                            quarantined_at: now,
                        });
                        quarantined += 1;
                    }
                    Err(_) => {
                        // No match anywhere — leave under legacy key.
                        // Operator will see it via attention list and
                        // can clean up explicitly.
                        state.channels.insert(legacy_name, legacy_state);
                        skipped += 1;
                    }
                }
            }
        }
    }

    Ok(MigrationOutcome {
        migrated,
        quarantined,
        skipped,
    })
}

/// PER-019: return value of [`migrate_attention_state`] for daemon-side
/// logging.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MigrationOutcome {
    pub migrated: usize,
    pub quarantined: usize,
    pub skipped: usize,
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

pub fn seconds_ago_millis(seconds: u64) -> i64 {
    now_unix_millis() - Duration::from_secs(seconds).as_millis() as i64
}

/// PER-023: which unit applies when an operator passes a bare integer
/// (no suffix) to a time-window flag. Matches the per-flag semantics
/// shipped before PER-023 — bare integer preserves today's behavior so
/// existing `--since 30` / `--timeout 10` invocations continue to work
/// unchanged. `30m` / `10m` is the preferred new shape.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimeWindowDefaultUnit {
    /// Bare integer = minutes. Used by `read --since`,
    /// `notifications --since`, `wait --timeout`.
    Minutes,
    /// Bare integer = seconds. Reserved for future flags where
    /// sub-minute resolution is the natural default.
    Seconds,
}

/// Help-text disclosure for any flag that takes a time-window value.
/// Per PER-023 Scope §3 + AC #3 the help text MUST disclose both the
/// accepted-suffix list and the rejected-suffix list explicitly so
/// operators don't hit silent unit-confusion footguns. Embed via clap's
/// `#[arg(long_help = ...)]` on the flag definition.
pub const TIME_WINDOW_SUFFIX_HELP: &str = "\
Accepted suffixes: s (seconds), m (minutes), h (hours), d (days). \
Bare integer (no suffix) preserves today's per-flag default unit. \
Rejected with diagnostic: uppercase 'M' and 'mo' (months/minutes ambiguity \
given chanvoy's existing minutes-default).";

/// PER-023 Scope §3 (settled in productbook PR #47): parse a time-window
/// string into seconds, applying the per-flag default unit to bare
/// integers.
///
/// Accepted suffixes: `s` / `m` / `h` / `d`. Bare integer (no suffix)
/// preserves today's per-flag semantics. Uppercase `M` and `mo` are
/// rejected with diagnostics naming the ambiguity — loud failure on
/// ambiguous-intent input is consistent with the brief's other contract
/// edges (e.g., bare `read --limit N` rejection); reject-then-relax
/// preserves optionality if a future brief introduces months as a
/// distinct unit.
pub fn parse_time_window(input: &str, default_unit: TimeWindowDefaultUnit) -> Result<u64, String> {
    let trimmed = input.trim();
    if trimmed.is_empty() {
        return Err(format!(
            "empty time-window value; {TIME_WINDOW_SUFFIX_HELP}"
        ));
    }
    // `mo` (months) — rejected upfront so a typo doesn't silently parse
    // as minutes. Lowercase comparison so `MO`, `Mo`, `mO` all hit.
    let lower = trimmed.to_ascii_lowercase();
    if lower.ends_with("mo") {
        return Err(format!(
            "time-window value {trimmed:?} uses 'mo' suffix; rejected to avoid \
             month/minute ambiguity given chanvoy's existing minutes-default. \
             Months are not supported today; use s/m/h/d for sub-month windows."
        ));
    }
    let last = trimmed
        .chars()
        .last()
        .expect("trimmed is non-empty per check above");
    let (num_str, multiplier_secs): (&str, u64) = if last.is_ascii_digit() {
        let multiplier = match default_unit {
            TimeWindowDefaultUnit::Minutes => 60,
            TimeWindowDefaultUnit::Seconds => 1,
        };
        (trimmed, multiplier)
    } else if last == 'M' {
        return Err(format!(
            "time-window value {trimmed:?} uses uppercase 'M' suffix; rejected \
             to avoid month/minute ambiguity. Use lowercase 'm' for minutes."
        ));
    } else {
        let suffix_start = trimmed.len() - last.len_utf8();
        let suffix = &trimmed[suffix_start..];
        let multiplier = match suffix {
            "s" => 1u64,
            "m" => 60,
            "h" => 3600,
            "d" => 86400,
            other => {
                return Err(format!(
                    "time-window value {trimmed:?} has unknown suffix {other:?}; \
                     {TIME_WINDOW_SUFFIX_HELP}"
                ));
            }
        };
        (&trimmed[..suffix_start], multiplier)
    };
    let n: u64 = num_str.parse().map_err(|err| {
        format!("time-window value {trimmed:?} contains invalid integer {num_str:?}: {err}")
    })?;
    n.checked_mul(multiplier_secs).ok_or_else(|| {
        format!(
            "time-window value {trimmed:?} overflows u64 seconds (max ≈ \
             5.84e11 years; pick a saner window)"
        )
    })
}

#[cfg(test)]
mod time_window_tests {
    use super::*;

    #[test]
    fn bare_integer_minutes_default() {
        assert_eq!(
            parse_time_window("30", TimeWindowDefaultUnit::Minutes).unwrap(),
            30 * 60
        );
        assert_eq!(
            parse_time_window("0", TimeWindowDefaultUnit::Minutes).unwrap(),
            0
        );
    }

    #[test]
    fn bare_integer_seconds_default() {
        assert_eq!(
            parse_time_window("30", TimeWindowDefaultUnit::Seconds).unwrap(),
            30
        );
    }

    #[test]
    fn suffix_seconds() {
        assert_eq!(
            parse_time_window("30s", TimeWindowDefaultUnit::Minutes).unwrap(),
            30
        );
    }

    #[test]
    fn suffix_minutes_lowercase() {
        assert_eq!(
            parse_time_window("5m", TimeWindowDefaultUnit::Minutes).unwrap(),
            5 * 60
        );
    }

    #[test]
    fn suffix_hours() {
        assert_eq!(
            parse_time_window("4h", TimeWindowDefaultUnit::Minutes).unwrap(),
            4 * 3600
        );
    }

    #[test]
    fn suffix_days() {
        assert_eq!(
            parse_time_window("2d", TimeWindowDefaultUnit::Minutes).unwrap(),
            2 * 86400
        );
    }

    #[test]
    fn uppercase_m_rejected() {
        let err = parse_time_window("30M", TimeWindowDefaultUnit::Minutes).unwrap_err();
        assert!(
            err.contains("uppercase 'M'"),
            "diagnostic should name the ambiguity, got: {err}"
        );
    }

    #[test]
    fn mo_lowercase_rejected() {
        let err = parse_time_window("3mo", TimeWindowDefaultUnit::Minutes).unwrap_err();
        assert!(
            err.contains("'mo'"),
            "diagnostic should name 'mo' suffix, got: {err}"
        );
    }

    #[test]
    fn mo_uppercase_rejected() {
        let err = parse_time_window("3MO", TimeWindowDefaultUnit::Minutes).unwrap_err();
        assert!(err.to_ascii_lowercase().contains("'mo'"));
    }

    #[test]
    fn mo_mixed_case_rejected() {
        for variant in ["3Mo", "3mO"] {
            let err = parse_time_window(variant, TimeWindowDefaultUnit::Minutes).unwrap_err();
            assert!(
                err.to_ascii_lowercase().contains("'mo'"),
                "variant {variant} should reject as months suffix; got: {err}"
            );
        }
    }

    #[test]
    fn empty_rejected() {
        let err = parse_time_window("", TimeWindowDefaultUnit::Minutes).unwrap_err();
        assert!(err.contains("empty"));
    }

    #[test]
    fn unknown_suffix_rejected() {
        let err = parse_time_window("5w", TimeWindowDefaultUnit::Minutes).unwrap_err();
        assert!(err.contains("unknown suffix"), "got: {err}");
    }

    #[test]
    fn whitespace_trimmed() {
        assert_eq!(
            parse_time_window("  5m  ", TimeWindowDefaultUnit::Minutes).unwrap(),
            5 * 60
        );
    }

    #[test]
    fn invalid_integer_with_known_suffix_rejected() {
        let err = parse_time_window("foo5m", TimeWindowDefaultUnit::Minutes).unwrap_err();
        assert!(
            err.contains("invalid integer"),
            "diagnostic should name invalid integer; got: {err}"
        );
    }

    #[test]
    fn purely_alphabetic_rejected() {
        // "foo" has no digits and 'o' is an unknown suffix; either
        // diagnostic is acceptable so long as we loud-fail.
        let err = parse_time_window("foo", TimeWindowDefaultUnit::Minutes).unwrap_err();
        assert!(
            err.contains("unknown suffix") || err.contains("invalid integer"),
            "diagnostic should loud-fail on non-numeric input; got: {err}"
        );
    }

    #[test]
    fn overflow_rejected() {
        let err =
            parse_time_window("99999999999999999999d", TimeWindowDefaultUnit::Minutes).unwrap_err();
        assert!(err.contains("invalid integer") || err.contains("overflow"));
    }
}

/// PER-019: cross-team channel resolution outcome.
///
/// Internal callers that need the team-id (e.g., for cursor metadata,
/// post-write team binding, attention display) consume `ResolvedChannel`
/// directly; callers that only need the bare channel-id (legacy compat)
/// use `MattermostClient::channel_id_for_name`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ResolvedChannel {
    pub channel_id: String,
    pub channel_name: String,
    pub team_id: String,
    pub team_name: String,
    pub resolution_source: ResolutionSource,
}

/// PER-019: which path the γ hybrid resolver matched on. Surfaced in
/// diagnostics + operator-guide `[fallback]` provenance notation so
/// operators can tell at a glance whether a name resolved via the
/// primary team, a fallback team, or an explicit override.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ResolutionSource {
    /// Resolved via the profile's primary team — common case, no extra
    /// API call beyond the by-name lookup.
    Primary,
    /// Resolved via a non-primary team the bot is a member of (γ hybrid
    /// step 2). Operator-visible diagnostic prefixes this with `[fallback]`.
    Fallback,
    /// Resolved via explicit `<team>/<channel>` syntax or `--team` flag
    /// override. Always wins over the primary/fallback chain.
    Explicit,
}

/// PER-019: bot's view of a Mattermost team. Cached inside the client
/// for cross-team resolution. Identity-bounded to teams the bot is a
/// member of (`/users/me/teams` endpoint).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TeamInfo {
    pub id: String,
    pub name: String,
    pub display_name: String,
}

/// PER-019 AC #11: per-team channel grouping for the cross-team
/// `chanvoy channels` listing. Keeps the JSON contract structured per
/// team so consumers can scope without parsing the human format.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TeamChannels {
    pub team_id: String,
    pub team_name: String,
    pub team_display_name: String,
    pub channels: Vec<Channel>,
}

/// PER-019: TTL for the bot's team-membership cache. 15 minutes per the
/// brief — generous enough to amortize the API call across most
/// operator workflows, short enough that a dispatch-initiated
/// membership add becomes visible without a manual refresh. The
/// resolver also force-refreshes on no-match before failing, so a
/// newly-added team membership is self-healing on next use even if
/// the TTL hasn't elapsed.
pub const TEAM_LIST_TTL: Duration = Duration::from_secs(15 * 60);

#[derive(Debug, Clone)]
struct TeamCacheEntry {
    teams: Vec<TeamInfo>,
    fetched_at: std::time::Instant,
}

#[derive(Clone)]
pub struct MattermostClient {
    base_url: String,
    team_name: String,
    token: String,
    client: Client,
    /// PER-019: cached bot team membership. None until first fetch.
    /// Shared across clones so all daemon contexts see the same cache.
    team_cache: Arc<tokio::sync::RwLock<Option<TeamCacheEntry>>>,
}

impl MattermostClient {
    pub fn new(profile: &Profile, token: String) -> Result<Self, CoreError> {
        let client = Client::builder()
            .user_agent(concat!("chanvoy/", env!("CARGO_PKG_VERSION")))
            .build()?;
        Ok(Self {
            base_url: profile.server_url.trim_end_matches('/').to_string(),
            team_name: profile.team_name.clone(),
            token,
            client,
            team_cache: Arc::new(tokio::sync::RwLock::new(None)),
        })
    }

    /// Slug of the profile's primary team. PER-019 uses this as the
    /// first-try team in the γ hybrid resolution chain.
    pub fn primary_team_name(&self) -> &str {
        &self.team_name
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
        self.list_channels_for_team_id(&team_id).await
    }

    async fn list_channels_for_team_id(&self, team_id: &str) -> Result<Vec<Channel>, CoreError> {
        #[derive(Deserialize)]
        struct RawChannel {
            id: String,
            name: String,
            display_name: String,
            #[serde(rename = "type")]
            channel_type: String,
            // PER-025: MM returns `last_post_at` per channel. 0 = no
            // posts yet (or never); we map that to `None` so the
            // operator surface (`null` in JSON, `—` in human) is the
            // single source of truth for "no activity," distinct from
            // a real epoch timestamp.
            #[serde(default)]
            last_post_at: i64,
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
                last_post_at: if channel.last_post_at > 0 {
                    Some(channel.last_post_at)
                } else {
                    None
                },
            })
            .collect())
    }

    /// PER-019 AC #11: list channels across every team the bot is a
    /// member of. Returned grouped by team for the new default
    /// `chanvoy channels` output. The single-team `list_channels()` path
    /// is retained as the `--primary-team` back-compat view.
    pub async fn list_channels_across_teams(&self) -> Result<Vec<TeamChannels>, CoreError> {
        let teams = self.list_my_teams().await?;
        let mut out = Vec::with_capacity(teams.len());
        for team in teams {
            let channels = self.list_channels_for_team_id(&team.id).await?;
            out.push(TeamChannels {
                team_id: team.id,
                team_name: team.name,
                team_display_name: team.display_name,
                channels,
            });
        }
        // Stable order: primary team first, then alphabetical by slug.
        out.sort_by(|a, b| {
            let a_primary = a.team_name == self.team_name;
            let b_primary = b.team_name == self.team_name;
            b_primary
                .cmp(&a_primary)
                .then(a.team_name.cmp(&b.team_name))
        });
        Ok(out)
    }

    pub async fn read_channel(
        &self,
        channel_name: &str,
        since_minutes: u64,
        team: Option<&str>,
    ) -> Result<Vec<Message>, CoreError> {
        let channel_id = self.resolve_channel(channel_name, team).await?.channel_id;
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
        team: Option<&str>,
    ) -> Result<Vec<Message>, CoreError> {
        let channel_id = self.resolve_channel(channel_name, team).await?.channel_id;
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
        team: Option<&str>,
    ) -> Result<Vec<Message>, CoreError> {
        let my_username = self.whoami().await?.username;
        let after_post_id = self
            .latest_authored_post_id(channel_name, &my_username, team)
            .await?
            .ok_or_else(|| CoreError::NoPriorAuthoredPost {
                channel: channel_name.to_string(),
                username: my_username.clone(),
            })?;

        self.read_channel_after(channel_name, &after_post_id, team)
            .await
    }

    pub async fn post_message(
        &self,
        channel_name: &str,
        message: &str,
        team: Option<&str>,
    ) -> Result<PostReceipt, CoreError> {
        let channel_id = self.resolve_channel(channel_name, team).await?.channel_id;
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
        Ok(PostReceipt {
            id: receipt.id,
            parent_id: None,
        })
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
            None,
        )
        .await
    }

    pub async fn notifications(&self, since_minutes: u64) -> Result<Vec<Notification>, CoreError> {
        let my_username = self.whoami().await?.username;
        let messages = self
            .read_channel(DEFAULT_NOTIFICATIONS_CHANNEL, since_minutes, None)
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
                self.read_channel_after(DEFAULT_NOTIFICATIONS_CHANNEL, post_id, None)
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

    /// Create a public channel. The channel lands on the profile's
    /// primary team unless `team` is `Some(<slug>)`, in which case
    /// the team slug is resolved through the bot's
    /// `/users/me/teams` membership cache (with one force-refresh on
    /// no-match for self-healing on newly-added memberships) before
    /// the channel is created. Refuses with
    /// `CoreError::NotAMemberOfTeam { team, teams }` if the bot is
    /// not a member of the requested team — matching the PER-019 γ
    /// resolver's enforcement posture rather than letting MM's
    /// authorization layer reject the downstream call. Closes the
    /// v0.2.1 cross-team admin-verb gap.
    pub async fn create_channel(
        &self,
        name: &str,
        display_name: &str,
        purpose: Option<String>,
        team: Option<&str>,
    ) -> Result<Channel, CoreError> {
        let team_id = match team {
            // Devrev PR #23 finding #1: team override must enforce
            // membership before posting, mirroring the PER-019
            // resolver's `NotAMemberOfTeam` semantics that operators
            // expect from the rest of the cross-team verbs.
            Some(slug) => self.team_id_for_member_slug(slug).await?,
            None => self.team_id().await?,
        };
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
            // Freshly-created channels have no posts yet — last_post_at
            // surfaces as `None` (renders `null` / `—`).
            last_post_at: None,
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

    /// PER-023 primitive 3: read with second-resolution time window.
    /// Resolves the channel via the γ hybrid resolver (PER-019), then hits
    /// MM `/channels/{id}/posts?since={millis}` directly so suffixes like
    /// `30s` aren't lossily rounded up to a full minute.
    pub async fn read_channel_since_secs(
        &self,
        channel_name: &str,
        since_secs: u64,
        team: Option<&str>,
    ) -> Result<Vec<Message>, CoreError> {
        let channel_id = self.resolve_channel(channel_name, team).await?.channel_id;
        let since = seconds_ago_millis(since_secs);
        self.read_channel_by_id_since_millis(&channel_id, since)
            .await
    }

    /// PER-023 primitive 1: fetch the channel's pinned posts via MM
    /// `GET /api/v4/channels/{id}/pinned_posts`. Pure read, no cursor side
    /// effects (mirrors the operator-facing pinned-as-context contract).
    /// Resolves via the γ hybrid resolver (PER-019); accepts
    /// `<team>/<channel>` syntax and the `--team` override.
    pub async fn read_channel_pinned(
        &self,
        channel_name: &str,
        team: Option<&str>,
    ) -> Result<Vec<Message>, CoreError> {
        let channel_id = self.resolve_channel(channel_name, team).await?.channel_id;
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
            #[serde(default)]
            posts: BTreeMap<String, RawPost>,
        }
        let response: PostsResponse = self
            .request(
                "GET",
                &format!("/channels/{channel_id}/pinned_posts"),
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

    /// PER-023 primitive 2: bounded most-recent-N posts. Maps the
    /// `--since-bootstrap` operator surface onto MM
    /// `GET /channels/{id}/posts?per_page=N` (descending order). Resolves
    /// via γ hybrid; no cursor side effects unless the daemon RPC layer
    /// applies `--advance` after.
    pub async fn read_channel_most_recent(
        &self,
        channel_name: &str,
        limit: u32,
        team: Option<&str>,
    ) -> Result<Vec<Message>, CoreError> {
        let channel_id = self.resolve_channel(channel_name, team).await?.channel_id;
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
                &format!("/channels/{channel_id}/posts?per_page={limit}"),
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

    /// PER-023 primitive 4: fetch the channel's current latest post id
    /// without surfacing the content. Used by `chanvoy ack <channel>` to
    /// advance the attention cursor to the channel's current latest.
    /// Returns `None` for empty channels (channel exists, no posts yet).
    /// Delegates to `read_channel_most_recent` with `per_page=1` — MM's
    /// channel-meta endpoint returns `last_post_at` but not the post id,
    /// so the most-recent-post fetch is the single round-trip that gives
    /// us the id directly.
    pub async fn channel_last_post_id(
        &self,
        channel_name: &str,
        team: Option<&str>,
    ) -> Result<Option<String>, CoreError> {
        let recent = self.read_channel_most_recent(channel_name, 1, team).await?;
        Ok(recent.into_iter().next_back().map(|m| m.id))
    }

    /// PER-025 primitive 1: search posts in a channel via MM
    /// `POST /api/v4/teams/{team_id}/posts/search`. Composes the
    /// chanvoy-owned scopes (`<channel>` → `in:<channel-name>`,
    /// `--from <author>` → `from:<author>`, `--since <secs>` →
    /// `after:<computed-date>`) into MM's native operator syntax.
    /// Operator-conflict detection happens **before** this call —
    /// chanvoy-cli runs `check_search_operator_conflicts` against the
    /// raw query and refuses with a diagnostic before issuing the
    /// daemon RPC, so by the time we get here the query is conflict-
    /// free relative to the chanvoy-owned scopes.
    pub async fn search_channel(
        &self,
        channel_name: &str,
        query: &str,
        limit: u32,
        from: Option<&str>,
        since_secs: Option<u64>,
        team: Option<&str>,
    ) -> Result<SearchResult, CoreError> {
        let resolved = self.resolve_channel(channel_name, team).await?;
        // Compose chanvoy-owned scopes onto the operator's query.
        // chanvoy uses MM's native operator syntax (`in:`, `from:`,
        // `after:`) so MM does the actual search-side work; chanvoy
        // contributes the resolved-channel scope plus any flag
        // narrowing.
        let mut terms = format!("{query} in:{}", resolved.channel_name);
        if let Some(author) = from {
            terms.push_str(&format!(" from:{author}"));
        }
        if let Some(secs) = since_secs {
            // MM accepts `after:YYYY-MM-DD` (date-only granularity is
            // the documented surface; finer-grained windows aren't
            // supported by this MM operator). Compute the date by
            // subtracting `secs` from now and formatting in UTC.
            let cutoff_millis = seconds_ago_millis(secs);
            let date = chrono::DateTime::from_timestamp_millis(cutoff_millis)
                .map(|dt| dt.format("%Y-%m-%d").to_string())
                .unwrap_or_else(|| "1970-01-01".to_string());
            terms.push_str(&format!(" after:{date}"));
        }

        #[derive(Serialize)]
        struct Payload<'a> {
            terms: &'a str,
            is_or_search: bool,
        }
        #[derive(Deserialize)]
        struct RawPost {
            id: String,
            user_id: String,
            message: String,
            create_at: i64,
            #[serde(default)]
            username: Option<String>,
        }
        #[derive(Deserialize)]
        struct SearchResponse {
            #[serde(default)]
            order: Vec<String>,
            #[serde(default)]
            posts: BTreeMap<String, RawPost>,
        }

        let response: SearchResponse = self
            .request(
                "POST",
                &format!("/teams/{}/posts/search", resolved.team_id),
                Some(Payload {
                    terms: &terms,
                    is_or_search: false,
                }),
            )
            .await?;

        // MM returns `order` (post ids in ranked order) plus a `posts`
        // map keyed by id. Walk `order` to preserve MM's ranking,
        // then truncate to the operator's `--limit`.
        let limit = limit as usize;
        let mut posts = Vec::with_capacity(response.order.len().min(limit));
        for id in response.order.iter().take(limit) {
            if let Some(raw) = response.posts.get(id) {
                posts.push(Message {
                    id: raw.id.clone(),
                    user_id: raw.user_id.clone(),
                    username: raw
                        .username
                        .clone()
                        .unwrap_or_else(|| "unknown".to_string()),
                    message: raw.message.clone(),
                    create_at: raw.create_at,
                });
            }
        }

        Ok(SearchResult {
            team: resolved.team_name,
            channel: resolved.channel_name,
            posts,
        })
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
        Ok(PostReceipt {
            id: receipt.id,
            parent_id: None,
        })
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
        // PER-024 AC #3a: threaded receipts surface `parent_id`
        // additively. Non-threaded paths (`post_message_by_id` /
        // `post_message`) leave it `None` so the JSON shape
        // collapses to `{ id }`.
        Ok(PostReceipt {
            id: receipt.id,
            parent_id: Some(root_id.to_string()),
        })
    }

    /// PER-024 primitive 1: high-level wrapper that resolves the
    /// channel via PER-019 γ hybrid, verifies the parent post exists
    /// on the **resolved** channel, then issues the threaded write.
    /// Validation order is provider-portable per AC #3.
    pub async fn post_threaded_reply_in_channel(
        &self,
        channel_name: &str,
        root_post_id: &str,
        message: &str,
        team: Option<&str>,
    ) -> Result<PostReceipt, CoreError> {
        let resolved = self.resolve_channel(channel_name, team).await?;
        self.assert_post_in_channel(&resolved.channel_id, &resolved.channel_name, root_post_id)
            .await?;
        self.post_threaded_reply(&resolved.channel_id, root_post_id, message)
            .await
    }

    /// PER-024 primitive 2: add an emoji reaction under the bot's
    /// identity. Validation order per AC #5a: resolve channel →
    /// verify post exists on resolved channel → write. Idempotent on
    /// duplicate-react per AC #5b: MM returns 200/201 with the
    /// existing reaction object on duplicates, so we accept any 2xx
    /// as success. Per @agent-bravo-devrev's PR #20 pre-impl pin #3
    /// (2026-05-04) — idempotency is deliberate at the chanvoy-core
    /// layer, not assumed from generic MM error behavior.
    pub async fn add_reaction(
        &self,
        channel_name: &str,
        post_id: &str,
        emoji: &str,
        team: Option<&str>,
    ) -> Result<ReactionResult, CoreError> {
        let resolved = self.resolve_channel(channel_name, team).await?;
        self.assert_post_in_channel(&resolved.channel_id, &resolved.channel_name, post_id)
            .await?;
        let user_id = self.whoami().await?.id;
        let normalized = normalize_emoji_name(emoji);
        #[derive(Serialize)]
        struct Payload<'a> {
            user_id: &'a str,
            post_id: &'a str,
            emoji_name: &'a str,
        }
        // MM returns the reaction object on success; we discard the
        // body via `Value`. The 2xx vs error distinction is the
        // operator-facing contract.
        let _: Value = self
            .request(
                "POST",
                "/reactions",
                Some(Payload {
                    user_id: &user_id,
                    post_id,
                    emoji_name: &normalized,
                }),
            )
            .await?;
        Ok(ReactionResult {
            team: resolved.team_name,
            channel: resolved.channel_name,
            post_id: post_id.to_string(),
            emoji: normalized,
            ok: true,
        })
    }

    /// PER-024 primitive 2: remove the bot's reaction. Validation
    /// order per AC #5a; idempotent on missing-reaction per AC #5b
    /// (404 normalized to success at this layer per devrev pre-impl
    /// pin #3 — the operator contract is "this reaction does not
    /// exist after this call returns," and a 404 from MM means it
    /// already didn't exist).
    pub async fn remove_reaction(
        &self,
        channel_name: &str,
        post_id: &str,
        emoji: &str,
        team: Option<&str>,
    ) -> Result<ReactionResult, CoreError> {
        let resolved = self.resolve_channel(channel_name, team).await?;
        self.assert_post_in_channel(&resolved.channel_id, &resolved.channel_name, post_id)
            .await?;
        let user_id = self.whoami().await?.id;
        let normalized = normalize_emoji_name(emoji);
        let path = format!("/users/{user_id}/posts/{post_id}/reactions/{normalized}");
        match self.request::<Value, Value>("DELETE", &path, None).await {
            Ok(_) => {}
            Err(CoreError::Api {
                status: StatusCode::NOT_FOUND,
                ..
            }) => {}
            Err(other) => return Err(other),
        }
        Ok(ReactionResult {
            team: resolved.team_name,
            channel: resolved.channel_name,
            post_id: post_id.to_string(),
            emoji: normalized,
            ok: true,
        })
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
        self.team_id_for_slug(&self.team_name).await
    }

    /// Resolve a team slug to a team-id. Used by the γ hybrid resolver for
    /// both the primary team and any explicit `<team>/<channel>` override.
    async fn team_id_for_slug(&self, slug: &str) -> Result<String, CoreError> {
        #[derive(Deserialize)]
        struct TeamResponse {
            id: String,
        }
        let team: TeamResponse = self
            .request("GET", &format!("/teams/name/{slug}"), None::<Value>)
            .await?;
        Ok(team.id)
    }

    /// Resolve a team slug through the bot's `/users/me/teams`
    /// membership cache (with one force-refresh on no-match for
    /// self-healing on newly-added memberships) before returning
    /// its id. Mirrors the membership check the PER-019 γ resolver
    /// runs in `resolve_in_team` — refuses with
    /// `CoreError::NotAMemberOfTeam { team, teams }` rather than
    /// silently letting MM's authorization layer reject the
    /// downstream call. Used by `create_channel`'s `--team` override
    /// so chanvoy enforces the "must be a team the bot is a member
    /// of" contract operator-side per devrev PR #23 finding #1.
    async fn team_id_for_member_slug(&self, slug: &str) -> Result<String, CoreError> {
        let teams = self.list_my_teams().await?;
        if let Some(team) = teams.iter().find(|t| t.name == slug) {
            return Ok(team.id.clone());
        }
        // No-match → force-refresh once before failing, matching
        // resolve_in_team's self-healing posture for newly-added
        // memberships (devrev's PR #40 pin on the resolver path).
        self.refresh_team_list().await?;
        let refreshed = self.list_my_teams().await?;
        match refreshed.iter().find(|t| t.name == slug) {
            Some(team) => Ok(team.id.clone()),
            None => Err(CoreError::NotAMemberOfTeam {
                team: slug.to_string(),
                teams: refreshed.into_iter().map(|t| t.name).collect(),
            }),
        }
    }

    /// PER-019: list the teams the bot is a member of. Identity-bounded —
    /// `/users/me/teams` returns only what the token already has access
    /// to. Cached with a 15-minute TTL; the resolver also force-refreshes
    /// on no-match before failing so newly-added memberships self-heal
    /// without operator action.
    pub async fn list_my_teams(&self) -> Result<Vec<TeamInfo>, CoreError> {
        if let Some(cached) = self.read_cached_teams().await {
            return Ok(cached);
        }
        self.refresh_team_list().await
    }

    async fn read_cached_teams(&self) -> Option<Vec<TeamInfo>> {
        let guard = self.team_cache.read().await;
        match guard.as_ref() {
            Some(entry) if entry.fetched_at.elapsed() < TEAM_LIST_TTL => Some(entry.teams.clone()),
            _ => None,
        }
    }

    /// Force-refresh the team-list cache. Called at the start of each
    /// resolver attempt when the cache is stale, and once more on a
    /// no-match outcome before failing (self-healing for newly-added
    /// memberships).
    pub async fn refresh_team_list(&self) -> Result<Vec<TeamInfo>, CoreError> {
        #[derive(Deserialize)]
        struct RawTeam {
            id: String,
            name: String,
            display_name: String,
        }
        let raw: Vec<RawTeam> = self
            .request("GET", "/users/me/teams", None::<Value>)
            .await?;
        let teams: Vec<TeamInfo> = raw
            .into_iter()
            .map(|t| TeamInfo {
                id: t.id,
                name: t.name,
                display_name: t.display_name,
            })
            .collect();
        let mut guard = self.team_cache.write().await;
        *guard = Some(TeamCacheEntry {
            teams: teams.clone(),
            fetched_at: std::time::Instant::now(),
        });
        Ok(teams)
    }

    /// PER-019 γ hybrid resolver. Operator hands in a channel argument
    /// (possibly `<team>/<channel>` syntax) and an optional `--team`
    /// override. Resolution chain:
    ///
    /// - Explicit `<team>/<channel>` or `--team` override → that team only
    /// - Primary team first → return on hit
    /// - Fallback to other member teams → return single hit; refuse on
    ///   ambiguity; refresh cache once and retry on no-match before
    ///   failing
    ///
    /// Returns a `ResolvedChannel` carrying both ids + slugs and a
    /// `ResolutionSource` for diagnostic provenance. Specific error
    /// variants distinguish no-match / not-a-member / ambiguity per
    /// secrev's pin so operators see the right next-step flag.
    pub async fn resolve_channel(
        &self,
        channel_arg: &str,
        team_override: Option<&str>,
    ) -> Result<ResolvedChannel, CoreError> {
        let trimmed = channel_arg.trim_start_matches('#');

        // Explicit `<team>/<channel>` syntax wins over both the primary
        // chain and the --team flag (operator typed it specifically).
        // Per devrev's PR #17 finding #4: also strip a leading `#` from
        // the channel segment so `<team>/#<channel>` works identically
        // to `<team>/<channel>` (operators routinely include the # when
        // pasting from the Mattermost UI).
        if let Some((team_slug, channel_name)) = trimmed.split_once('/') {
            return self
                .resolve_in_team(
                    channel_name.trim_start_matches('#'),
                    team_slug,
                    ResolutionSource::Explicit,
                )
                .await;
        }

        // --team flag wins over the primary/fallback chain.
        if let Some(team_slug) = team_override {
            return self
                .resolve_in_team(trimmed, team_slug, ResolutionSource::Explicit)
                .await;
        }

        // Default chain: primary team first.
        if let Some(resolved) = self
            .try_channel_in_team(trimmed, &self.team_name, ResolutionSource::Primary)
            .await?
        {
            return Ok(resolved);
        }

        // Fallback: search across other member teams. Use the cache, then
        // force one refresh on no-match (self-healing for newly-added
        // memberships per devrev's PR #40 pin).
        match self.fallback_search(trimmed).await? {
            Some(resolved) => Ok(resolved),
            None => {
                self.refresh_team_list().await?;
                match self.fallback_search(trimmed).await? {
                    Some(resolved) => Ok(resolved),
                    None => {
                        let teams = self
                            .list_my_teams()
                            .await?
                            .into_iter()
                            .map(|t| t.name)
                            .collect();
                        Err(CoreError::ChannelNotFoundInAnyTeam {
                            channel: trimmed.to_string(),
                            teams,
                        })
                    }
                }
            }
        }
    }

    /// Search every non-primary team for the channel name. Returns
    /// `Some(resolved)` on a unique hit, refuses with `AmbiguousChannel`
    /// on multiple hits, returns `None` if no team has it (caller decides
    /// whether to refresh and retry or fail).
    async fn fallback_search(
        &self,
        channel_name: &str,
    ) -> Result<Option<ResolvedChannel>, CoreError> {
        let teams = self.list_my_teams().await?;
        let mut matches: Vec<ResolvedChannel> = Vec::new();
        for team in &teams {
            if team.name == self.team_name {
                continue;
            }
            if let Some(resolved) = self
                .try_channel_in_team_by_id(
                    channel_name,
                    &team.id,
                    &team.name,
                    ResolutionSource::Fallback,
                )
                .await?
            {
                matches.push(resolved);
            }
        }
        match matches.len() {
            0 => Ok(None),
            1 => Ok(matches.into_iter().next()),
            _ => Err(CoreError::AmbiguousChannel {
                channel: channel_name.to_string(),
                teams: matches.into_iter().map(|m| m.team_name).collect(),
            }),
        }
    }

    /// Resolve `channel_name` strictly within `team_slug`. Used by both
    /// `<team>/<channel>` and `--team` paths. Distinguishes "team not in
    /// my membership" from "channel not in that team" per secrev's pin.
    async fn resolve_in_team(
        &self,
        channel_name: &str,
        team_slug: &str,
        source: ResolutionSource,
    ) -> Result<ResolvedChannel, CoreError> {
        let teams = self.list_my_teams().await?;
        let team = match teams.iter().find(|t| t.name == team_slug) {
            Some(t) => t.clone(),
            None => {
                self.refresh_team_list().await?;
                let refreshed = self.list_my_teams().await?;
                match refreshed.iter().find(|t| t.name == team_slug) {
                    Some(t) => t.clone(),
                    None => {
                        return Err(CoreError::NotAMemberOfTeam {
                            team: team_slug.to_string(),
                            teams: refreshed.into_iter().map(|t| t.name).collect(),
                        });
                    }
                }
            }
        };
        match self
            .try_channel_in_team_by_id(channel_name, &team.id, &team.name, source)
            .await?
        {
            Some(resolved) => Ok(resolved),
            None => Err(CoreError::ChannelNotFoundInAnyTeam {
                channel: channel_name.to_string(),
                teams: vec![team.name],
            }),
        }
    }

    /// Try the by-name lookup against a specific team. Returns
    /// `Some(resolved)` on a 200, `None` on a 404 (caller continues the
    /// chain or decides to fail), bubbles other errors.
    async fn try_channel_in_team(
        &self,
        channel_name: &str,
        team_slug: &str,
        source: ResolutionSource,
    ) -> Result<Option<ResolvedChannel>, CoreError> {
        let team_id = self.team_id_for_slug(team_slug).await?;
        self.try_channel_in_team_by_id(channel_name, &team_id, team_slug, source)
            .await
    }

    async fn try_channel_in_team_by_id(
        &self,
        channel_name: &str,
        team_id: &str,
        team_slug: &str,
        source: ResolutionSource,
    ) -> Result<Option<ResolvedChannel>, CoreError> {
        #[derive(Deserialize)]
        struct ChannelResponse {
            id: String,
            name: String,
        }
        let result: Result<ChannelResponse, CoreError> = self
            .request(
                "GET",
                &format!("/teams/{team_id}/channels/name/{channel_name}"),
                None::<Value>,
            )
            .await;
        match result {
            Ok(channel) => Ok(Some(ResolvedChannel {
                channel_id: channel.id,
                channel_name: channel.name,
                team_id: team_id.to_string(),
                team_name: team_slug.to_string(),
                resolution_source: source,
            })),
            Err(CoreError::Api { status, .. }) if status == reqwest::StatusCode::NOT_FOUND => {
                Ok(None)
            }
            Err(err) => Err(err),
        }
    }

    /// Compatibility wrapper preserving the pre-PER-019 public API that
    /// returns just the channel-id. Internal callers should prefer
    /// `resolve_channel` directly so they get the team metadata too.
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

    /// PER-019 (secrev pin, PR #40 review): the post-search endpoint is
    /// team-scoped via `/teams/{team_id}/posts/search`. Pre-PER-019 this
    /// resolved `team_id` from the profile's primary team only, so a
    /// caller asking for `read --since-last-mine` against a non-primary-
    /// team channel would search the wrong team and miss the prior post.
    /// Now routes through `resolve_channel` so the search uses the
    /// channel's actual team-id.
    async fn latest_authored_post_id(
        &self,
        channel_name: &str,
        username: &str,
        team: Option<&str>,
    ) -> Result<Option<String>, CoreError> {
        let resolved = self.resolve_channel(channel_name, team).await?;

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
                &format!("/teams/{}/posts/search", resolved.team_id),
                Some(SearchPayload {
                    terms: format!("from:{username} in:{}", resolved.channel_name),
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

    /// PER-019: now routes through the γ hybrid resolver so internal
    /// callers automatically inherit cross-team behavior. Public name
    /// preserved; behavior changed from "single primary-team lookup" to
    /// "primary first, fallback across member teams". The compatibility
    /// wrapper `channel_id_for_name` keeps the bare-id return shape for
    /// any external consumer.
    async fn channel_id(&self, channel_name: &str) -> Result<String, CoreError> {
        let resolved = self.resolve_channel(channel_name, None).await?;
        Ok(resolved.channel_id)
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
    pub catchup_in_flight: Arc<AtomicBool>,
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
            catchup_in_flight: Arc::new(AtomicBool::new(false)),
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
            // Wait for this cycle's catchup to finish before clearing.
            // Otherwise a slow catchup can set `suspected_gap=true`
            // *after* the grace task has already run, leaving the
            // daemon stuck in recovering until a later reconnect.
            // PER-010, secrev follow-up.
            let wait_start = tokio::time::Instant::now();
            while ws.catchup_in_flight.load(Ordering::Relaxed) {
                if wait_start.elapsed() >= Duration::from_secs(60) {
                    // Pathologically slow catchup — don't clear.
                    // `suspected_gap` stays at whatever catchup last
                    // set it to; a later reconnect will resolve.
                    return;
                }
                sleep(Duration::from_millis(200)).await;
            }
            // Re-check target after the wait in case a new reconnect
            // restamped it while we were waiting on catchup.
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
        // Mark this cycle's catchup as in-flight so the grace-window
        // task (armed before we entered this function) will wait for
        // us to finish before clearing suspected_gap. Otherwise a slow
        // catchup — many monitored channels, slow REST — can leave the
        // grace task firing early, clearing a gap that hadn't been
        // flagged yet, then catchup sets the gap post-hoc with no
        // further clear. PER-010, secrev follow-up.
        self.ws_state
            .catchup_in_flight
            .store(true, Ordering::Relaxed);
        // Each reconnect cycle starts with a clean slate. If this cycle's
        // outage exceeded the 5-min window and emits a Gap below, we'll
        // re-flag suspected_gap; the arm_recovery_window task clears it
        // at grace-window completion (after we finish).
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
        self.ws_state
            .catchup_in_flight
            .store(false, Ordering::Relaxed);
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
                "org-lanytehq/per-008".to_string(),
                ChannelCursorState {
                    last_seen_post_id: Some("post-123".to_string()),
                    updated_at: Some(1_776_000_000_000),
                    last_known_stale: false,
                    last_checked_at: None,
                    channel_id: "ch-008".to_string(),
                    team_id: "team-lanytehq".to_string(),
                    team_name: "org-lanytehq".to_string(),
                    channel_name: "per-008".to_string(),
                },
            )]),
            mentions: MentionCursorState {
                last_seen_post_id: Some("mention-456".to_string()),
                updated_at: Some(1_776_000_000_001),
            },
            quarantined: Vec::new(),
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

        #[tokio::test]
        async fn compute_seed_outcomes_skips_qualified_key_after_per019_migration() {
            // entarch PR #17 P2: post-PER-019 the daemon passes
            // qualified `<team>/<channel>` cursor keys into
            // compute_seed_outcomes. Pre-fix the helper compared bare
            // names against the qualified set, so already-cursored
            // primary-team channels were no longer skipped at
            // enumeration. With the fix, qualifying the enumerated
            // name with the primary team before checking matches.
            let server = MockServer::start().await;
            mock_whoami(&server, 200).await;
            mock_team(&server, "org-lanytehq", 200).await;

            Mock::given(method("GET"))
                .and(path("/api/v4/users/me/teams/team-id-456/channels"))
                .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!([
                    {"id": "ch-existing", "name": "bravo-team", "display_name": "Bravo", "type": "O"},
                    {"id": "ch-new", "name": "per-019", "display_name": "PER-019", "type": "O"},
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
            // No mock for ch-existing/posts — if the helper fetched
            // it, wiremock returns 404 → outcomes contain Failed.
            // The fix ensures it skips at enumeration instead.

            let client =
                MattermostClient::new(&test_profile(&server.uri()), "token".into()).unwrap();
            // Daemon now passes qualified keys (post-migration shape).
            let mut existing = std::collections::BTreeSet::new();
            existing.insert("org-lanytehq/bravo-team".to_string());
            let outcomes = compute_seed_outcomes(&client, &existing).await.unwrap();

            let channels: Vec<_> = outcomes
                .iter()
                .map(|o| match o {
                    SeededChannelOutcome::Seeded { channel, .. }
                    | SeededChannelOutcome::UnseededEmptyChannel { channel }
                    | SeededChannelOutcome::Failed { channel, .. } => channel.as_str(),
                })
                .collect();
            assert_eq!(
                channels,
                vec!["per-019"],
                "qualified-key cursor must skip the seed enumeration; bare-name match would falsely emit Failed"
            );
            // Belt-and-suspenders: verify nothing emitted for the
            // existing channel (no Failed, no Seeded, no Empty).
            assert!(
                !outcomes.iter().any(|o| match o {
                    SeededChannelOutcome::Seeded { channel, .. }
                    | SeededChannelOutcome::UnseededEmptyChannel { channel }
                    | SeededChannelOutcome::Failed { channel, .. } => channel == "bravo-team",
                }),
                "already-cursored channel must not appear in outcomes"
            );
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
            // PER-014: matched probe → drift=Some(false) (probe ran cleanly,
            // no drift). Distinguishes from None (probe didn't run / failed).
            assert_eq!(status.mattermost_identity_drift, Some(false));
        }

        #[test]
        fn build_status_marks_identity_drift_when_whoami_returns_other_user() {
            // PER-014 drift floor: the post-bind probe (or any later
            // daemon_status call) returned a username that does NOT match
            // the configured bot_username. The status must surface this so
            // operators can see what's wrong, and the dispatcher can refuse
            // network-backed RPCs while keeping the local socket bound.
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
                // Token now authenticates as a different bot — drift.
                Ok("agent-impersonator".to_string()),
                ws,
                ipc_absent(),
                now,
            );
            assert!(status.mattermost_ok);
            assert_eq!(status.mattermost_username, "agent-impersonator");
            assert_eq!(
                status.mattermost_identity_drift,
                Some(true),
                "drift must be surfaced when whoami returns a different username"
            );
        }

        #[test]
        fn build_status_drift_is_none_when_probe_failed() {
            // When the probe itself failed (network blocked, sandbox,
            // transient outage), drift cannot be determined — surface
            // None rather than a misleading false. Operators see
            // mattermost_ok=false + mattermost_last_error=Some, which
            // is the right signal in this case.
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
                Err("probe timed out".to_string()),
                ws,
                ipc_absent(),
                now,
            );
            assert!(!status.mattermost_ok);
            assert_eq!(status.mattermost_identity_drift, None);
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
        async fn gap_detected_after_grace_still_returns_to_healthy() {
            // secrev follow-up: on a slow catchup path, the grace-task
            // can fire before reconnect_catchup sets suspected_gap. If
            // the task cleared unconditionally at that moment, the
            // post-hoc Gap from catchup would leave the daemon stuck
            // in recovering / suspected_gap=true until a *later*
            // reconnect cycle — defeating the fix.
            //
            // This test simulates a catchup still in flight when the
            // grace window expires: the task must wait for catchup to
            // finish before deciding whether to clear.
            let ws = Arc::new(WsState::new());
            ws.set_state(WsConnectionState::Disconnected).await;
            ws.set_state(WsConnectionState::Healthy).await;
            // Mark catchup as in-flight BEFORE arming the grace task,
            // mirroring the real flow (arm -> await catchup; catchup
            // raises the flag at entry).
            ws.catchup_in_flight.store(true, Ordering::Relaxed);
            ws.arm_recovery_window();

            // Let the grace window fully elapse. The task must be
            // parked on the catchup-in-flight wait, not already done.
            tokio::time::sleep(Duration::from_millis((RECOVERY_GRACE_MS as u64) + 300)).await;
            assert_eq!(
                ws.last_recovered_at.load(Ordering::Relaxed),
                0,
                "task must wait for catchup to complete, not clear early"
            );

            // Now simulate catchup finally completing — and emitting a
            // gap post-expiry. This is the worst case secrev described.
            ws.suspected_gap.store(true, Ordering::Relaxed);
            ws.catchup_in_flight.store(false, Ordering::Relaxed);

            // Give the task's 200ms poll a couple of cycles to pick up
            // the flag change and apply the clear.
            tokio::time::sleep(Duration::from_millis(500)).await;

            assert!(
                !ws.suspected_gap.load(Ordering::Relaxed),
                "suspected_gap must clear once catchup completes, even post-grace"
            );
            assert!(
                ws.last_recovered_at.load(Ordering::Relaxed) > 0,
                "last_recovered_at must be stamped after catchup completes"
            );
            let now = now_unix_millis();
            let (conn, gap, ru) = snapshot(&ws);
            assert_eq!(
                derive_daemon_health(now, Some(conn), gap, ru),
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

    mod resolver {
        use super::*;

        fn inputs<'a>(
            profiles: &'a [String],
            running: &'a [String],
            active: Option<&'a str>,
            role: Option<&'a str>,
            scope: Option<&'a str>,
            chanvoy_profile: Option<&'a str>,
        ) -> ResolverInputs<'a> {
            ResolverInputs {
                profiles,
                running_daemon_profiles: running,
                active_profile: active,
                env_role: role,
                env_scope: scope,
                env_chanvoy_profile: chanvoy_profile,
            }
        }

        fn names(items: &[&str]) -> Vec<String> {
            items.iter().map(|s| s.to_string()).collect()
        }

        // -- Rule 1: explicit --profile flag wins, unconditional. ----------

        #[test]
        fn explicit_flag_wins_even_when_env_disagrees() {
            let profiles = names(&["bravo-devlead-lanytehq", "cxotech-lanytehq"]);
            let inputs = inputs(
                &profiles,
                &[],
                Some("cxotech-lanytehq"),
                Some("bravo-devlead"),
                Some("lanytehq"),
                None,
            );
            let resolved = resolve_profile_name(
                Some("cxotech-lanytehq"),
                FallbackPolicy::ExplicitOnly,
                &inputs,
            )
            .unwrap();
            assert_eq!(resolved, "cxotech-lanytehq");
        }

        #[test]
        fn explicit_flag_is_not_validated_against_profile_list() {
            // The flag is the operator's stated intent. If it points
            // at a profile that doesn't exist, downstream daemon-RPC
            // errors will surface that — the resolver does not
            // second-guess explicit operator input.
            let inputs = inputs(&[], &[], None, None, None, None);
            let resolved = resolve_profile_name(
                Some("typo-profile"),
                FallbackPolicy::AllowReadFallbacks,
                &inputs,
            )
            .unwrap();
            assert_eq!(resolved, "typo-profile");
        }

        // -- Rule 2: CHANVOY_PROFILE env, must exist (devrev pin). ---------

        #[test]
        fn chanvoy_profile_env_resolves_when_profile_exists() {
            let profiles = names(&["bravo-devlead-lanytehq", "cxotech-enacthq"]);
            let inputs = inputs(&profiles, &[], None, None, None, Some("cxotech-enacthq"));
            let resolved =
                resolve_profile_name(None, FallbackPolicy::AllowReadFallbacks, &inputs).unwrap();
            assert_eq!(resolved, "cxotech-enacthq");
        }

        #[test]
        fn chanvoy_profile_env_refuses_when_profile_missing() {
            // devrev pin: do NOT fall through to env-derived exact /
            // single-daemon / active_profile when CHANVOY_PROFILE is
            // set to a non-existent name. Refuse with the live list.
            let profiles = names(&["bravo-devlead-lanytehq"]);
            let running = names(&["bravo-devlead-lanytehq"]);
            let inputs = inputs(
                &profiles,
                &running,
                Some("bravo-devlead-lanytehq"),
                Some("bravo-devlead"),
                Some("lanytehq"),
                Some("nonexistent-profile"),
            );
            let err = resolve_profile_name(None, FallbackPolicy::AllowReadFallbacks, &inputs)
                .unwrap_err();
            match err {
                ResolverError::EnvProfileNotFound { name, .. } => {
                    assert_eq!(name, "nonexistent-profile");
                }
                other => panic!("expected EnvProfileNotFound, got {other:?}"),
            }
        }

        // -- Rule 3: ${ROLE}-${SCOPE} exact-name. --------------------------

        #[test]
        fn env_exact_name_wins_when_profile_exists() {
            let profiles = names(&["bravo-devlead-lanytehq", "cxotech-lanytehq"]);
            let inputs = inputs(
                &profiles,
                &[],
                None,
                Some("bravo-devlead"),
                Some("lanytehq"),
                None,
            );
            let resolved =
                resolve_profile_name(None, FallbackPolicy::AllowReadFallbacks, &inputs).unwrap();
            assert_eq!(resolved, "bravo-devlead-lanytehq");
        }

        #[test]
        fn env_exact_name_wins_over_sibling_profiles_sharing_role_scope() {
            // The PER-010 trace: sibling profiles `*-bootstrap` and
            // `*-custom-team` exist alongside the canonical name. The
            // old resolver bailed on this ambiguity; the new resolver
            // resolves to the exact canonical name and ignores the
            // siblings.
            let profiles = names(&[
                "bravo-devlead-lanytehq",
                "bravo-devlead-bootstrap",
                "bravo-devlead-custom-team",
            ]);
            let inputs = inputs(
                &profiles,
                &[],
                None,
                Some("bravo-devlead"),
                Some("lanytehq"),
                None,
            );
            let resolved =
                resolve_profile_name(None, FallbackPolicy::AllowReadFallbacks, &inputs).unwrap();
            assert_eq!(resolved, "bravo-devlead-lanytehq");
        }

        #[test]
        fn env_set_but_no_exact_match_refuses_hard() {
            // Hard refuse — falling through to single-daemon or
            // active_profile when the operator's env states
            // bravo-devlead/lanytehq is exactly the silent
            // mis-attribution class PER-012 closes.
            let profiles = names(&["cxotech-lanytehq", "dispatch-lanytehq"]);
            let running = names(&["cxotech-lanytehq"]); // single daemon present
            let inputs = inputs(
                &profiles,
                &running,
                Some("cxotech-lanytehq"),
                Some("bravo-devlead"),
                Some("lanytehq"),
                None,
            );
            let err = resolve_profile_name(None, FallbackPolicy::AllowReadFallbacks, &inputs)
                .unwrap_err();
            match err {
                ResolverError::EnvExactMatchNotFound {
                    expected,
                    role,
                    scope,
                    ..
                } => {
                    assert_eq!(expected, "bravo-devlead-lanytehq");
                    assert_eq!(role, "bravo-devlead");
                    assert_eq!(scope, "lanytehq");
                }
                other => panic!("expected EnvExactMatchNotFound, got {other:?}"),
            }
        }

        // -- Rule 4: single running daemon (AllowReadFallbacks only). ------

        #[test]
        fn env_unset_single_daemon_resolves_with_read_fallbacks() {
            let profiles = names(&["bravo-devlead-lanytehq", "cxotech-lanytehq"]);
            let running = names(&["cxotech-lanytehq"]);
            let inputs = inputs(&profiles, &running, None, None, None, None);
            let resolved =
                resolve_profile_name(None, FallbackPolicy::AllowReadFallbacks, &inputs).unwrap();
            assert_eq!(resolved, "cxotech-lanytehq");
        }

        #[test]
        fn env_unset_multi_daemon_refuses_with_running_list() {
            let profiles = names(&["a-x", "b-x", "c-x"]);
            let running = names(&["a-x", "b-x"]);
            let inputs = inputs(&profiles, &running, None, None, None, None);
            let err = resolve_profile_name(None, FallbackPolicy::AllowReadFallbacks, &inputs)
                .unwrap_err();
            match err {
                ResolverError::AmbiguousMultiDaemon { running } => {
                    assert_eq!(running, names(&["a-x", "b-x"]));
                }
                other => panic!("expected AmbiguousMultiDaemon, got {other:?}"),
            }
        }

        // -- Rule 5: active_profile fallback (AllowReadFallbacks only). ----

        #[test]
        fn env_unset_no_daemons_active_profile_resolves_with_read_fallbacks() {
            let profiles = names(&["bravo-devlead-lanytehq", "cxotech-lanytehq"]);
            let inputs = inputs(
                &profiles,
                &[],
                Some("bravo-devlead-lanytehq"),
                None,
                None,
                None,
            );
            let resolved =
                resolve_profile_name(None, FallbackPolicy::AllowReadFallbacks, &inputs).unwrap();
            assert_eq!(resolved, "bravo-devlead-lanytehq");
        }

        #[test]
        fn active_profile_never_overrides_env_derived() {
            // Reverse case for AC #2: env says bravo-devlead-lanytehq;
            // active_profile says cxotech-lanytehq. Env wins.
            let profiles = names(&["bravo-devlead-lanytehq", "cxotech-lanytehq"]);
            let inputs = inputs(
                &profiles,
                &[],
                Some("cxotech-lanytehq"),
                Some("bravo-devlead"),
                Some("lanytehq"),
                None,
            );
            let resolved =
                resolve_profile_name(None, FallbackPolicy::AllowReadFallbacks, &inputs).unwrap();
            assert_eq!(resolved, "bravo-devlead-lanytehq");
        }

        #[test]
        fn stale_active_profile_pointer_refuses_with_dedicated_variant() {
            // entarch follow-up: a marker pointing at a deleted /
            // renamed profile is dead state. Once dispatch runs the
            // bare-profile rename sweep, every operator's active_profile
            // file may briefly point at a name that no longer exists.
            // The resolver must validate membership (same as rule 2 for
            // CHANVOY_PROFILE) and refuse with an actionable error
            // rather than returning the dead name and forcing a later
            // ProfileNotFound / NotRunning failure to diagnose for them.
            let profiles = names(&["bravo-devlead-lanytehq", "cxotech-lanytehq"]);
            let inputs = inputs(
                &profiles,
                &[],
                Some("bravo-devlead"), // bare name, post-rename-sweep stale
                None,
                None,
                None,
            );
            let err = resolve_profile_name(None, FallbackPolicy::AllowReadFallbacks, &inputs)
                .unwrap_err();
            match err {
                ResolverError::ActiveProfileNotFound { name, available } => {
                    assert_eq!(name, "bravo-devlead");
                    assert_eq!(available, profiles);
                }
                other => panic!("expected ActiveProfileNotFound, got {other:?}"),
            }
        }

        // -- Rule 6: refuse with available list. ---------------------------

        #[test]
        fn no_inputs_at_all_refuses_with_available_list() {
            let profiles = names(&["bravo-devlead-lanytehq", "cxotech-lanytehq"]);
            let inputs = inputs(&profiles, &[], None, None, None, None);
            let err = resolve_profile_name(None, FallbackPolicy::AllowReadFallbacks, &inputs)
                .unwrap_err();
            match err {
                ResolverError::CannotResolve { available } => {
                    assert_eq!(available, profiles);
                }
                other => panic!("expected CannotResolve, got {other:?}"),
            }
        }

        // -- ExplicitOnly policy. ------------------------------------------

        #[test]
        fn explicit_only_refuses_on_single_daemon_fallback() {
            // Side-effecting verb: even a single-running-daemon
            // resolution is unsafe when the operator hasn't stated
            // intent via flag or env.
            let profiles = names(&["dispatch-lanytehq"]);
            let running = names(&["dispatch-lanytehq"]);
            let inputs = inputs(&profiles, &running, None, None, None, None);
            let err =
                resolve_profile_name(None, FallbackPolicy::ExplicitOnly, &inputs).unwrap_err();
            assert!(matches!(
                err,
                ResolverError::DestructiveRequiresExplicit { .. }
            ));
        }

        #[test]
        fn explicit_only_refuses_on_active_profile_fallback() {
            let profiles = names(&["bravo-devlead-lanytehq"]);
            let inputs = inputs(
                &profiles,
                &[],
                Some("bravo-devlead-lanytehq"),
                None,
                None,
                None,
            );
            let err =
                resolve_profile_name(None, FallbackPolicy::ExplicitOnly, &inputs).unwrap_err();
            assert!(matches!(
                err,
                ResolverError::DestructiveRequiresExplicit { .. }
            ));
        }

        #[test]
        fn explicit_only_accepts_explicit_flag() {
            let inputs = inputs(&[], &[], None, None, None, None);
            let resolved = resolve_profile_name(
                Some("bravo-devlead-lanytehq"),
                FallbackPolicy::ExplicitOnly,
                &inputs,
            )
            .unwrap();
            assert_eq!(resolved, "bravo-devlead-lanytehq");
        }

        #[test]
        fn explicit_only_accepts_chanvoy_profile_env_when_valid() {
            let profiles = names(&["bravo-devlead-lanytehq"]);
            let inputs = inputs(
                &profiles,
                &[],
                None,
                None,
                None,
                Some("bravo-devlead-lanytehq"),
            );
            let resolved =
                resolve_profile_name(None, FallbackPolicy::ExplicitOnly, &inputs).unwrap();
            assert_eq!(resolved, "bravo-devlead-lanytehq");
        }

        #[test]
        fn explicit_only_accepts_env_exact_name_match() {
            let profiles = names(&["bravo-devlead-lanytehq"]);
            let inputs = inputs(
                &profiles,
                &[],
                None,
                Some("bravo-devlead"),
                Some("lanytehq"),
                None,
            );
            let resolved =
                resolve_profile_name(None, FallbackPolicy::ExplicitOnly, &inputs).unwrap();
            assert_eq!(resolved, "bravo-devlead-lanytehq");
        }
    }

    /// PER-019 cross-team channel resolution tests. Wiremock-based —
    /// covers the γ hybrid resolver's primary-first / fallback /
    /// ambiguity / no-match / explicit-override branches plus the
    /// SOP-MM-015 regression case dispatch flagged 2026-04-28.
    mod per_019_resolver {
        use super::*;
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        fn test_profile(server_url: &str) -> Profile {
            Profile {
                name: "bravo-devlead-lanytehq".to_string(),
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

        async fn mock_my_teams(server: &MockServer, teams: Vec<(&str, &str)>) {
            let body: Vec<_> = teams
                .into_iter()
                .map(|(id, name)| {
                    serde_json::json!({
                        "id": id,
                        "name": name,
                        "display_name": name,
                    })
                })
                .collect();
            Mock::given(method("GET"))
                .and(path("/api/v4/users/me/teams"))
                .respond_with(ResponseTemplate::new(200).set_body_json(body))
                .mount(server)
                .await;
        }

        async fn mock_team_by_slug(server: &MockServer, slug: &str, id: &str) {
            Mock::given(method("GET"))
                .and(path(format!("/api/v4/teams/name/{slug}")))
                .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                    "id": id,
                    "name": slug,
                })))
                .mount(server)
                .await;
        }

        async fn mock_channel_in_team(
            server: &MockServer,
            team_id: &str,
            channel_name: &str,
            channel_id: &str,
        ) {
            Mock::given(method("GET"))
                .and(path(format!(
                    "/api/v4/teams/{team_id}/channels/name/{channel_name}"
                )))
                .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                    "id": channel_id,
                    "name": channel_name,
                })))
                .mount(server)
                .await;
        }

        async fn mock_channel_404_in_team(server: &MockServer, team_id: &str, channel_name: &str) {
            Mock::given(method("GET"))
                .and(path(format!(
                    "/api/v4/teams/{team_id}/channels/name/{channel_name}"
                )))
                .respond_with(ResponseTemplate::new(404).set_body_json(serde_json::json!({
                    "id": "app.channel.get_by_name.missing.app_error",
                    "message": "Channel does not exist.",
                    "status_code": 404,
                })))
                .mount(server)
                .await;
        }

        #[tokio::test]
        async fn ac1_primary_team_channel_resolves_primary_source() {
            // AC #1: primary-team channels resolve unchanged from
            // pre-PER-019 behavior — single API call, no fallback.
            let server = MockServer::start().await;
            mock_my_teams(&server, vec![("team-lanytehq", "org-lanytehq")]).await;
            mock_team_by_slug(&server, "org-lanytehq", "team-lanytehq").await;
            mock_channel_in_team(&server, "team-lanytehq", "general", "ch-general").await;

            let client = MattermostClient::new(&test_profile(&server.uri()), "tok".into()).unwrap();
            let resolved = client.resolve_channel("general", None).await.unwrap();
            assert_eq!(resolved.channel_id, "ch-general");
            assert_eq!(resolved.team_name, "org-lanytehq");
            assert_eq!(resolved.resolution_source, ResolutionSource::Primary);
        }

        #[tokio::test]
        async fn ac2_cross_team_fallback_finds_unique_match() {
            // AC #2: bot in two teams, channel only on non-primary →
            // γ hybrid step 2 fallback succeeds and tags as Fallback.
            let server = MockServer::start().await;
            mock_my_teams(
                &server,
                vec![
                    ("team-lanytehq", "org-lanytehq"),
                    ("team-ops", "3-leaps-operations"),
                ],
            )
            .await;
            mock_team_by_slug(&server, "org-lanytehq", "team-lanytehq").await;
            mock_channel_404_in_team(&server, "team-lanytehq", "leadership").await;
            mock_channel_in_team(&server, "team-ops", "leadership", "ch-leadership").await;

            let client = MattermostClient::new(&test_profile(&server.uri()), "tok".into()).unwrap();
            let resolved = client.resolve_channel("leadership", None).await.unwrap();
            assert_eq!(resolved.channel_id, "ch-leadership");
            assert_eq!(resolved.team_name, "3-leaps-operations");
            assert_eq!(resolved.resolution_source, ResolutionSource::Fallback);
        }

        #[tokio::test]
        async fn ac3_ambiguous_channel_refuses_with_team_list() {
            // AC #3: same channel name on multiple non-primary teams →
            // refuse with AmbiguousChannel listing the matching teams.
            let server = MockServer::start().await;
            mock_my_teams(
                &server,
                vec![
                    ("team-lanytehq", "org-lanytehq"),
                    ("team-ops", "3-leaps-operations"),
                    ("team-fulmen", "org-fulmenhq"),
                ],
            )
            .await;
            mock_team_by_slug(&server, "org-lanytehq", "team-lanytehq").await;
            mock_channel_404_in_team(&server, "team-lanytehq", "general").await;
            mock_channel_in_team(&server, "team-ops", "general", "ch-ops-general").await;
            mock_channel_in_team(&server, "team-fulmen", "general", "ch-fulmen-general").await;

            let client = MattermostClient::new(&test_profile(&server.uri()), "tok".into()).unwrap();
            let err = client
                .resolve_channel("general", None)
                .await
                .expect_err("ambiguous resolution must refuse");
            match err {
                CoreError::AmbiguousChannel { channel, teams } => {
                    assert_eq!(channel, "general");
                    assert_eq!(teams.len(), 2);
                }
                other => panic!("expected AmbiguousChannel, got {other:?}"),
            }
        }

        #[tokio::test]
        async fn ac4_no_match_refuses_with_searched_teams_after_refresh() {
            // AC #4 + AC #14 self-healing: channel not in any member
            // team → force refresh, then refuse with searched-teams list.
            let server = MockServer::start().await;
            mock_my_teams(&server, vec![("team-lanytehq", "org-lanytehq")]).await;
            mock_team_by_slug(&server, "org-lanytehq", "team-lanytehq").await;
            mock_channel_404_in_team(&server, "team-lanytehq", "phantom").await;

            let client = MattermostClient::new(&test_profile(&server.uri()), "tok".into()).unwrap();
            let err = client
                .resolve_channel("phantom", None)
                .await
                .expect_err("no-match must refuse");
            match err {
                CoreError::ChannelNotFoundInAnyTeam { channel, teams } => {
                    assert_eq!(channel, "phantom");
                    assert!(teams.contains(&"org-lanytehq".to_string()));
                }
                other => panic!("expected ChannelNotFoundInAnyTeam, got {other:?}"),
            }
        }

        #[tokio::test]
        async fn ac5_explicit_team_channel_syntax_overrides() {
            // AC #5: <team>/<channel> takes precedence over the chain.
            let server = MockServer::start().await;
            mock_my_teams(
                &server,
                vec![
                    ("team-lanytehq", "org-lanytehq"),
                    ("team-ops", "3-leaps-operations"),
                ],
            )
            .await;
            // Both teams have a #general; explicit syntax forces ops.
            mock_channel_in_team(&server, "team-ops", "general", "ch-ops-general").await;

            let client = MattermostClient::new(&test_profile(&server.uri()), "tok".into()).unwrap();
            let resolved = client
                .resolve_channel("3-leaps-operations/general", None)
                .await
                .unwrap();
            assert_eq!(resolved.channel_id, "ch-ops-general");
            assert_eq!(resolved.team_name, "3-leaps-operations");
            assert_eq!(resolved.resolution_source, ResolutionSource::Explicit);
        }

        #[tokio::test]
        async fn explicit_team_not_a_member_refuses_distinctly() {
            // Per secrev's pin: distinguish "team you are not a member
            // of" from "channel not found".
            let server = MockServer::start().await;
            mock_my_teams(&server, vec![("team-lanytehq", "org-lanytehq")]).await;

            let client = MattermostClient::new(&test_profile(&server.uri()), "tok".into()).unwrap();
            let err = client
                .resolve_channel("not-my-team/anything", None)
                .await
                .expect_err("not-a-member must refuse");
            match err {
                CoreError::NotAMemberOfTeam { team, .. } => {
                    assert_eq!(team, "not-my-team");
                }
                other => panic!("expected NotAMemberOfTeam, got {other:?}"),
            }
        }

        #[tokio::test]
        async fn ac8_sop_mm_015_regression_3_leaps_operations_leadership() {
            // AC #8 SOP-MM-015 regression pin: post to
            // 3-leaps-operations/#leadership from a profile bound to
            // org-lanytehq (the exact case dispatch hit 2026-04-28).
            let server = MockServer::start().await;
            mock_my_teams(
                &server,
                vec![
                    ("team-lanytehq", "org-lanytehq"),
                    ("team-ops", "3-leaps-operations"),
                ],
            )
            .await;
            mock_team_by_slug(&server, "org-lanytehq", "team-lanytehq").await;
            mock_channel_404_in_team(&server, "team-lanytehq", "leadership").await;
            mock_channel_in_team(&server, "team-ops", "leadership", "ch-leadership").await;

            let client = MattermostClient::new(&test_profile(&server.uri()), "tok".into()).unwrap();
            let resolved = client.resolve_channel("leadership", None).await.unwrap();
            assert_eq!(resolved.team_name, "3-leaps-operations");
            assert_eq!(resolved.channel_id, "ch-leadership");
            assert_eq!(resolved.resolution_source, ResolutionSource::Fallback);
        }

        #[tokio::test]
        async fn ac15_migration_quarantines_ambiguous_legacy_record() {
            // AC #15: a legacy `(profile, channel-name)` cursor that now
            // resolves ambiguously must be quarantined, not silently
            // bound to one team.
            let server = MockServer::start().await;
            mock_my_teams(
                &server,
                vec![
                    ("team-lanytehq", "org-lanytehq"),
                    ("team-ops", "3-leaps-operations"),
                ],
            )
            .await;
            mock_team_by_slug(&server, "org-lanytehq", "team-lanytehq").await;
            // Primary AND fallback both have the channel — devrev's pin
            // says primary wins on migration even when fallbacks also match.
            mock_channel_in_team(&server, "team-lanytehq", "general", "ch-lh-general").await;
            mock_channel_in_team(&server, "team-ops", "general", "ch-ops-general").await;

            // Build state with a legacy bare-name entry.
            let mut state = AttentionState {
                channels: BTreeMap::from([(
                    "general".to_string(),
                    ChannelCursorState {
                        last_seen_post_id: Some("pre-merge-post".to_string()),
                        updated_at: Some(1_776_000_000_000),
                        last_known_stale: false,
                        last_checked_at: None,
                        channel_id: String::new(),
                        team_id: String::new(),
                        team_name: String::new(),
                        channel_name: String::new(),
                    },
                )]),
                mentions: MentionCursorState::default(),
                quarantined: Vec::new(),
            };

            let client = MattermostClient::new(&test_profile(&server.uri()), "tok".into()).unwrap();
            // Primary lookup matches → migrates clean to the qualified
            // key for the primary team. (devrev's "primary-team-first"
            // pin: even when fallback teams also have the name, primary
            // wins on migration.)
            let outcome = migrate_attention_state(&mut state, &client).await.unwrap();
            assert_eq!(outcome.migrated, 1);
            assert_eq!(outcome.quarantined, 0);
            assert!(state.channels.contains_key("org-lanytehq/general"));
            assert!(!state.channels.contains_key("general"));
        }

        #[tokio::test]
        async fn devrev_pr17_finding4_explicit_strips_hash_from_channel_segment() {
            // devrev PR #17 finding #4: `<team>/#<channel>` should
            // resolve identically to `<team>/<channel>`. Operators
            // routinely include `#` when pasting channel names from
            // the Mattermost UI; the resolver must normalize.
            let server = MockServer::start().await;
            mock_my_teams(
                &server,
                vec![
                    ("team-lanytehq", "org-lanytehq"),
                    ("team-ops", "3-leaps-operations"),
                ],
            )
            .await;
            mock_channel_in_team(&server, "team-ops", "development", "ch-dev").await;

            let client = MattermostClient::new(&test_profile(&server.uri()), "tok".into()).unwrap();
            let resolved_with_hash = client
                .resolve_channel("3-leaps-operations/#development", None)
                .await
                .unwrap();
            let resolved_no_hash = client
                .resolve_channel("3-leaps-operations/development", None)
                .await
                .unwrap();
            assert_eq!(resolved_with_hash.channel_id, resolved_no_hash.channel_id);
            assert_eq!(resolved_with_hash.team_name, "3-leaps-operations");
            assert_eq!(resolved_with_hash.channel_name, "development");
            assert_eq!(
                resolved_with_hash.resolution_source,
                ResolutionSource::Explicit
            );
        }

        #[tokio::test]
        async fn devrev_pr17_finding3_since_last_mine_uses_explicit_team() {
            // devrev PR #17 finding #3: read --since-last-mine had a
            // bug where `latest_authored_post_id` resolved with team=None
            // even when the caller passed `team=Some(...)`. With
            // duplicate-name channels, the search would hit the wrong
            // team's posts. After the fix, both the search and the
            // subsequent read should target the explicit team.
            //
            // Build a server where the channel name "duplicates" exists
            // on both teams, and verify that --team Ops directs the
            // search at Ops's team_id.
            let server = MockServer::start().await;
            mock_my_teams(
                &server,
                vec![
                    ("team-lanytehq", "org-lanytehq"),
                    ("team-ops", "3-leaps-operations"),
                ],
            )
            .await;
            // Both teams have a channel named "duplicates".
            mock_channel_in_team(&server, "team-lanytehq", "duplicates", "ch-lh-dup").await;
            mock_channel_in_team(&server, "team-ops", "duplicates", "ch-ops-dup").await;
            // Mock search ONLY on Ops's team_id; if the resolver
            // ignored the override, the search would 404 or land on
            // the wrong team's mock.
            Mock::given(method("POST"))
                .and(path("/api/v4/teams/team-ops/posts/search"))
                .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                    "posts": {
                        "post-ops-1": { "id": "post-ops-1", "create_at": 1_777_000_000_000_i64 }
                    }
                })))
                .mount(&server)
                .await;
            // Whoami needed by read_channel_since_last_mine.
            Mock::given(method("GET"))
                .and(path("/api/v4/users/me"))
                .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                    "id": "bot-id",
                    "username": "agent-bravo-devlead",
                    "is_bot": true,
                    "nickname": null,
                    "email": null,
                })))
                .mount(&server)
                .await;
            // Mock the after-anchor read (assert_post_in_channel + posts page).
            Mock::given(method("GET"))
                .and(path("/api/v4/posts/post-ops-1"))
                .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                    "channel_id": "ch-ops-dup"
                })))
                .mount(&server)
                .await;
            Mock::given(method("GET"))
                .and(path("/api/v4/channels/ch-ops-dup/posts"))
                .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                    "posts": {}
                })))
                .mount(&server)
                .await;

            let client = MattermostClient::new(&test_profile(&server.uri()), "tok".into()).unwrap();
            // No assertion of "primary mock NOT called" — wiremock
            // doesn't enforce that without `expect`. The fact that
            // this returns Ok proves the search hit the Ops team's
            // mock; if it had hit team-lanytehq's team_id, the
            // unmocked path would 404 and the call would error.
            let _msgs = client
                .read_channel_since_last_mine("duplicates", Some("3-leaps-operations"))
                .await
                .expect("read should target Ops team");
        }

        #[tokio::test]
        async fn migration_quarantines_when_only_fallbacks_are_ambiguous() {
            // Variant of AC #15: primary doesn't have it; multiple
            // fallback teams do → quarantine.
            let server = MockServer::start().await;
            mock_my_teams(
                &server,
                vec![
                    ("team-lanytehq", "org-lanytehq"),
                    ("team-ops", "3-leaps-operations"),
                    ("team-fulmen", "org-fulmenhq"),
                ],
            )
            .await;
            mock_team_by_slug(&server, "org-lanytehq", "team-lanytehq").await;
            mock_channel_404_in_team(&server, "team-lanytehq", "general").await;
            mock_channel_in_team(&server, "team-ops", "general", "ch-ops-general").await;
            mock_channel_in_team(&server, "team-fulmen", "general", "ch-fulmen-general").await;

            let mut state = AttentionState {
                channels: BTreeMap::from([(
                    "general".to_string(),
                    ChannelCursorState {
                        last_seen_post_id: Some("pre-merge-post".to_string()),
                        updated_at: Some(1_776_000_000_000),
                        last_known_stale: false,
                        last_checked_at: None,
                        channel_id: String::new(),
                        team_id: String::new(),
                        team_name: String::new(),
                        channel_name: String::new(),
                    },
                )]),
                mentions: MentionCursorState::default(),
                quarantined: Vec::new(),
            };

            let client = MattermostClient::new(&test_profile(&server.uri()), "tok".into()).unwrap();
            let outcome = migrate_attention_state(&mut state, &client).await.unwrap();
            assert_eq!(outcome.migrated, 0);
            assert_eq!(outcome.quarantined, 1);
            assert!(state.channels.is_empty());
            assert_eq!(state.quarantined.len(), 1);
            assert_eq!(state.quarantined[0].legacy_channel_name, "general");
            assert_eq!(state.quarantined[0].ambiguous_teams.len(), 2);
        }
    }
}
