pub mod bootstrap;
pub mod safe_read;

pub use safe_read::{
    read_caller_named_file, read_credential_file, read_tool_owned_file, SafeReadError,
    CREDENTIAL_MAX_BYTES, DEFAULT_MAX_BYTES,
};

pub use bootstrap::{
    bootstrap_path_for_profile, build_bootstrap_state, compute_profile_fingerprint,
    consume_bootstrap_state, generate_nonce, read_bootstrap_state, resolve_startup_identity,
    validate_bootstrap_state, write_bootstrap_state, BootstrapError, BootstrapResolution,
    BootstrapState, BOOTSTRAP_MAX_AGE_SECS, BOOTSTRAP_NONCE_ENV,
};

use std::collections::{BTreeMap, BTreeSet, HashMap};
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
    /// PER-035: optional profile-level identity-reduction policy. When
    /// set, channel-targeted writes whose resolved channel lives
    /// *outside* this profile's `team_name` post under
    /// `reduce.use_profile`'s identity instead of this profile's. Lets a
    /// stream-suffixed engagement bot defer to its bare family bot for
    /// galaxy-wide posts without per-call `--profile` discipline.
    /// Omitted (`None`) ⇒ no reduction; this profile handles all posts
    /// (today's behavior). Distinct from PER-019's channel-team
    /// `fallback` (which resolves *which channel*); `reduce` resolves
    /// *which identity posts*.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reduce: Option<ReducePolicy>,
}

/// PER-035: identity-reduction policy. Serializes as a `[reduce]` TOML
/// table on the profile. One level only (stream → family); no
/// transitive chains (brief §Out of scope).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReducePolicy {
    /// Name of the bare family profile to reduce to. Must exist on disk
    /// (`<profile-dir>/<use_profile>.toml`); a missing target is a loud
    /// failure at daemon start, never a silent fall-back to the bare
    /// daemon identity (brief AC: negative case).
    pub use_profile: String,
}

/// PER-035 (pure, testable): does posting identity reduce for a write
/// whose channel resolved into `resolved_team_name`, given the calling
/// profile's primary team `profile_team_name`?
///
/// The rule is exactly the brief's §Scope semantics: inside the
/// profile's own team ⇒ keep this identity; anywhere else ⇒ reduce.
/// The `--team` override and `<team>/<channel>` syntax change *which*
/// team the channel resolves into, so they flow through this same
/// comparison with no special-casing.
pub fn identity_reduces(profile_team_name: &str, resolved_team_name: &str) -> bool {
    profile_team_name != resolved_team_name
}

/// PER-035 (pure, testable): the provenance tags a write's audit-log
/// line carries, naming the PER-019 channel-resolution path and the
/// PER-035 posting-identity path *independently* (brief AC). Returns
/// `[team-fallback]` when the channel resolved via a non-primary team,
/// and `[identity-reduce]` when the posting identity reduced; both when
/// both apply; empty when neither (primary-team channel, no reduction).
pub fn posting_provenance_tags(
    resolution_source: ResolutionSource,
    identity_reduced: bool,
) -> Vec<&'static str> {
    let mut tags = Vec::new();
    if resolution_source == ResolutionSource::Fallback {
        tags.push("team-fallback");
    }
    if identity_reduced {
        tags.push("identity-reduce");
    }
    tags
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
    /// Thread the message belongs to. A top-level post carries its own
    /// id here; a reply carries the id of the post that started the
    /// thread. Never empty on messages produced by chanvoy.
    ///
    /// `#[serde(default)]` is deliberate and load-bearing: a freshly
    /// installed CLI must still be able to read responses from an
    /// older daemon that is still running and does not send this
    /// field. Such messages deserialize with an empty `root_id`,
    /// which callers treat as "thread unknown".
    #[serde(default)]
    pub root_id: String,
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
    /// The thread this post belongs to: the post's own id when it is
    /// top-level, the thread's root id when it is a reply. Carried on
    /// the push path so a caller can reply to a pushed message without
    /// a second round trip — replying to a reply is rejected by the
    /// provider, so the distinction is load-bearing, not cosmetic.
    ///
    /// Defaulted for tolerance of events produced before this field
    /// existed; normalized to a non-empty value on the way in.
    #[serde(default)]
    pub root_id: String,
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

/// Parameters for fetching one post by id. Pure read; the channel is
/// required so the post can be bound to a channel the caller named
/// before any body is returned.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct GetPostParams {
    pub channel: String,
    pub post_id: String,
    #[serde(default)]
    pub team: Option<String>,
}

/// Parameters for reading a thread. `post_id` may name either the
/// thread's root or any reply in it — the canonical root is derived
/// from the anchor post. Pure read; no cursor side effects.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReadThreadParams {
    pub channel: String,
    pub post_id: String,
    /// Keep only the final message of the thread. The response stays a
    /// list either way.
    #[serde(default)]
    pub latest: bool,
    #[serde(default)]
    pub team: Option<String>,
}

#[cfg(test)]
mod read_thread_params_tests {
    use super::ReadThreadParams;

    /// The optional fields have to be genuinely optional on the wire.
    ///
    /// A caller older than `--latest` — or any peer that never learned
    /// about it — sends only the channel and the post id. Without the
    /// defaults that request fails to parse and the whole verb is
    /// unavailable across a version skew, rather than degrading to its
    /// original behavior. The values matter as much as the parse: a
    /// `latest` that defaulted to `true` would silently turn every
    /// legacy thread read into a one-message read, and a `team` that
    /// defaulted to anything but `None` would move the read off the
    /// profile's primary team without the caller asking.
    #[test]
    fn omitted_optional_fields_default_to_the_original_behavior() {
        let params: ReadThreadParams =
            serde_json::from_str(r#"{"channel":"bravo-team","post_id":"post-1"}"#)
                .expect("a request without the optional fields must still parse");

        assert_eq!(params.channel, "bravo-team");
        assert_eq!(params.post_id, "post-1");
        assert!(
            !params.latest,
            "an unstated --latest means the whole thread, not its last message"
        );
        assert_eq!(
            params.team, None,
            "an unstated team means the profile's own resolution chain"
        );
    }

    /// The defaults must not be shadowing values that were sent.
    #[test]
    fn stated_optional_fields_are_carried_through() {
        let params: ReadThreadParams = serde_json::from_str(
            r#"{"channel":"bravo-team","post_id":"post-1","latest":true,"team":"org-otherhq"}"#,
        )
        .expect("a fully specified request parses");

        assert!(params.latest);
        assert_eq!(params.team.as_deref(), Some("org-otherhq"));
    }
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

/// PER-034: parameters for `chanvoy pin <channel> <post_id>`. Same
/// shape as `ReactParams` minus the emoji; pin/unpin operate on a
/// (channel, post-id) pair without further qualifiers.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PinParams {
    pub channel: String,
    pub post_id: String,
    #[serde(default)]
    pub team: Option<String>,
}

/// PER-034: parameters for `chanvoy unpin <channel> <post_id>`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct UnpinParams {
    pub channel: String,
    pub post_id: String,
    #[serde(default)]
    pub team: Option<String>,
}

/// PER-034: outcome of a `chanvoy pin` call. `was_already_pinned`
/// surfaces the pre-call pin state so wrapping scripts can
/// distinguish "I just pinned this" from "this was already pinned" —
/// useful for the dispatch pin-rotation workflow. Determined by
/// reading the post object's `is_pinned` field before issuing the
/// write (zero extra round-trips: the channel-membership assertion
/// already needs to GET the post).
///
/// Field shape (per brief AC #5 + devrev PR #36 review):
/// - `verb` and `channel_id` are explicit brief-required fields
/// - `team` and `ok` mirror `ReactionResult`'s shape (PER-024 pre-impl
///   pin #2) so cross-team channel disambiguation and the
///   uniform-success contract are consistent across write verbs
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PinResult {
    /// Always `"pin"`. Lets `--json` consumers identify the verb
    /// directly per brief §Output formats sample.
    pub verb: String,
    pub team: String,
    pub channel: String,
    /// Mattermost channel id (the stable provider-level id, not the
    /// slug). Brief AC #5 sample field.
    pub channel_id: String,
    pub post_id: String,
    /// `"pinned"` — the post-call state, distinct from `verb`.
    pub result: String,
    /// True iff `is_pinned` was already true when this call started.
    pub was_already_pinned: bool,
    pub ok: bool,
}

/// PER-034: outcome of a `chanvoy unpin` call. Symmetric to
/// `PinResult` with `was_already_unpinned` as the idempotency
/// signal.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct UnpinResult {
    /// Always `"unpin"`.
    pub verb: String,
    pub team: String,
    pub channel: String,
    pub channel_id: String,
    pub post_id: String,
    /// `"unpinned"`.
    pub result: String,
    /// True iff `is_pinned` was already false when this call started.
    pub was_already_unpinned: bool,
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

/// PER-038: enhanced wait RPC. Method name is the capability gate an
/// old daemon cannot ignore (`wait_channel_v2`). Carries content filter
/// and exclusive baseline anchor; timeout is always second-resolution.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct WaitChannelV2Params {
    pub channel: String,
    pub timeout_secs: u64,
    /// PER-019: optional `--team <slug>` override.
    #[serde(default)]
    pub team: Option<String>,
    /// Literal body substring (case-sensitive). Empty is refused.
    #[serde(default)]
    pub contains: Option<String>,
    /// Rust `regex` pattern over body only. Empty is refused. Source
    /// capped at 256 UTF-8 bytes; compiled size capped at 64 KiB.
    #[serde(default)]
    pub pattern: Option<String>,
    /// Exclusive baseline post id: only posts strictly after this id
    /// can wake the wait.
    #[serde(default)]
    pub after: Option<String>,
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

/// Errors from the core client.
///
/// Marked `#[non_exhaustive]`: this is a growing operational taxonomy,
/// not a closed state machine, and every future addition would otherwise
/// be a source-breaking change for anyone matching on it exhaustively.
/// Callers must carry a catch-all arm.
#[derive(Debug, Error)]
#[non_exhaustive]
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
    /// A caller reached the removed unbound thread read.
    ///
    /// The replacement takes the channel the read is scoped to. The old
    /// entry point cannot be forwarded to it, because forwarding would
    /// have to invent a channel and would reinstate precisely the
    /// unscoped read the replacement exists to prevent. It refuses
    /// instead, and says what to call.
    #[error(
        "read_thread was removed: it could not verify which channel a thread belonged to. Call read_thread_in_channel(expected_channel_id, channel_name, root_post_id) instead."
    )]
    UnboundThreadReadRemoved,
    /// A thread read came back with zero posts. Every thread contains at
    /// least its own root, so an empty body means the provider could not
    /// give us the thread rather than that the thread is empty — surface
    /// it loudly instead of returning a successful empty list that reads
    /// to an operator as "nothing was said here".
    #[error(
        "thread {root_id} came back empty. A thread always contains at least its root post, \
         so this means the post was deleted, the id belongs to a channel this bot cannot \
         read, or the id is not a post id. Check the post id and the bot's channel access."
    )]
    EmptyThread { root_id: String },
    #[error("no stored cursor exists for channel {channel}")]
    NoStoredCursor { channel: String },
    #[error("operation requires elevated capability")]
    RequiresElevatedCapability,
    #[error("timeout waiting for channel {0}")]
    WaitTimeout(String),
    /// PER-038: wait input/config hard failure (bad filter, empty
    /// needle, foreign/missing/substituted anchor). Never a deadman:
    /// agents reconfigure rather than re-arm with backoff alone.
    #[error("wait input error: {0}")]
    WaitFilterInvalid(String),
    /// PER-038: observation failed until the absolute wait deadline
    /// (retryable provider 429/5xx/transport). Distinct from clean
    /// deadman: never reports `timeout:true` at the CLI.
    #[error("wait observation failed for channel {channel}: {message}")]
    WaitProviderDegraded { channel: String, message: String },
    #[error("profile {0} not found")]
    ProfileNotFound(String),
    /// PER-035: a profile's `reduce.use_profile` names a family profile
    /// that does not exist on disk. Loud failure at daemon start — the
    /// daemon must NOT silently fall back to the bare daemon identity
    /// (brief AC: negative case), since that would leak stream identity
    /// into the galaxy precisely when the operator asked for reduction.
    #[error(
        "profile '{calling}' has reduce.use_profile = '{missing}' but no such profile exists; \
         create it (`chanvoy auto-setup` under the family identity) or correct the reduce policy. \
         Available profiles: {available:?}"
    )]
    ReduceProfileNotFound {
        calling: String,
        missing: String,
        available: Vec<String>,
    },
    /// PER-035 (devrev PR #37 P1): the token loaded for a reduce target
    /// authenticates as a *different* bot than the family profile names.
    /// This is the silent-identity-leak guard: if the family profile
    /// shares an `env_name` with the stream profile, `load_token` returns
    /// the stream token, and the "family" client would post as the
    /// stream bot while the audit log claimed family identity. The daemon
    /// refuses to start rather than leak stream identity into the galaxy
    /// under a false attribution.
    #[error(
        "reduce target profile '{profile}' is configured for bot '{expected}', but the token \
         resolved for it authenticates as '{actual}' — the family profile likely shares an \
         env_name/token source with the calling (stream) profile. Give the family profile its \
         own token env (distinct `env_name`) so its identity cannot be shadowed by the stream's."
    )]
    ReduceIdentityMismatch {
        profile: String,
        expected: String,
        actual: String,
    },
    /// PER-036A / ADR-0016: an agent-critical file read was refused
    /// (symlink, non-regular file, over-cap, loose credential permissions,
    /// or non-UTF-8). Carries the specific reason + remediation.
    #[error(transparent)]
    SafeRead(#[from] SafeReadError),
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
    /// PER-034 AC #8: chanvoy normalizes MM 403 on `POST /posts/{id}/pin`
    /// or `/unpin` into a verb-specific diagnostic that names the
    /// missing channel-admin permission, rather than surfacing the
    /// generic API-error path. Operators with no MM-API context see
    /// what to ask their workspace admin for.
    #[error(
        "bot {bot_username:?} lacks the channel-admin permission required \
         to {verb} posts in {team:?}/{channel:?}. \
         Ask your Mattermost workspace admin to grant the bot the \
         `manage_public_channel_members` / `manage_private_channel_members` \
         scheme role on the channel, or use a profile whose bot already \
         has channel-admin there. \
         Mattermost reported: {message}"
    )]
    PinPermissionDenied {
        verb: &'static str,
        bot_username: String,
        team: String,
        channel: String,
        message: String,
    },
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
    // PER-036A / ADR-0016: an env-file is caller-named credential material
    // (it carries the Mattermost token and determines posting identity).
    // Read it through the credential-tier safe reader: refuse a symlinked
    // final component, non-regular files, group/world-accessible perms, and
    // bound the read at 64 KiB — before parsing. Diagnostics name the path
    // and policy failure, never token contents.
    let contents = safe_read::read_credential_file(path, safe_read::CREDENTIAL_MAX_BYTES)?;
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
    // PER-036A / ADR-0016: persisted profile TOML carries identity
    // (bot_username, team) and the PER-035 reduction policy — agent-critical.
    // It is a chanvoy-owned config read (the profile dir is chanvoy-created
    // 0700), so use the tool-owned tier: a symlink to a regular file is
    // allowed (dotfile/seclusor layouts), but non-regular targets are refused
    // and the read is bounded before parsing.
    let contents = match safe_read::read_tool_owned_file(&path, safe_read::DEFAULT_MAX_BYTES) {
        Ok(contents) => contents,
        Err(err) if err.is_not_found() => return Err(CoreError::ProfileNotFound(name.to_string())),
        Err(err) => return Err(err.into()),
    };
    let mut profile: Profile = toml::from_str(&contents)?;
    if profile.name.is_empty() {
        profile.name = name.to_string();
    }
    Ok(profile)
}

pub fn load_active_profile() -> Result<Option<String>, CoreError> {
    let path = active_profile_path();
    // PER-036A / ADR-0016: the active-profile marker is low-criticality
    // tool-owned state (a single profile name in the chanvoy-created 0700
    // config dir), but route it through the tool-owned reader anyway so a
    // non-regular path can't block and the read is bounded. Absent marker is
    // the normal "no active profile" case.
    match safe_read::read_tool_owned_file(&path, safe_read::DEFAULT_MAX_BYTES) {
        Ok(contents) => {
            let trimmed = contents.trim();
            if trimmed.is_empty() {
                Ok(None)
            } else {
                Ok(Some(trimmed.to_string()))
            }
        }
        Err(err) if err.is_not_found() => Ok(None),
        Err(err) => Err(err.into()),
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
        // PER-036A / ADR-0016 (devrev PR #39 finding #1): each profile TOML
        // feeds the resolver / profile collection, so it is agent-critical
        // and must go through the same tool-owned safe reader as
        // `load_profile` — non-regular refusal, private-parent verification,
        // and a bounded read — not a raw `fs::read_to_string`.
        let contents = safe_read::read_tool_owned_file(&path, safe_read::DEFAULT_MAX_BYTES)?;
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
    /// Variant name predates the widening of `ExplicitOnly` beyond
    /// `daemon stop`. The message says "side-effecting daemon-lifecycle
    /// verb" because that is what the policy actually covers: `daemon start`
    /// is not destructive, but resolving its target by fallback could start a
    /// daemon under an identity the operator never named. Renaming the
    /// variant is a breaking change for downstream matchers and is not worth
    /// it for accuracy the message already carries.
    #[error(
        "side-effecting daemon-lifecycle verb requires explicit profile selection; \
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
    // PER-036A / ADR-0016: attention state is tool-owned control-plane state
    // (cursors) in the chanvoy-created 0700 config dir. Tool-owned tier:
    // non-regular refusal + bounded read before parsing; absent file is the
    // normal cold-start default.
    match safe_read::read_tool_owned_file(&path, safe_read::DEFAULT_MAX_BYTES) {
        Ok(contents) => Ok(serde_json::from_str(&contents)?),
        Err(err) if err.is_not_found() => Ok(AttentionState::default()),
        Err(err) => Err(err.into()),
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

/// How long a resolved author name stays usable before chanvoy asks the
/// provider again. Matches the team-list window above: long enough to
/// amortize the lookup across a working session, short enough that a
/// rename becomes visible without restarting the daemon.
pub const AUTHOR_CACHE_TTL: Duration = Duration::from_secs(15 * 60);

/// Hard ceiling on cached author entries. A busy channel can surface a
/// long tail of one-off authors, so the cache is bounded rather than
/// unbounded-with-a-TTL: expired entries are dropped first, then the
/// oldest entries, so memory cannot grow with the number of distinct
/// people a long-running daemon has ever seen.
pub const AUTHOR_CACHE_MAX_ENTRIES: usize = 1024;

/// How long a single author lookup may take before chanvoy gives up on
/// it and reports the user id instead.
///
/// Author resolution is a courtesy on top of a read: the read already
/// has everything it needs except a display name. Without a deadline
/// the fallback is only reachable when the provider *refuses* — a
/// provider that accepts the connection and then stalls would hang the
/// read that triggered it, and every `read` and `wait` goes through
/// this path. Bounding the wait is what makes "falls back to the user
/// id" true in all cases rather than only the polite ones.
pub const AUTHOR_RESOLVE_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Debug, Clone)]
struct AuthorCacheEntry {
    username: String,
    fetched_at: std::time::Instant,
}

/// The provider's post shape, as chanvoy consumes it. Every read path
/// (channel reads, pinned, most-recent, search, thread) decodes into
/// this one type so a shape fix lands in a single place.
///
/// Note what is *not* here: the provider does not send an author name
/// on a post, only `user_id`. Anything that wants a display name has to
/// resolve it separately.
#[derive(Debug, Clone, Deserialize)]
pub(crate) struct RawPost {
    pub id: String,
    /// Owning channel. Defaulted because the thread and search shapes
    /// do not always carry it. Read by the point-fetch path, which
    /// compares it against the channel the caller named before it will
    /// hand back a body.
    #[serde(default)]
    pub channel_id: String,
    pub user_id: String,
    pub message: String,
    pub create_at: i64,
    /// Empty for a top-level post; the thread's root id for a reply.
    /// Normalized to the post's own id on the way into a `Message`.
    #[serde(default)]
    pub root_id: String,
}

/// The provider's envelope for any list-of-posts response: a ranked
/// `order` array plus a map of posts keyed by id.
///
/// Both fields default. `posts` is the load-bearing one — the provider
/// omits the key entirely for a channel with no pinned posts, and a
/// non-defaulted field would turn that into a decode failure.
#[derive(Debug, Clone, Default, Deserialize)]
pub(crate) struct PostsEnvelope {
    #[serde(default)]
    pub order: Vec<String>,
    #[serde(default)]
    pub posts: BTreeMap<String, RawPost>,
}

/// The thread a post belongs to, as chanvoy reports it.
///
/// The provider sends an empty root for a top-level post. Chanvoy
/// reports the post's own id instead, so that every message names a
/// usable reply target and callers never have to special-case an empty
/// string. Replying to a reply is rejected by the provider, so a caller
/// that blindly reused a post id would fail on exactly the replies this
/// value exists to disambiguate.
///
/// Used by both the request-response read path and the push path so the
/// two cannot drift.
fn normalize_root_id(provider_root_id: &str, post_id: &str) -> String {
    if provider_root_id.is_empty() {
        post_id.to_string()
    } else {
        provider_root_id.to_string()
    }
}

/// How many posts one time-window channel read asks the provider for.
///
/// A single page. A caller that receives exactly this many cannot tell
/// a complete window from a truncated one, so completeness must be
/// reported as unknown rather than guessed at — see the peer surface's
/// truncation reporting.
pub const CHANNEL_WINDOW_PAGE_SIZE: usize = 30;

/// Whether a post's channel satisfies the channel it was requested in.
///
/// An empty value on either side is refused rather than treated as a
/// match. The provider always sends a channel on a post, so an empty
/// one means the response is not what we believe it is — and this check
/// is the only thing standing between a bare post id and a read, so it
/// is the wrong place to extend the provider any benefit of the doubt.
fn binding_holds(actual_channel_id: &str, expected_channel_id: &str) -> bool {
    !actual_channel_id.is_empty()
        && !expected_channel_id.is_empty()
        && actual_channel_id == expected_channel_id
}

/// Insert a resolved author into the cache, evicting first by expiry
/// and then by age so the map stays under `AUTHOR_CACHE_MAX_ENTRIES`.
/// Free function so the bound can be exercised directly in tests
/// without a live client.
fn insert_author_entry(
    cache: &mut HashMap<String, AuthorCacheEntry>,
    user_id: String,
    username: String,
    now: std::time::Instant,
) {
    if !cache.contains_key(&user_id) && cache.len() >= AUTHOR_CACHE_MAX_ENTRIES {
        cache.retain(|_, entry| now.duration_since(entry.fetched_at) < AUTHOR_CACHE_TTL);
        while cache.len() >= AUTHOR_CACHE_MAX_ENTRIES {
            let oldest = cache
                .iter()
                .min_by_key(|(_, entry)| entry.fetched_at)
                .map(|(key, _)| key.clone());
            match oldest {
                Some(key) => {
                    cache.remove(&key);
                }
                None => break,
            }
        }
    }
    cache.insert(
        user_id,
        AuthorCacheEntry {
            username,
            fetched_at: now,
        },
    );
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
    /// Resolved author names keyed by user id, so a channel read does
    /// not re-ask the provider for the same handful of people on every
    /// call. Keyed by user id and nothing else — no credential material
    /// ever enters this map. Shared across clones for the same reason
    /// the team cache is: every daemon context should see one cache.
    author_cache: Arc<tokio::sync::RwLock<HashMap<String, AuthorCacheEntry>>>,
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
            author_cache: Arc::new(tokio::sync::RwLock::new(HashMap::new())),
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

    /// The credential this client authenticates with.
    ///
    /// Exposed so that surfaces which need the same identity derive it
    /// from the client rather than reading the token source a second
    /// time. Two reads can straddle a rotation and authenticate as
    /// different identities.
    pub(crate) fn token(&self) -> &str {
        &self.token
    }

    /// Resolve a single author name, preferring the shared cache.
    /// Falls back to the literal user id when the provider cannot tell
    /// us the name — an id an operator can look up beats a placeholder
    /// that pretends to be a person.
    pub async fn author_username(&self, user_id: &str) -> String {
        let mut wanted = BTreeSet::new();
        wanted.insert(user_id.to_string());
        self.resolve_authors(&wanted)
            .await
            .get(user_id)
            .cloned()
            .unwrap_or_else(|| user_id.to_string())
    }

    /// Resolve display names for a set of author ids.
    ///
    /// The cache lock is never held across a network call: we take the
    /// read lock to collect hits, drop it, fetch the misses, then take
    /// the write lock to record what we learned. Only successes are
    /// cached — a transient provider failure must not pin a fallback
    /// name for the whole cache window.
    ///
    /// Ids that could not be resolved are simply absent from the
    /// returned map; callers substitute the id itself.
    async fn resolve_authors(&self, user_ids: &BTreeSet<String>) -> HashMap<String, String> {
        let mut resolved: HashMap<String, String> = HashMap::new();
        let mut missing: Vec<String> = Vec::new();
        {
            let guard = self.author_cache.read().await;
            for user_id in user_ids {
                match guard.get(user_id) {
                    Some(entry) if entry.fetched_at.elapsed() < AUTHOR_CACHE_TTL => {
                        resolved.insert(user_id.clone(), entry.username.clone());
                    }
                    _ => missing.push(user_id.clone()),
                }
            }
        }
        if missing.is_empty() {
            return resolved;
        }

        let mut fetched: Vec<(String, String)> = Vec::with_capacity(missing.len());
        for user_id in missing {
            if let Some(username) = self.fetch_username(&user_id).await {
                fetched.push((user_id, username));
            }
        }
        if fetched.is_empty() {
            return resolved;
        }

        let now = std::time::Instant::now();
        let mut guard = self.author_cache.write().await;
        for (user_id, username) in fetched {
            insert_author_entry(&mut guard, user_id.clone(), username.clone(), now);
            resolved.insert(user_id, username);
        }
        resolved
    }

    /// One author lookup. Returns `None` on any failure; the diagnostic
    /// stays short and deliberately carries no response body, since a
    /// user record holds more about a person than chanvoy needs.
    async fn fetch_username(&self, user_id: &str) -> Option<String> {
        self.fetch_username_within(user_id, AUTHOR_RESOLVE_TIMEOUT)
            .await
    }

    /// The body of a single author lookup, with the deadline passed in
    /// so a test can prove the elapsed path without waiting out the
    /// production timeout.
    async fn fetch_username_within(&self, user_id: &str, deadline: Duration) -> Option<String> {
        #[derive(Deserialize)]
        struct RawUser {
            username: String,
        }
        let endpoint = format!("/users/{user_id}");
        let lookup = self.request::<RawUser, Value>("GET", &endpoint, None);
        let outcome = match tokio::time::timeout(deadline, lookup).await {
            Ok(result) => result,
            Err(_elapsed) => {
                warn!(
                    user_id,
                    timeout_secs = deadline.as_secs(),
                    "author name lookup timed out; falling back to the user id"
                );
                return None;
            }
        };
        match outcome {
            Ok(user) => Some(user.username),
            Err(err) => {
                let reason = match &err {
                    CoreError::Api { status, .. } => format!("status {status}"),
                    CoreError::Http(_) => "transport failure".to_string(),
                    _ => "decode failure".to_string(),
                };
                warn!(
                    user_id,
                    reason, "could not resolve author name; falling back to the user id"
                );
                None
            }
        }
    }

    /// Turn provider posts into messages, resolving each distinct
    /// author once for the whole batch.
    ///
    /// Order-preserving by contract: the output is in the same order as
    /// the input and this function never sorts. Callers own their
    /// ordering — search results stay in the provider's ranked order,
    /// channel reads sort chronologically.
    async fn hydrate_posts(&self, posts: Vec<RawPost>) -> Vec<Message> {
        let wanted: BTreeSet<String> = posts.iter().map(|p| p.user_id.clone()).collect();
        let authors = self.resolve_authors(&wanted).await;
        posts
            .into_iter()
            .map(|post| {
                let username = authors
                    .get(&post.user_id)
                    .cloned()
                    .unwrap_or_else(|| post.user_id.clone());
                let root_id = normalize_root_id(&post.root_id, &post.id);
                Message {
                    id: post.id,
                    user_id: post.user_id,
                    username,
                    message: post.message,
                    create_at: post.create_at,
                    root_id,
                }
            })
            .collect()
    }

    /// Chronological order with a deterministic tie-break, used by
    /// every channel-shaped read so equal timestamps do not shuffle
    /// between calls.
    fn sort_chronologically(messages: &mut [Message]) {
        messages.sort_by(|left, right| {
            left.create_at
                .cmp(&right.create_at)
                .then_with(|| left.id.cmp(&right.id))
        });
    }

    pub async fn read_channel(
        &self,
        channel_name: &str,
        since_minutes: u64,
        team: Option<&str>,
    ) -> Result<Vec<Message>, CoreError> {
        let channel_id = self.resolve_channel(channel_name, team).await?.channel_id;
        let since = minutes_ago_millis(since_minutes);
        let response: PostsEnvelope = self
            .request(
                "GET",
                &format!("/channels/{channel_id}/posts?since={since}&per_page=30"),
                None::<Value>,
            )
            .await?;
        let mut posts = self
            .hydrate_posts(response.posts.into_values().collect())
            .await;
        Self::sort_chronologically(&mut posts);
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
        self.posts_after_by_channel_id(&channel_id, after_post_id)
            .await
    }

    /// Page posts strictly after `after_post_id` on an already-resolved
    /// channel. Does **not** re-assert the anchor — callers that need
    /// binding proof must call `assert_post_in_channel` first (once).
    /// Used by PER-038 wait backfill and lag recovery so scans advance
    /// over noise rather than re-fetching a fixed latest-N window.
    pub async fn posts_after_by_channel_id(
        &self,
        channel_id: &str,
        after_post_id: &str,
    ) -> Result<Vec<Message>, CoreError> {
        let mut page = 0;
        let mut messages = Vec::new();

        loop {
            let response: PostsEnvelope = self
                .request(
                    "GET",
                    &format!(
                        "/channels/{channel_id}/posts?after={after_post_id}&page={page}&per_page=200"
                    ),
                    None::<Value>,
                )
                .await?;

            let mut page_messages = self
                .hydrate_posts(response.posts.into_values().collect())
                .await;

            if page_messages.is_empty() {
                break;
            }

            Self::sort_chronologically(&mut page_messages);
            let page_len = page_messages.len();
            messages.extend(page_messages);

            if page_len < 200 {
                break;
            }

            page += 1;
        }

        Self::sort_chronologically(&mut messages);

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
        let response: PostsEnvelope = self
            .request(
                "GET",
                &format!(
                    "/channels/{channel_id}/posts?since={since_millis}&per_page={CHANNEL_WINDOW_PAGE_SIZE}"
                ),
                None::<Value>,
            )
            .await?;
        let mut posts = self
            .hydrate_posts(response.posts.into_values().collect())
            .await;
        Self::sort_chronologically(&mut posts);
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
    /// `GET /api/v4/channels/{id}/pinned`. Pure read, no cursor side
    /// effects (mirrors the operator-facing pinned-as-context contract).
    /// Resolves via the γ hybrid resolver (PER-019); accepts
    /// `<team>/<channel>` syntax and the `--team` override.
    ///
    /// Endpoint shape note (2026-05-07 fix): the canonical MM v4 path
    /// is `/channels/{id}/pinned` — NOT `/pinned_posts`. The
    /// `_posts` suffix returns 404 against real Mattermost.
    /// Originally shipped in PER-023 with the wrong URL; the
    /// wiremock test mocked the same wrong URL so it didn't catch
    /// the live divergence. Prodmktg dogfooding flagged this
    /// 2026-05-07 in #repo-chanvoy-ops.
    pub async fn read_channel_pinned(
        &self,
        channel_name: &str,
        team: Option<&str>,
    ) -> Result<Vec<Message>, CoreError> {
        let channel_id = self.resolve_channel(channel_name, team).await?.channel_id;
        // A channel with nothing pinned comes back without a `posts`
        // key at all, which the envelope's defaulting handles.
        let response: PostsEnvelope = self
            .request(
                "GET",
                &format!("/channels/{channel_id}/pinned"),
                None::<Value>,
            )
            .await?;
        let mut posts = self
            .hydrate_posts(response.posts.into_values().collect())
            .await;
        Self::sort_chronologically(&mut posts);
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
        let response: PostsEnvelope = self
            .request(
                "GET",
                &format!("/channels/{channel_id}/posts?per_page={limit}"),
                None::<Value>,
            )
            .await?;
        let mut posts = self
            .hydrate_posts(response.posts.into_values().collect())
            .await;
        Self::sort_chronologically(&mut posts);
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
        let response: PostsEnvelope = self
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
        // then truncate to the operator's `--limit`. Hydration keeps
        // this order — search results must never be re-sorted, or the
        // operator loses the ranking they asked for.
        let limit = limit as usize;
        let mut ranked = Vec::with_capacity(response.order.len().min(limit));
        for id in response.order.iter().take(limit) {
            if let Some(raw) = response.posts.get(id) {
                ranked.push(raw.clone());
            }
        }
        let posts = self.hydrate_posts(ranked).await;

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
        self.react_by_id(&resolved, post_id, emoji).await
    }

    /// PER-035 terminal write: add the reaction on an already-resolved,
    /// already-verified channel. Split out of `add_reaction` so the
    /// daemon can run resolution + the `assert_post_in_channel` check on
    /// the *calling* identity, then route this write to a *reduced*
    /// identity for outside-team channels. The reaction is bound to
    /// **this** client's user (`self.whoami()`), so calling it on the
    /// family client posts the reaction under the family bot — exactly
    /// the reduction contract. When `self` is the calling client (no
    /// reduction), behavior is identical to the pre-split `add_reaction`.
    pub async fn react_by_id(
        &self,
        resolved: &ResolvedChannel,
        post_id: &str,
        emoji: &str,
    ) -> Result<ReactionResult, CoreError> {
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
            team: resolved.team_name.clone(),
            channel: resolved.channel_name.clone(),
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
        self.unreact_by_id(&resolved, post_id, emoji).await
    }

    /// PER-035 terminal write: remove the reaction on an
    /// already-resolved, already-verified channel. The DELETE is scoped
    /// to **this** client's user, so reducing to the family client
    /// removes the family bot's reaction. Symmetric to `react_by_id`.
    pub async fn unreact_by_id(
        &self,
        resolved: &ResolvedChannel,
        post_id: &str,
        emoji: &str,
    ) -> Result<ReactionResult, CoreError> {
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
            team: resolved.team_name.clone(),
            channel: resolved.channel_name.clone(),
            post_id: post_id.to_string(),
            emoji: normalized,
            ok: true,
        })
    }

    /// PER-034: pin a post under the bot's identity. Validation
    /// order mirrors `add_reaction`: resolve channel → assert post
    /// in resolved channel → write. The post-fetch step also returns
    /// the current `is_pinned` value so the result surfaces
    /// `was_already_pinned` with no extra round-trips.
    ///
    /// Idempotent: re-pinning a pinned post issues the POST anyway
    /// and accepts MM's success response (MM v4 returns 200 on
    /// already-pinned posts). The operator-facing contract is "this
    /// post is pinned after this call returns"; `was_already_pinned`
    /// surfaces the prior state for callers who care.
    pub async fn pin_post(
        &self,
        channel_name: &str,
        post_id: &str,
        team: Option<&str>,
    ) -> Result<PinResult, CoreError> {
        let resolved = self.resolve_channel(channel_name, team).await?;
        let was_already_pinned = self
            .fetch_post_pinned_state(&resolved.channel_id, &resolved.channel_name, post_id)
            .await?;
        self.pin_by_id(&resolved, post_id, was_already_pinned).await
    }

    /// PER-035 terminal write: pin on an already-resolved,
    /// already-pin-state-read channel. The pin write lands under
    /// **this** client's token, so reducing to the family client pins
    /// as the family bot. `was_already_pinned` is the pre-read taken on
    /// the calling client and threaded through for the result's
    /// idempotency field. The 403 → `PinPermissionDenied` normalization
    /// names *this* client's bot (the identity that actually attempted
    /// the write).
    pub async fn pin_by_id(
        &self,
        resolved: &ResolvedChannel,
        post_id: &str,
        was_already_pinned: bool,
    ) -> Result<PinResult, CoreError> {
        match self
            .request::<Value, Value>("POST", &format!("/posts/{post_id}/pin"), None)
            .await
        {
            Ok(_) => {}
            // PER-034 AC #8: MM 403 on the pin write means the bot
            // lacks channel-admin. Normalize into a verb-specific
            // diagnostic naming the missing permission.
            Err(CoreError::Api {
                status: StatusCode::FORBIDDEN,
                message,
            }) => {
                let bot_username = self.whoami().await.map(|i| i.username).unwrap_or_default();
                return Err(CoreError::PinPermissionDenied {
                    verb: "pin",
                    bot_username,
                    team: resolved.team_name.clone(),
                    channel: resolved.channel_name.clone(),
                    message,
                });
            }
            Err(other) => return Err(other),
        }
        Ok(PinResult {
            verb: "pin".to_string(),
            team: resolved.team_name.clone(),
            channel: resolved.channel_name.clone(),
            channel_id: resolved.channel_id.clone(),
            post_id: post_id.to_string(),
            result: "pinned".to_string(),
            was_already_pinned,
            ok: true,
        })
    }

    /// PER-034: unpin a post. Symmetric to `pin_post`. Idempotent
    /// on already-unpinned (MM returns 200 either way). 403 from the
    /// write surfaces via `CoreError::PinPermissionDenied` with
    /// `verb: "unpin"`.
    pub async fn unpin_post(
        &self,
        channel_name: &str,
        post_id: &str,
        team: Option<&str>,
    ) -> Result<UnpinResult, CoreError> {
        let resolved = self.resolve_channel(channel_name, team).await?;
        let was_pinned = self
            .fetch_post_pinned_state(&resolved.channel_id, &resolved.channel_name, post_id)
            .await?;
        self.unpin_by_id(&resolved, post_id, was_pinned).await
    }

    /// PER-035 terminal write: unpin on an already-resolved,
    /// already-pin-state-read channel. Symmetric to `pin_by_id`;
    /// reduces identity the same way.
    pub async fn unpin_by_id(
        &self,
        resolved: &ResolvedChannel,
        post_id: &str,
        was_pinned: bool,
    ) -> Result<UnpinResult, CoreError> {
        match self
            .request::<Value, Value>("POST", &format!("/posts/{post_id}/unpin"), None)
            .await
        {
            Ok(_) => {}
            Err(CoreError::Api {
                status: StatusCode::FORBIDDEN,
                message,
            }) => {
                let bot_username = self.whoami().await.map(|i| i.username).unwrap_or_default();
                return Err(CoreError::PinPermissionDenied {
                    verb: "unpin",
                    bot_username,
                    team: resolved.team_name.clone(),
                    channel: resolved.channel_name.clone(),
                    message,
                });
            }
            Err(other) => return Err(other),
        }
        Ok(UnpinResult {
            verb: "unpin".to_string(),
            team: resolved.team_name.clone(),
            channel: resolved.channel_name.clone(),
            channel_id: resolved.channel_id.clone(),
            post_id: post_id.to_string(),
            result: "unpinned".to_string(),
            was_already_unpinned: !was_pinned,
            ok: true,
        })
    }

    /// Removed: an unbound thread read.
    ///
    /// Retained as an exported symbol so that a *call* to it still
    /// builds, with a deprecation warning rather than a hard compile
    /// error. That does not make the release a drop-in recompile:
    /// `CoreError` gained a variant in the same change, so code that
    /// matches exhaustively on it still has to add a wildcard arm
    /// before it will build. It always refuses: silently
    /// forwarding to the bound read would require inventing a channel,
    /// which would recreate the unscoped read this was removed for.
    #[deprecated(
        since = "0.3.0",
        note = "use read_thread_in_channel: a thread read must be scoped to the channel it was requested in"
    )]
    pub async fn read_thread(&self, _root_post_id: &str) -> Result<Vec<Message>, CoreError> {
        Err(CoreError::UnboundThreadReadRemoved)
    }

    pub async fn read_thread_in_channel(
        &self,
        expected_channel_id: &str,
        channel_name: &str,
        root_post_id: &str,
    ) -> Result<Vec<Message>, CoreError> {
        let response: PostsEnvelope = self
            .request(
                "GET",
                &format!("/posts/{root_post_id}/thread"),
                None::<Value>,
            )
            .await?;
        // Every refusal below names the id the CALLER supplied, never an
        // id the provider returned. A stray post's id is not the caller's
        // to learn: echoing it back would disclose the existence and
        // identity of a post outside the channel they named, which is the
        // narrower form of the existence oracle this binding prevents.
        let refuse = |channel_name: &str| CoreError::AnchorChannelMismatch {
            post_id: root_post_id.to_string(),
            channel: channel_name.to_string(),
        };
        if response.posts.is_empty() {
            return Err(CoreError::EmptyThread {
                root_id: root_post_id.to_string(),
            });
        }
        // Validate against the keyed map, not a bare list of values.
        //
        // The key is the provider's own claim about a post's id. Throwing
        // the keys away first means two distinct keys can carry the same
        // post id, and the checks below — which key off the id — then see
        // one canonical root and wave its duplicate through as a second
        // root. Disagreement between a key and the post it holds means the
        // response is not what it says it is.
        for (key, post) in &response.posts {
            if key != &post.id {
                return Err(refuse(channel_name));
            }
        }
        let mut raw: Vec<RawPost> = response.posts.into_values().collect();
        // Bind every post the provider returned, not only the anchor the
        // caller was checked against.
        //
        // The bot's credential can see many channels; the channel the
        // caller named is the narrower scope, and it is the one that
        // governs this read. Nothing downstream can re-check it: the
        // channel is dropped on the way into a message, and the peer
        // surface stamps every result with the channel the caller asked
        // for. Verifying here is what makes that label true rather than
        // merely plausible. Costs no extra request.
        for post in &raw {
            if !binding_holds(&post.channel_id, expected_channel_id) {
                return Err(refuse(channel_name));
            }
        }

        // Same channel is not the same conversation. Prove the envelope
        // really is the thread that was asked for, before any of it is
        // hydrated or returned:
        //
        //   - the requested post is present at all;
        //   - it is the thread's root, not a reply that happens to sit
        //     in the envelope;
        //   - every other post names that root as its own.
        //
        // Without this an in-channel envelope missing the requested root,
        // or carrying a post from a different conversation, is returned
        // and labelled as the requested thread — and `--latest` can then
        // select a post that was never part of it.
        // Exactly one record may claim to be the requested post, and it
        // must be top-level. The provider sends an empty root on a
        // top-level post; a record naming itself as its own root is not a
        // shape the provider produces, so accepting it would only ever
        // admit something malformed.
        let mut roots = raw.iter().filter(|post| post.id == root_post_id);
        let root = roots.next().ok_or_else(|| refuse(channel_name))?;
        if roots.next().is_some() {
            return Err(refuse(channel_name));
        }
        if !root.root_id.is_empty() {
            return Err(refuse(channel_name));
        }
        for post in &raw {
            if post.id == root_post_id {
                continue;
            }
            if post.root_id != root_post_id {
                return Err(refuse(channel_name));
            }
        }
        // Root first, then replies by timestamp, tie-broken by id so the
        // order is stable across calls.
        //
        // The root is pinned explicitly rather than left to fall out of
        // the timestamp comparison. Sorting on time alone gets the root
        // first only because it is normally the oldest post; a reply
        // written in the same millisecond whose id sorts lower would
        // displace it, and "the first item is the root" is a property
        // callers rely on.
        //
        // Deliberately NOT the envelope's `order` array: that array is
        // not guaranteed to be chronological, and the contract here is
        // root-first chronological. Do not "fix" this back to `order`.
        raw.sort_by(|left, right| {
            let left_is_root = left.id == root_post_id;
            let right_is_root = right.id == root_post_id;
            right_is_root
                .cmp(&left_is_root)
                .then_with(|| left.create_at.cmp(&right.create_at))
                .then_with(|| left.id.cmp(&right.id))
        });
        Ok(self.hydrate_posts(raw).await)
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
        let response: PostsEnvelope = self
            .request(
                "GET",
                &format!("/channels/{channel_id}/posts?per_page={per_page}"),
                None::<Value>,
            )
            .await?;
        let mut posts = self
            .hydrate_posts(response.posts.into_values().collect())
            .await;
        Self::sort_chronologically(&mut posts);
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

    /// PER-035: `pub` so the daemon can run the pre-write
    /// existence/channel-binding check on the **calling** profile's
    /// client (resolution + verification stay with the caller) before
    /// routing the terminal write to a possibly-reduced identity.
    pub async fn assert_post_in_channel(
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

    /// Fetch one post, bound to the channel the caller named.
    ///
    /// The binding is the point of this call: the channel comparison
    /// runs on the decoded post **before** anything is hydrated or
    /// returned, so a post that lives somewhere else never yields a
    /// body to the caller. Same two refusals as
    /// `assert_post_in_channel` — a missing post is `AnchorNotFound`,
    /// a post in another channel is `AnchorChannelMismatch`.
    ///
    /// One round-trip. Hydration goes through the shared
    /// `hydrate_posts` path so the author name and the thread-root
    /// normalization are identical to every other read.
    pub async fn get_post_in_channel(
        &self,
        expected_channel_id: &str,
        channel_name: &str,
        post_id: &str,
    ) -> Result<Message, CoreError> {
        let post: RawPost = match self
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

        // The provider was asked for one specific post. A response
        // carrying a different one is not an answer to the question, and
        // trusting it would let a malformed reply substitute someone
        // else's post: `show` would return that post's body under a
        // successful fetch, and a thread read would take its root and
        // fetch an entirely different conversation. Checked before the
        // channel, before hydration, before anything is believed.
        if post.id != post_id {
            return Err(CoreError::AnchorChannelMismatch {
                post_id: post_id.to_string(),
                channel: channel_name.to_string(),
            });
        }
        if !binding_holds(&post.channel_id, expected_channel_id) {
            return Err(CoreError::AnchorChannelMismatch {
                post_id: post_id.to_string(),
                channel: channel_name.to_string(),
            });
        }

        let mut hydrated = self.hydrate_posts(vec![post]).await;
        // `hydrate_posts` is one-for-one and order-preserving, so the
        // single input post is always here.
        hydrated
            .pop()
            .ok_or_else(|| CoreError::AnchorNotFound(post_id.to_string()))
    }

    /// PER-034: like `assert_post_in_channel`, but returns the
    /// `is_pinned` field of the post so callers can surface the
    /// pre-call pin state without a second round-trip. The
    /// channel-mismatch and not-found error paths match
    /// `assert_post_in_channel` exactly.
    ///
    /// PER-035: `pub` for the same reason as `assert_post_in_channel` —
    /// the pin-state pre-read runs on the calling client; only the
    /// terminal pin/unpin write reduces.
    pub async fn fetch_post_pinned_state(
        &self,
        expected_channel_id: &str,
        channel_name: &str,
        post_id: &str,
    ) -> Result<bool, CoreError> {
        #[derive(Deserialize)]
        struct PostResponse {
            channel_id: String,
            #[serde(default)]
            is_pinned: bool,
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

        Ok(post.is_pinned)
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
    /// The websocket credential is taken from `client` rather than
    /// accepted as a parameter, so the websocket and the
    /// request-response surface cannot be handed different identities.
    pub fn new(
        profile: &Profile,
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
            token: client.token().to_string(),
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
        let raw_root_id = post
            .get("root_id")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();

        if sender_id == self.my_user_id {
            return;
        }

        if post_id.is_empty() || channel_id.is_empty() {
            return;
        }

        let root_id = normalize_root_id(&raw_root_id, &post_id);

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
                    root_id,
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
                    root_id,
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
                        // Already normalized by the read path.
                        root_id: msg.root_id,
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

    // TODO: this fetches the bot's entire channel list on every inbound
    // event just to turn one channel id into a name. It should go
    // through a shared, bounded channel-name cache on the client the
    // same way author names now do.
    async fn resolve_channel_name(&self, channel_id: &str) -> String {
        let channels = self.client.list_channels().await.unwrap_or_default();
        channels
            .iter()
            .find(|c| c.id == channel_id)
            .map(|c| c.name.clone())
            .unwrap_or_else(|| channel_id.to_string())
    }

    /// Author name for an inbound push event. Delegates to the shared
    /// client helper so pushed messages and messages read back over
    /// REST can never disagree about who wrote something.
    async fn resolve_username(&self, user_id: &str) -> String {
        self.client.author_username(user_id).await
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

    // ------------------------------------------------------------------
    // Thread-root normalization. One rule, shared by the read path and
    // the push path, so a caller can always use the reported root as a
    // reply target.
    // ------------------------------------------------------------------

    #[test]
    fn top_level_post_is_its_own_thread_root() {
        assert_eq!(normalize_root_id("", "post-1"), "post-1");
    }

    #[test]
    fn a_reply_keeps_the_thread_root_and_does_not_claim_to_be_one() {
        // The regression this guards: reporting a reply's own id as its
        // root. That reads as a valid reply target but the provider
        // rejects a reply aimed at a reply, so the failure surfaces far
        // from here — at write time, in the caller.
        assert_eq!(normalize_root_id("root-1", "reply-9"), "root-1");
        assert_ne!(normalize_root_id("root-1", "reply-9"), "reply-9");
    }

    // ------------------------------------------------------------------
    // Chronological ordering and its tie rule.
    //
    // The tie-break cannot be observed through a single provider
    // response: those arrive in a map keyed by post id, so equally
    // timestamped posts are already in ascending id order and a stable
    // sort leaves them there. It becomes observable when results from
    // more than one page are merged, which is why it is exercised
    // against the sort directly rather than through a mocked read.
    // ------------------------------------------------------------------

    fn message_at(id: &str, create_at: i64) -> Message {
        Message {
            id: id.to_string(),
            user_id: "u".to_string(),
            username: "alice".to_string(),
            message: "body".to_string(),
            create_at,
            root_id: id.to_string(),
        }
    }

    #[test]
    fn equal_timestamps_are_broken_by_id_not_by_arrival_order() {
        let mut messages = vec![
            message_at("zeta", 1_000),
            message_at("alpha", 1_000),
            message_at("beta", 1_000),
        ];
        MattermostClient::sort_chronologically(&mut messages);
        assert_eq!(
            messages.iter().map(|m| m.id.as_str()).collect::<Vec<_>>(),
            vec!["alpha", "beta", "zeta"],
            "posts sharing a timestamp order by id, so a merge of two \
             pages cannot shuffle between calls"
        );
    }

    #[test]
    fn ordering_is_by_time_first_and_id_only_as_a_tie_break() {
        let mut messages = vec![
            message_at("alpha", 3_000),
            message_at("zeta", 1_000),
            message_at("beta", 2_000),
        ];
        MattermostClient::sort_chronologically(&mut messages);
        assert_eq!(
            messages.iter().map(|m| m.id.as_str()).collect::<Vec<_>>(),
            vec!["zeta", "beta", "alpha"],
            "time wins over id; id is only consulted for ties"
        );
    }

    // ------------------------------------------------------------------
    // Author-cache bounds. Exercised against the insertion helper
    // directly so the eviction rules can be checked without a clock or
    // a network.
    // ------------------------------------------------------------------

    fn author_cache_of(
        entries: &[(&str, std::time::Instant)],
    ) -> HashMap<String, AuthorCacheEntry> {
        entries
            .iter()
            .map(|(user_id, fetched_at)| {
                (
                    (*user_id).to_string(),
                    AuthorCacheEntry {
                        username: format!("name-of-{user_id}"),
                        fetched_at: *fetched_at,
                    },
                )
            })
            .collect()
    }

    #[test]
    fn author_cache_never_grows_past_its_bound() {
        let base = std::time::Instant::now();
        let mut cache = HashMap::new();
        for index in 0..(AUTHOR_CACHE_MAX_ENTRIES * 2) {
            insert_author_entry(
                &mut cache,
                format!("user-{index}"),
                format!("name-{index}"),
                base + Duration::from_millis(index as u64),
            );
            assert!(
                cache.len() <= AUTHOR_CACHE_MAX_ENTRIES,
                "cache exceeded its bound at insert {index}: {}",
                cache.len()
            );
        }
    }

    #[test]
    fn author_cache_drops_expired_entries_before_live_ones() {
        let base = std::time::Instant::now();
        let stale_at = base;
        let fresh_at = base + AUTHOR_CACHE_TTL;
        let mut entries: Vec<(String, std::time::Instant)> = Vec::new();
        for index in 0..(AUTHOR_CACHE_MAX_ENTRIES - 1) {
            entries.push((format!("stale-{index}"), stale_at));
        }
        entries.push(("still-fresh".to_string(), fresh_at));
        let borrowed: Vec<(&str, std::time::Instant)> =
            entries.iter().map(|(id, at)| (id.as_str(), *at)).collect();
        let mut cache = author_cache_of(&borrowed);
        assert_eq!(cache.len(), AUTHOR_CACHE_MAX_ENTRIES);

        // Now is past the TTL for every stale entry but not for the
        // fresh one.
        let now = fresh_at + Duration::from_secs(1);
        insert_author_entry(&mut cache, "newcomer".to_string(), "new".to_string(), now);

        assert!(cache.contains_key("newcomer"));
        assert!(
            cache.contains_key("still-fresh"),
            "a live entry must not be evicted while expired ones remain"
        );
        assert!(
            !cache.contains_key("stale-0"),
            "expired entries are the first to go"
        );
    }

    #[test]
    fn author_cache_drops_the_oldest_when_nothing_has_expired() {
        let base = std::time::Instant::now();
        let entries: Vec<(String, std::time::Instant)> = (0..AUTHOR_CACHE_MAX_ENTRIES)
            .map(|index| {
                (
                    format!("user-{index}"),
                    base + Duration::from_millis(index as u64),
                )
            })
            .collect();
        let borrowed: Vec<(&str, std::time::Instant)> =
            entries.iter().map(|(id, at)| (id.as_str(), *at)).collect();
        let mut cache = author_cache_of(&borrowed);

        let now = base + Duration::from_millis(AUTHOR_CACHE_MAX_ENTRIES as u64);
        insert_author_entry(&mut cache, "newcomer".to_string(), "new".to_string(), now);

        assert_eq!(cache.len(), AUTHOR_CACHE_MAX_ENTRIES);
        assert!(cache.contains_key("newcomer"));
        assert!(
            !cache.contains_key("user-0"),
            "the oldest entry is the one evicted"
        );
        assert!(cache.contains_key("user-1"), "newer entries are retained");
    }

    #[test]
    fn refreshing_a_cached_author_does_not_evict_anything() {
        let base = std::time::Instant::now();
        let entries: Vec<(String, std::time::Instant)> = (0..AUTHOR_CACHE_MAX_ENTRIES)
            .map(|index| (format!("user-{index}"), base))
            .collect();
        let borrowed: Vec<(&str, std::time::Instant)> =
            entries.iter().map(|(id, at)| (id.as_str(), *at)).collect();
        let mut cache = author_cache_of(&borrowed);

        insert_author_entry(
            &mut cache,
            "user-0".to_string(),
            "renamed".to_string(),
            base + Duration::from_millis(1),
        );

        assert_eq!(cache.len(), AUTHOR_CACHE_MAX_ENTRIES);
        assert_eq!(cache["user-0"].username, "renamed");
    }

    #[test]
    fn parses_env_file_lines() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("mm.env");
        fs::write(
            &path,
            "export LANYTE_MM_TOKEN=\"secret\"\n# comment\nOTHER=value\n",
        )
        .unwrap();
        // PER-036A / ADR-0016: env-files are credential material and must be
        // owner-only; the credential reader refuses group/world-accessible
        // modes, so a realistic test fixture is chmod 600.
        fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
        let values = parse_env_file(&path).unwrap();
        assert_eq!(values.get("LANYTE_MM_TOKEN"), Some(&"secret".to_string()));
        assert_eq!(values.get("OTHER"), Some(&"value".to_string()));
    }

    #[cfg(unix)]
    fn env_file_profile(env_file: &Path) -> Profile {
        Profile {
            name: "cred-test".to_string(),
            role: "test".to_string(),
            scope: "test".to_string(),
            provider: Provider::Mattermost,
            bot_username: String::new(),
            team_name: "org-test".to_string(),
            server_url: "https://mm.example.com".to_string(),
            env_name: "LANYTE_MM_TOKEN".to_string(),
            env_file: Some(env_file.to_path_buf()),
            credential_mode: CredentialMode::EnvFile,
            capability_class: CapabilityClass::Standard,
            monitored_channels: Vec::new(),
            ipc: None,
            reduce: None,
        }
    }

    #[cfg(unix)]
    #[test]
    fn load_token_reads_owner_only_env_file() {
        // PER-036A: daemon token loading happy path — a 0600 env-file loads.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("mm.env");
        fs::write(&path, "LANYTE_MM_TOKEN=tok-123\n").unwrap();
        fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
        let token = load_token(&env_file_profile(&path)).unwrap();
        assert_eq!(token, "tok-123");
    }

    #[cfg(unix)]
    #[test]
    fn load_token_refuses_loose_permission_env_file() {
        // PER-036A / ADR-0016 A2: a group/world-accessible credential file
        // is refused before parsing — a token leak even when not symlinked.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("loose.env");
        fs::write(&path, "LANYTE_MM_TOKEN=tok-123\n").unwrap();
        fs::set_permissions(&path, fs::Permissions::from_mode(0o644)).unwrap();
        let err = load_token(&env_file_profile(&path)).expect_err("0644 env-file must be refused");
        assert!(
            matches!(
                err,
                CoreError::SafeRead(SafeReadError::LoosePermissions { .. })
            ),
            "expected LoosePermissions, got {err:?}"
        );
    }

    #[cfg(unix)]
    #[test]
    fn load_token_refuses_symlinked_env_file() {
        // PER-036A / ADR-0016 A2: credential files fail closed on a
        // symlinked final component (pass the resolved path).
        let dir = tempfile::tempdir().unwrap();
        let target = dir.path().join("real.env");
        fs::write(&target, "LANYTE_MM_TOKEN=tok-123\n").unwrap();
        fs::set_permissions(&target, fs::Permissions::from_mode(0o600)).unwrap();
        let link = dir.path().join("link.env");
        std::os::unix::fs::symlink(&target, &link).unwrap();
        let err = load_token(&env_file_profile(&link)).expect_err("symlinked env-file must refuse");
        assert!(
            matches!(err, CoreError::SafeRead(SafeReadError::Symlink { .. })),
            "expected Symlink, got {err:?}"
        );
    }

    // ---- PER-035: identity-reduction pure helpers ----

    #[test]
    fn identity_reduces_only_outside_team() {
        // In-team write keeps the calling identity; outside-team reduces.
        assert!(!identity_reduces("org-acme", "org-acme"));
        assert!(identity_reduces("org-acme", "org-3leaps"));
        assert!(identity_reduces("org-acme", "org-fulmenhq"));
    }

    #[test]
    fn provenance_tags_name_both_paths_independently() {
        // Primary-team channel, no reduction → no tags.
        assert!(posting_provenance_tags(ResolutionSource::Primary, false).is_empty());
        // PER-019 channel team-fallback only.
        assert_eq!(
            posting_provenance_tags(ResolutionSource::Fallback, false),
            vec!["team-fallback"]
        );
        // PER-035 identity reduction only (channel resolved on primary).
        assert_eq!(
            posting_provenance_tags(ResolutionSource::Primary, true),
            vec!["identity-reduce"]
        );
        // Both apply on one call → both named, independently and in order.
        assert_eq!(
            posting_provenance_tags(ResolutionSource::Fallback, true),
            vec!["team-fallback", "identity-reduce"]
        );
        // Explicit `<team>/<channel>` resolution is not a fallback, so it
        // contributes no channel-resolution tag even when identity reduces.
        assert_eq!(
            posting_provenance_tags(ResolutionSource::Explicit, true),
            vec!["identity-reduce"]
        );
    }

    #[test]
    fn reduce_policy_toml_round_trips_and_defaults_absent() {
        // A profile with no `[reduce]` table parses to `reduce: None`
        // (back-compat: existing on-disk profiles predate the field).
        let without = "\
name = \"dataeng-galaxy\"
role = \"dataeng\"
scope = \"galaxy\"
provider = \"mattermost\"
bot_username = \"agent-dataeng-blue\"
team_name = \"org-3leaps\"
server_url = \"https://mm.example.com\"
env_name = \"LANYTE_MM_TOKEN\"
";
        let parsed: Profile = toml::from_str(without).unwrap();
        assert!(parsed.reduce.is_none());

        // A `[reduce]` table parses into the policy, and re-serializes
        // back to the same table (round-trip stable).
        let with = format!("{without}\n[reduce]\nuse_profile = \"dataeng-galaxy\"\n");
        let parsed: Profile = toml::from_str(&with).unwrap();
        assert_eq!(
            parsed.reduce.as_ref().map(|r| r.use_profile.as_str()),
            Some("dataeng-galaxy")
        );
        let reserialized = toml::to_string_pretty(&parsed).unwrap();
        assert!(
            reserialized.contains("[reduce]"),
            "reduce table must survive re-serialization; got:\n{reserialized}"
        );
        assert!(reserialized.contains("use_profile = \"dataeng-galaxy\""));
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

    #[cfg(unix)]
    #[test]
    fn list_profiles_routes_through_safe_reader() {
        // PER-036A / ADR-0016 (devrev PR #39 #1): list_profiles must read each
        // profile TOML via the tool-owned safe reader, so a non-regular entry
        // (here a directory named like a profile) is refused, not blindly
        // read. Proves the resolver/collection input is covered, not just
        // load_profile.
        let _guard = CONFIG_ENV_LOCK.lock().unwrap();
        let config = tempfile::tempdir().unwrap();
        unsafe { env::set_var("CHANVOY_CONFIG_DIR", config.path()) };

        let profiles_dir = config.path().join("profiles");
        fs::create_dir_all(&profiles_dir).unwrap();
        fs::set_permissions(&profiles_dir, fs::Permissions::from_mode(0o700)).unwrap();
        // A valid profile lists fine on its own.
        let good = "name = \"good\"\nrole = \"r\"\nscope = \"s\"\nprovider = \"mattermost\"\n\
                    bot_username = \"agent-good\"\nteam_name = \"org-s\"\n\
                    server_url = \"https://mm.example.com\"\nenv_name = \"LANYTE_MM_TOKEN\"\n";
        fs::write(profiles_dir.join("good.toml"), good).unwrap();
        // A directory whose name ends in `.toml` is a non-regular "profile".
        fs::create_dir(profiles_dir.join("evil.toml")).unwrap();

        let result = list_profiles();
        unsafe { env::remove_var("CHANVOY_CONFIG_DIR") };

        match result {
            Err(CoreError::SafeRead(SafeReadError::NonRegular { .. })) => {}
            other => panic!("expected NonRegular refusal from list_profiles, got {other:?}"),
        }
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
                root_id: "post456".to_string(),
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
                root_id: "p1".to_string(),
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
                root_id: "root-p0".to_string(),
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
                reduce: None,
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
                reduce: None,
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
    /// The websocket parser boundary, driven with provider-shaped
    /// `posted` payloads.
    ///
    /// These exist because the earlier regressions all started after
    /// parsing, from an already-correct payload — so none of them could
    /// catch the parser failing to read the provider's root, which is
    /// exactly the defect that occurred. The assertions here are on what
    /// comes out of the event bus, so the parse is genuinely covered.
    /// Author-cache behavior through the client, rather than arithmetic
    /// on the insertion helper. An expired entry has to actually cause a
    /// refresh, and a stalled provider has to actually reach the
    /// fallback — neither is observable from the helper alone.
    mod author_resolution {
        use super::*;
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        fn seat_profile(server_url: &str) -> Profile {
            Profile {
                name: "seat".to_string(),
                role: "seat".to_string(),
                scope: "scope".to_string(),
                provider: Provider::Mattermost,
                bot_username: "agent-seat".to_string(),
                team_name: "org-team".to_string(),
                server_url: server_url.to_string(),
                env_name: "LANYTE_MM_TOKEN".to_string(),
                env_file: None,
                credential_mode: CredentialMode::EnvName,
                capability_class: CapabilityClass::Standard,
                monitored_channels: Vec::new(),
                ipc: None,
                reduce: None,
            }
        }

        /// A cached name is re-fetched once its entry has aged out, and
        /// the caller sees the new name — a rename becomes visible
        /// without restarting the daemon.
        #[tokio::test]
        async fn an_expired_entry_is_refreshed_from_the_provider() {
            let server = MockServer::start().await;
            Mock::given(method("GET"))
                .and(path("/api/v4/users/u-1"))
                .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                    "id": "u-1", "username": "renamed"
                })))
                .mount(&server)
                .await;
            let profile = seat_profile(&server.uri());
            let client = MattermostClient::new(&profile, "t".to_string()).unwrap();

            // Seed an entry that is already past its useful life.
            {
                let mut guard = client.author_cache.write().await;
                guard.insert(
                    "u-1".to_string(),
                    AuthorCacheEntry {
                        username: "stale-name".to_string(),
                        fetched_at: std::time::Instant::now() - AUTHOR_CACHE_TTL,
                    },
                );
            }

            let name = client.author_username("u-1").await;
            assert_eq!(
                name, "renamed",
                "an aged-out entry must be refreshed, not served stale"
            );
            assert_eq!(
                server.received_requests().await.unwrap().len(),
                1,
                "exactly one refresh for the expired entry"
            );

            // The refreshed value is now cached: no second request.
            let again = client.author_username("u-1").await;
            assert_eq!(again, "renamed");
            assert_eq!(
                server.received_requests().await.unwrap().len(),
                1,
                "the refreshed entry serves the next call from cache"
            );
        }

        /// A provider that accepts the connection and then stalls must
        /// still reach the user-id fallback.
        ///
        /// This is the case the refusal-shaped tests cannot reach: a
        /// connection-refused or a 500 returns promptly, so the fallback
        /// was only ever proven for a provider that fails politely. An
        /// accepted-and-silent connection would otherwise hang every
        /// read and wait that hydrates a message.
        #[tokio::test]
        async fn a_stalled_lookup_falls_back_to_the_user_id() {
            let server = MockServer::start().await;
            // Accepted, then silent for far longer than the deadline.
            Mock::given(method("GET"))
                .and(path("/api/v4/users/u-1"))
                .respond_with(
                    ResponseTemplate::new(200)
                        .set_delay(Duration::from_secs(30))
                        .set_body_json(serde_json::json!({"id": "u-1", "username": "never"})),
                )
                .mount(&server)
                .await;
            let profile = seat_profile(&server.uri());
            let client = MattermostClient::new(&profile, "t".to_string()).unwrap();

            // A short deadline stands in for the production constant so
            // the test proves the elapsed path in milliseconds.
            let started = std::time::Instant::now();
            let resolved = client
                .fetch_username_within("u-1", Duration::from_millis(50))
                .await;
            assert!(
                resolved.is_none(),
                "a stalled lookup must give up rather than wait for a response that is not coming"
            );
            assert!(
                started.elapsed() < Duration::from_secs(5),
                "the deadline must actually bound the wait"
            );
        }

        /// The production constant is the one actually applied, so the
        /// test seam above cannot drift away from real behavior.
        #[test]
        fn the_author_deadline_is_bounded_and_short() {
            assert!(
                AUTHOR_RESOLVE_TIMEOUT <= Duration::from_secs(10),
                "author resolution is a courtesy on top of a read; it must not \
                 dominate the read's latency"
            );
        }
    }

    mod push_parser {
        use super::*;
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        fn ws_profile(server_url: &str) -> Profile {
            Profile {
                name: "seat".to_string(),
                role: "seat".to_string(),
                scope: "scope".to_string(),
                provider: Provider::Mattermost,
                bot_username: "agent-seat".to_string(),
                team_name: "org-team".to_string(),
                server_url: server_url.to_string(),
                env_name: "LANYTE_MM_TOKEN".to_string(),
                env_file: None,
                credential_mode: CredentialMode::EnvName,
                capability_class: CapabilityClass::Standard,
                monitored_channels: vec!["general".to_string()],
                ipc: None,
                reduce: None,
            }
        }

        async fn mock_channel_and_user(server: &MockServer) {
            Mock::given(method("GET"))
                .and(path("/api/v4/teams/name/org-team"))
                .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                    "id": "team-1", "name": "org-team"
                })))
                .mount(server)
                .await;
            Mock::given(method("GET"))
                .and(path("/api/v4/users/me/teams/team-1/channels"))
                .respond_with(
                    ResponseTemplate::new(200).set_body_json(serde_json::json!([{
                        "id": "ch-1", "name": "general", "display_name": "General",
                        "type": "O", "last_post_at": 0
                    }])),
                )
                .mount(server)
                .await;
            Mock::given(method("GET"))
                .and(path("/api/v4/users/u-sender"))
                .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                    "id": "u-sender", "username": "alice"
                })))
                .mount(server)
                .await;
        }

        /// Drive a reply-shaped payload through the parser and assert
        /// the provider's root survives to the emitted event.
        #[tokio::test]
        async fn a_pushed_reply_keeps_the_providers_thread_root() {
            let server = MockServer::start().await;
            mock_channel_and_user(&server).await;
            let profile = ws_profile(&server.uri());
            let client = MattermostClient::new(&profile, "t".to_string()).unwrap();
            let bus = Arc::new(EventBus::new(16));
            let mut rx = bus.subscribe();
            let ws = MattermostWs::new(&profile, client, Arc::clone(&bus), "u-me".to_string());

            // Provider shape: the post's own id differs from its root.
            let data = serde_json::json!({
                "post": serde_json::to_string(&serde_json::json!({
                    "id": "reply-9",
                    "channel_id": "ch-1",
                    "user_id": "u-sender",
                    "message": "a reply",
                    "create_at": 1_700_000_000_000i64,
                    "root_id": "root-1",
                }))
                .unwrap(),
            });
            ws.handle_post_event(&data).await;

            let event = tokio::time::timeout(Duration::from_secs(5), rx.recv())
                .await
                .expect("an event must be emitted; a silent parser would otherwise hang here")
                .expect("event bus delivered");
            let DaemonEventPayloadInner::Inbound(payload) = &event.payload else {
                panic!("expected an inbound payload");
            };
            assert_eq!(payload.post_id, "reply-9");
            assert_eq!(
                payload.root_id, "root-1",
                "the parser must read the provider's root, not the post's own id"
            );
            assert_eq!(payload.sender_username, "alice");
        }

        /// A top-level payload arrives with an empty provider root and
        /// must be normalized to its own id, so every pushed event names
        /// a usable reply target.
        #[tokio::test]
        async fn a_pushed_top_level_post_reports_itself_as_the_root() {
            let server = MockServer::start().await;
            mock_channel_and_user(&server).await;
            let profile = ws_profile(&server.uri());
            let client = MattermostClient::new(&profile, "t".to_string()).unwrap();
            let bus = Arc::new(EventBus::new(16));
            let mut rx = bus.subscribe();
            let ws = MattermostWs::new(&profile, client, Arc::clone(&bus), "u-me".to_string());

            let data = serde_json::json!({
                "post": serde_json::to_string(&serde_json::json!({
                    "id": "post-1",
                    "channel_id": "ch-1",
                    "user_id": "u-sender",
                    "message": "top level",
                    "create_at": 1_700_000_000_000i64,
                    "root_id": "",
                }))
                .unwrap(),
            });
            ws.handle_post_event(&data).await;

            let event = tokio::time::timeout(Duration::from_secs(5), rx.recv())
                .await
                .expect("an event must be emitted; a silent parser would otherwise hang here")
                .expect("event bus delivered");
            let DaemonEventPayloadInner::Inbound(payload) = &event.payload else {
                panic!("expected an inbound payload");
            };
            assert_eq!(payload.root_id, "post-1");
        }
    }

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
                reduce: None,
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
