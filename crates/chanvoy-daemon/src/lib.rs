use std::collections::HashMap;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::{env, fs, io};

use chanvoy_core::{
    daemon_event_to_notification, list_profiles, load_attention_state, load_profile, load_token,
    now_unix_millis, pid_path_for_profile, rpc_error, rpc_result, socket_path_for_profile,
    store_attention_state, AckChannelParams, AckResult, AddMemberParams, ArchiveChannelParams,
    AttentionShowParams, AttentionState, CapabilityClass, Channel, CheckChannelParams, CheckResult,
    CoreError, CreateChannelParams, DaemonEvent, DaemonEventKind, DaemonEventPayloadInner,
    DaemonHealth, DaemonStatus, DirectMessageParams, DmConversation, EventBus, GetPostParams,
    IpcConfig, JsonRpcNotification, JsonRpcRequest, JsonRpcResponse, MattermostClient,
    MattermostWs, NotificationsParams, NotifyParams, PinParams, PinResult, PinnedChannelParams,
    PostMessageParams, Profile, ProfileStatus, Provider, ReactParams, ReactionResult,
    ReadChannelParams, ReadDirectMessageParams, ReadThreadParams, SearchParams, SearchResult,
    ShutdownResult, SubscribeParams, SubscriptionAck, SubscriptionFilter, UnpinParams, UnpinResult,
    UnreactParams, UnreadNotifications, UnsubscribeParams, WaitChannelParams, WaitResult, WsState,
};
use chanvoy_ipc::{IpcPeer, IpcPeerState};
use serde::de::DeserializeOwned;
use serde::Serialize;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::{oneshot, Mutex};
use tokio::time::{sleep, timeout, Duration};
use tracing::{info, warn};

#[derive(Debug, thiserror::Error)]
pub enum DaemonError {
    #[error(transparent)]
    Core(#[from] CoreError),
    #[error("io error: {0}")]
    Io(#[from] io::Error),
    #[error("socket already running at {0}")]
    AlreadyRunning(String),
    /// Message reworded now that `Display` is what operators read: both
    /// construction sites pass a **socket path**, so calling it a profile was
    /// simply wrong. This is the string every field report of the
    /// start-then-vanish failure quoted, so it is worth being both accurate and
    /// actionable. Variant name and shape unchanged.
    #[error("no chanvoy daemon is listening at {0}; start one with `chanvoy --profile <name> daemon start`")]
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
    /// PER-014 drift floor. Set by the post-bind probe (and refreshed by
    /// every `daemon_status` call) when `whoami()` returns a username that
    /// does not match the configured `bot_username`. Network-backed RPCs
    /// inspect this and refuse with a clear diagnostic; the local socket
    /// stays bound so operators can query `daemon_status` to learn what's
    /// wrong.
    identity_drift: Arc<AtomicBool>,
    /// PER-035: the family-identity writer this profile reduces to, if a
    /// `[reduce]` policy is configured. `None` ⇒ no reduction; every
    /// write posts under `client`'s identity (today's behavior). Built
    /// once at `start()` from `profile.reduce.use_profile`; a missing
    /// target fails startup (never a silent bare-identity fallback).
    reduce_writer: Option<ReduceWriter>,
}

/// PER-035: a pre-built client bound to the family profile a stream
/// profile reduces to. Channel-targeted **writes** whose resolved
/// channel lives outside the calling profile's `team_name` route their
/// terminal MM call through this client; resolution and pre-write
/// verification always stay on the calling `AppState::client`.
#[derive(Clone)]
struct ReduceWriter {
    /// Family profile name (the reduce target). Surfaced in the
    /// startup log so operators can confirm the active reduction.
    profile_name: String,
    /// Family bot username, captured from the family profile so the
    /// audit log can name the posting identity without a startup whoami.
    bot_username: String,
    /// Client bound to the family profile's token.
    client: MattermostClient,
}

impl AppState {
    /// PER-035: pick the client that performs the terminal write for a
    /// channel that resolved into `resolved_team`. Reduces to the family
    /// identity iff a reduction policy is configured AND the channel
    /// lives outside this profile's `team_name` (the brief's §Scope
    /// rule, via the pure `identity_reduces` helper). Returns the chosen
    /// client, the bot username it posts as (audit logging), and whether
    /// reduction was applied. With no policy, or for an in-team channel,
    /// returns this profile's own client unchanged.
    fn select_writer(&self, resolved_team: &str) -> (&MattermostClient, &str, bool) {
        match &self.reduce_writer {
            Some(rw) if chanvoy_core::identity_reduces(&self.profile.team_name, resolved_team) => {
                (&rw.client, rw.bot_username.as_str(), true)
            }
            _ => (&self.client, self.profile.bot_username.as_str(), false),
        }
    }
}

/// PER-035: emit the audit-log line for a channel-targeted write,
/// naming the PER-019 channel-resolution provenance and the PER-035
/// posting-identity provenance independently (brief AC). `posting_identity`
/// is the bot the terminal write actually lands under.
fn log_posting_identity(
    verb: &str,
    state: &AppState,
    resolved: &chanvoy_core::ResolvedChannel,
    posting_identity: &str,
    reduced: bool,
) {
    let provenance = chanvoy_core::posting_provenance_tags(resolved.resolution_source, reduced);
    info!(
        verb,
        selected_profile = %state.profile.name,
        posting_identity = %posting_identity,
        channel = %resolved.channel_name,
        team = %resolved.team_name,
        channel_resolution = ?resolved.resolution_source,
        identity_reduced = reduced,
        provenance = ?provenance,
        "chanvoy write identity resolution"
    );
}

/// PER-035: build the family-identity writer for a profile that carries
/// a reduction policy. A missing reduce target is a loud
/// `ReduceProfileNotFound` (brief AC: negative case — never a silent
/// fall-back to the bare daemon identity). The family token is loaded
/// from the daemon's environment via the family profile's
/// `credential_mode`; an unresolvable token surfaces as a normal
/// token-load error so the operator sees exactly what is missing.
///
/// PER-035 (devrev PR #37 P1): the loaded token is **validated** with a
/// `whoami` against the family profile's expected `bot_username` before
/// the writer is trusted. Without this, a family profile that shares an
/// `env_name` with the stream profile (both default `LANYTE_MM_TOKEN`)
/// would load the *stream* token in a stream shell — every outside-team
/// write would post as the stream bot while the audit log claimed
/// family identity. The whoami-returned username is recorded as the
/// authoritative audit identity (so the log can never disagree with the
/// token that actually posts). This is a network call at startup, made
/// only for reduce-configured profiles; it fails closed (the daemon
/// refuses to start) on mismatch or unreachable identity endpoint,
/// because a stream daemon that cannot prove its family identity must
/// not bind — every reduced write would otherwise be a potential
/// identity leak.
async fn build_reduce_writer(
    calling: &Profile,
    policy: &chanvoy_core::ReducePolicy,
) -> Result<ReduceWriter, DaemonError> {
    let family = match load_profile(&policy.use_profile) {
        Ok(family) => family,
        Err(CoreError::ProfileNotFound(_)) => {
            let available = list_profiles()
                .map(|profiles| profiles.into_iter().map(|p| p.name).collect())
                .unwrap_or_default();
            return Err(CoreError::ReduceProfileNotFound {
                calling: calling.name.clone(),
                missing: policy.use_profile.clone(),
                available,
            }
            .into());
        }
        Err(other) => return Err(other.into()),
    };
    let token = load_token(&family)?;
    let client = MattermostClient::new(&family, token)?;
    let identity = client.whoami().await?;
    if !family.bot_username.is_empty() && identity.username != family.bot_username {
        return Err(CoreError::ReduceIdentityMismatch {
            profile: family.name.clone(),
            expected: family.bot_username.clone(),
            actual: identity.username,
        }
        .into());
    }
    Ok(ReduceWriter {
        profile_name: family.name.clone(),
        // Authoritative, whoami-verified identity — never the
        // potentially-stale stored value, so the audit log cannot
        // disagree with the token that actually posts.
        bot_username: identity.username,
        client,
    })
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
    // Loaded exactly once and reused for every surface this daemon
    // brings up. Reading it a second time later would let a rotation
    // between the two reads pair a request-response client
    // authenticated as one identity with a websocket authenticated by
    // another — and the drift probe only ever inspects the first.
    let token = load_token(&profile)?;
    let client = MattermostClient::new(&profile, token.clone())?;

    // PER-035: if this profile carries a reduction policy, build the
    // family-identity writer up-front. Loud failure on a missing target
    // (the brief's negative case) — we must not bind a daemon that would
    // silently post stream identity into the galaxy.
    let reduce_writer = match &profile.reduce {
        Some(policy) => {
            let writer = build_reduce_writer(&profile, policy).await?;
            info!(
                profile = profile_name,
                reduce_to = %writer.profile_name,
                reduce_identity = %writer.bot_username,
                "PER-035 reduction policy active: outside-team writes reduce to family identity"
            );
            Some(writer)
        }
        None => None,
    };

    // PER-014: three startup paths, distinguished by whether the parent
    // advertised a handoff via `CHANVOY_BOOTSTRAP_NONCE` and whether the
    // bootstrap-state file is present:
    //
    // 1. **File present**: validated bootstrap path. Validate
    //    freshness + profile_fingerprint + nonce-env match + username
    //    match, consume-and-delete, bind without a network call. This is
    //    the auto-setup → sandboxed-daemon-spawn happy path.
    // 2. **File missing, nonce env set**: failed auto-setup handoff.
    //    The parent advertised a handoff (env var set) but the daemon
    //    child could not find the file. Likely runtime-dir drift between
    //    parent and child, sandbox temp cleanup, or a consume race.
    //    Refuse with `BootstrapHandoffFailed` so the operator can
    //    distinguish from a legacy manual invocation. Per
    //    @agent-bravo-devrev's PR #16 finding (2026-04-27).
    // 3. **File missing, nonce env absent**: legacy / non-auto-setup
    //    path. Manual `chanvoy daemon serve`. Fall through to the
    //    original network whoami() — works in unsandboxed shells and
    //    is the right thing for developer-mode invocations.
    let env_nonce = env::var(chanvoy_core::BOOTSTRAP_NONCE_ENV).ok();
    let resolution =
        chanvoy_core::resolve_startup_identity(profile_name, &profile, env_nonce.as_deref())?;
    let my_user_id = match resolution {
        chanvoy_core::BootstrapResolution::Validated { user_id } => {
            info!(
                profile = profile_name,
                "chanvoy daemon trusted pre-validated identity from bootstrap state"
            );
            user_id
        }
        chanvoy_core::BootstrapResolution::Legacy => {
            // Manual `chanvoy daemon serve` (not via auto-setup): no
            // handoff in flight. Network whoami() runs as before.
            let identity = client.whoami().await?;
            if !profile.bot_username.is_empty() && identity.username != profile.bot_username {
                return Err(CoreError::ProfileIdentityMismatch {
                    expected: profile.bot_username.clone(),
                    actual: identity.username,
                }
                .into());
            }
            identity.id
        }
    };
    let listener = UnixListener::bind(&socket_path)?;
    fs::set_permissions(&socket_path, fs::Permissions::from_mode(0o600))?;
    fs::write(&pid_path, std::process::id().to_string())?;
    fs::set_permissions(&pid_path, fs::Permissions::from_mode(0o600))?;

    let ws_state_holder: Arc<Mutex<Option<Arc<WsState>>>> = Arc::new(Mutex::new(None));
    let event_bus: Arc<EventBus> = Arc::new(EventBus::new(256));
    let cancel_token = tokio_util::sync::CancellationToken::new();

    // Shared identity-drift bit, allocated before AppState so the IPC
    // peer (constructed before AppState) can also observe it. PER-014
    // (entarch finding #1, 2026-04-28): IPC must honor the same drift
    // gate as the local UDS surface.
    let identity_drift = Arc::new(AtomicBool::new(false));

    let ipc_state: Option<Arc<tokio::sync::Mutex<IpcPeerState>>> = match &profile.ipc {
        Some(IpcConfig {
            enabled: true,
            gateway_socket,
        }) if !gateway_socket.is_empty() => {
            // Clone rather than build a second client: clones share the
            // client's caches, so the IPC surface and the local socket
            // surface resolve teams and authors from the same place.
            let client_for_ipc = client.clone();
            let ipc_peer = Arc::new(IpcPeer::new(
                &profile,
                client_for_ipc,
                Arc::clone(&event_bus),
                gateway_socket.clone(),
                Arc::clone(&identity_drift),
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

    // PER-019 load-time migration: walk pre-PER-019 cursor entries
    // (keyed by bare channel name) and rewrite them under qualified
    // `<team_name>/<channel_name>` keys. Ambiguous names quarantine.
    // Idempotent — already-qualified entries are skipped.
    let mut attention = load_attention_state(&profile.name)?;
    match chanvoy_core::migrate_attention_state(&mut attention, &client).await {
        Ok(outcome) if outcome.migrated + outcome.quarantined > 0 => {
            info!(
                profile = profile_name,
                migrated = outcome.migrated,
                quarantined = outcome.quarantined,
                skipped = outcome.skipped,
                "PER-019 attention-state migration completed"
            );
            // Persist the rewritten state so write-time paths land in
            // qualified-key territory.
            store_attention_state(&profile.name, &attention)?;
        }
        Ok(_) => {
            // Nothing to migrate — no-op.
        }
        Err(err) => {
            // Migration is best-effort at startup; if the team-list
            // endpoint is unreachable now, write paths will resolve
            // lazily and the legacy entries will be rewritten on
            // first cursor update. Don't block daemon startup.
            tracing::warn!(
                profile = profile_name,
                %err,
                "PER-019 attention-state migration deferred — write paths will retry"
            );
        }
    }

    let state = Arc::new(AppState {
        profile: profile.clone(),
        client,
        socket_path: socket_path.clone(),
        my_user_id,
        event_bus: Arc::clone(&event_bus),
        subscriptions: Arc::new(Mutex::new(HashMap::new())),
        ws_state_holder: ws_state_holder.clone(),
        ipc_state,
        attention_state: Arc::new(Mutex::new(attention)),
        identity_drift,
        reduce_writer,
    });
    let (shutdown_tx, mut shutdown_rx) = oneshot::channel::<()>();
    let shutdown_tx = Arc::new(Mutex::new(Some(shutdown_tx)));

    let (ws_shutdown_tx, ws_shutdown_rx) = tokio::sync::watch::channel(false);
    {
        // Same-profile client, shared by clone (see the IPC peer above)
        // so the websocket pipeline reads author names out of the same
        // cache the request-response paths fill — and authenticates
        // with that client's credential, which is why no token is
        // passed here.
        let client_for_ws = state.client.clone();
        let event_bus = Arc::clone(&event_bus);
        let ws = Arc::new(MattermostWs::new(
            &profile,
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

    // PER-014 post-bind drift probe. Bind-first: the local UDS is already
    // listening. Probe-after: this runs asynchronously so the bind result
    // is not gated on Mattermost reachability — sandbox-blocked or
    // unreachable network surfaces as `mattermost_ok=false` via
    // `daemon_status`, never a startup failure. On identity mismatch
    // (whoami returns a different username than the configured
    // `bot_username`), we set the `identity_drift` bit; network-backed
    // RPCs surface this with a clear diagnostic. The local socket stays
    // bound regardless so operators can query `daemon_status` to learn
    // what's wrong. Per @agent-bravo-devrev's drift-floor framing
    // (#per-014, 2026-04-27).
    {
        let probe_state = Arc::clone(&state);
        tokio::spawn(async move {
            let probe = chanvoy_core::probe_whoami(
                &probe_state.client,
                chanvoy_core::STATUS_PROBE_TIMEOUT_MS,
            )
            .await;
            match probe {
                Ok(username) => {
                    if !probe_state.profile.bot_username.is_empty()
                        && username != probe_state.profile.bot_username
                    {
                        probe_state.identity_drift.store(true, Ordering::Relaxed);
                        warn!(
                            expected = %probe_state.profile.bot_username,
                            actual = %username,
                            "post-bind whoami probe surfaced identity drift; daemon stays bound, network RPCs will refuse"
                        );
                    }
                }
                Err(err) => {
                    info!(
                        profile = %probe_state.profile.name,
                        error = %err,
                        "post-bind whoami probe failed (sandbox-blocked or transient); daemon_status will retry on each call"
                    );
                }
            }
        });
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

/// Local-only readiness check for the daemon UDS socket. Use this when
/// the question is "is the daemon bound and answering RPCs?" — not
/// "does the daemon's Mattermost token still work?". Calls
/// `profile_status`, which is in `LOCAL_ONLY_METHODS` and never touches
/// the network.
///
/// PER-014 (entarch PR #16 finding #2): used by `ensure_daemon_running`'s
/// post-spawn "did the child I just spawned come up?" loop. Under sandbox
/// restrictions where REST is stalled rather than denied, a daemon_status-
/// based readiness check could exceed the post-spawn ping timeout and
/// cause `auto-setup` to report `Daemon(NotRunning)` even though the
/// daemon bound its socket — exactly the failure mode PER-014 is trying
/// to eliminate.
pub async fn ping(profile_name: &str) -> Result<ProfileStatus, DaemonError> {
    daemon_client(profile_name).profile_status().await
}

/// Network-aware health check for the daemon. Use this when the question
/// is "is the existing daemon usable?" — i.e., bound AND with a working
/// Mattermost token AND no identity drift. Calls `daemon_status`, which
/// runs `probe_whoami` against Mattermost.
///
/// PER-014 (entarch PR #16 residual finding, 2026-04-28): used by
/// `ensure_daemon_running`'s pre-spawn check to decide whether the
/// existing daemon should be reused or torn down and respawned. A
/// daemon with a revoked/rotated token or drifted identity must be
/// replaced rather than reused — the previous semantics relied on
/// the network probe to surface that, and PER-014's local-only ping()
/// retarget would otherwise mask it.
pub async fn ping_full(profile_name: &str) -> Result<DaemonStatus, DaemonError> {
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
                        // PER-014: if identity drift is set, suppress
                        // forwarding network-sourced events to subscribed
                        // clients. The post-bind probe (or a later
                        // daemon_status call) caught the bot's identity
                        // diverging from the configured bot_username; the
                        // drift floor's contract is "no Mattermost-sourced
                        // data flows while drift is true." Operators query
                        // daemon_status.mattermost_identity_drift to learn
                        // why the event stream paused; the local socket
                        // and `unsubscribe` / `daemon_status` /
                        // `profile_status` / attention RPCs stay
                        // answerable. Per @agent-bravo-devrev's PR #16
                        // finding, 2026-04-27.
                        if state.identity_drift.load(Ordering::Relaxed) {
                            continue;
                        }
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

/// PER-014: methods that NEVER serve Mattermost-sourced data. Even when
/// the drift gate is tripped, these must remain answerable so operators
/// can learn what's wrong via `daemon_status` and the daemon stays
/// administrable (`shutdown`, `unsubscribe`, `profile_status`, attention).
///
/// `subscribe` is intentionally NOT on this list: subscriptions forward
/// Mattermost WebSocket events from the daemon to clients, so accepting
/// new subscriptions under drift would let network-sourced data flow
/// for the wrong authenticated bot. Existing subscribers also have
/// their event forwarding gated on the drift bit (see
/// `handle_client`'s receive arm). `unsubscribe` stays local so
/// operators can clean up state without first un-drifting.
/// (Per @agent-bravo-devrev's PR #16 finding, 2026-04-27.)
const LOCAL_ONLY_METHODS: &[&str] = &[
    "daemon_status",
    "profile_status",
    "unsubscribe",
    "attention_list",
    "attention_show",
    "shutdown",
];

async fn dispatch_request(
    request: JsonRpcRequest,
    state: &AppState,
    shutdown_tx: &Arc<Mutex<Option<oneshot::Sender<()>>>>,
) -> JsonRpcResponse {
    // PER-014 drift gate. If the post-bind probe (or any later
    // `daemon_status` probe) caught the bot's Mattermost identity
    // diverging from the configured `bot_username`, network-backed RPCs
    // refuse with a clear diagnostic. Local-only RPCs stay answerable so
    // operators can query `daemon_status` and shut down cleanly. The
    // local socket stays bound regardless. Per @agent-bravo-devrev's
    // drift-floor framing (#per-014, 2026-04-27).
    let method = request.method.as_str();
    if state.identity_drift.load(Ordering::Relaxed) && !LOCAL_ONLY_METHODS.contains(&method) {
        return rpc_error(
            request.id,
            -32_000,
            "identity drift detected: configured bot_username does not match the Mattermost-returned username for this token; network-backed RPCs are refused. Inspect daemon_status.mattermost_identity_drift and re-run `chanvoy auto-setup` to re-validate identity.".to_string(),
        );
    }

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
        "list_channels_across_teams" => state
            .client
            .list_channels_across_teams()
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
            let team = params.team.as_deref();
            // PER-023 Scope §2 + AC #2a: bootstrap mode hits MM directly
            // for bounded-most-recent-N posts (default N=50, --limit
            // override). Mode-independent of --since/--after/etc.; CLI
            // enforces mutual exclusion.
            let mut messages = if params.since_bootstrap {
                let limit = params.limit.unwrap_or(50);
                state
                    .client
                    .read_channel_most_recent(&params.channel, limit, team)
                    .await?
            } else if let Some(after_post_id) = params.after_post_id {
                state
                    .client
                    .read_channel_after(&params.channel, &after_post_id, team)
                    .await?
            } else if params.since_last_mine {
                state
                    .client
                    .read_channel_since_last_mine(&params.channel, team)
                    .await?
            } else if let Some(secs) = params.since_secs {
                state
                    .client
                    .read_channel_since_secs(&params.channel, secs, team)
                    .await?
            } else {
                state
                    .client
                    .read_channel(&params.channel, params.since_minutes.unwrap_or(60), team)
                    .await?
            };
            // PER-023 Scope §2 + AC #2a: general --limit truncates the
            // existing read-mode result set (hard cap; no full-window
            // pagination semantics added by PER-023). Bootstrap already
            // applied the limit at the API layer, so this no-ops there.
            if let Some(limit) = params.limit {
                if !params.since_bootstrap {
                    let limit = limit as usize;
                    if messages.len() > limit {
                        // Keep the most-recent N — sort is ascending by
                        // create_at, so truncate from the front.
                        let drop = messages.len() - limit;
                        messages.drain(..drop);
                    }
                }
            }
            // PER-023 Scope §4 + AC #4: --advance advances the cursor
            // to the latest post **returned** (mode-independent rule).
            // No-op when zero posts returned.
            if params.advance {
                if let Some(latest) = messages.last() {
                    record_channel_cursor(state, &params.channel, &latest.id, team).await?;
                }
            }
            Ok::<_, CoreError>(messages)
        })
        .await
        .map(to_value),
        "pinned_channel" => {
            parse_and_call(&request.params, |params: PinnedChannelParams| async move {
                state
                    .client
                    .read_channel_pinned(&params.channel, params.team.as_deref())
                    .await
            })
            .await
            .map(to_value)
        }
        "get_post" => parse_and_call(&request.params, |params: GetPostParams| async move {
            // Pure read. Resolution first so the post is bound against
            // the channel id the operator's channel argument actually
            // named, then one point-fetch that refuses before it
            // returns a body if the post lives elsewhere.
            let resolved = state
                .client
                .resolve_channel(&params.channel, params.team.as_deref())
                .await?;
            state
                .client
                .get_post_in_channel(
                    &resolved.channel_id,
                    &resolved.channel_name,
                    &params.post_id,
                )
                .await
        })
        .await
        .map(to_value),
        "read_thread" => parse_and_call(&request.params, |params: ReadThreadParams| async move {
            // Pure read. The anchor point-fetch does double duty: it is
            // the channel binding (no thread request is issued at all if
            // it refuses) and it is where the canonical root comes from,
            // which is what lets an operator name any reply in the
            // thread rather than having to know the root.
            let resolved = state
                .client
                .resolve_channel(&params.channel, params.team.as_deref())
                .await?;
            let anchor = state
                .client
                .get_post_in_channel(
                    &resolved.channel_id,
                    &resolved.channel_name,
                    &params.post_id,
                )
                .await?;
            let mut messages = state
                .client
                .read_thread_in_channel(
                    &resolved.channel_id,
                    &resolved.channel_name,
                    &anchor.root_id,
                )
                .await?;
            // `--latest` narrows the list; it does not change its type.
            //
            // Select the genuine maximum rather than taking the tail.
            // The thread ordering pins the root first no matter when it
            // was written, so "the last element" means "the last reply",
            // which is a different post from "the newest message" for
            // any thread whose root carries a later timestamp than a
            // reply. Ties break on id so the choice is stable across
            // calls.
            if params.latest {
                if let Some(index) = messages
                    .iter()
                    .enumerate()
                    .max_by(|(_, a), (_, b)| (a.create_at, &a.id).cmp(&(b.create_at, &b.id)))
                    .map(|(index, _)| index)
                {
                    let latest = messages.swap_remove(index);
                    messages = vec![latest];
                }
            }
            Ok::<_, CoreError>(messages)
        })
        .await
        .map(to_value),
        "ack_channel" => parse_and_call(&request.params, |params: AckChannelParams| async move {
            let team = params.team.as_deref();
            // Resolve up-front so the result carries the operator-visible
            // qualified-channel info even when the channel turns out to
            // be empty.
            let resolved = state.client.resolve_channel(&params.channel, team).await?;
            let cursor_post_id = state
                .client
                .channel_last_post_id(&params.channel, team)
                .await?;
            if let Some(ref post_id) = cursor_post_id {
                record_channel_cursor(state, &params.channel, post_id, team).await?;
            }
            Ok::<_, CoreError>(AckResult {
                channel: resolved.channel_name,
                team: resolved.team_name,
                cursor_post_id,
            })
        })
        .await
        .map(to_value),
        "check_channel" => {
            parse_and_call(&request.params, |params: CheckChannelParams| async move {
                // PER-019 (devrev PR #17 finding #1): thread --team
                // through to the channel resolution so duplicate-name
                // channels check on the requested team, not the
                // primary-team default.
                check_channel(
                    state,
                    &params.channel,
                    params.after_post_id.as_deref(),
                    params.team.as_deref(),
                )
                .await
            })
            .await
            .map(to_value)
        }
        "post_message" => parse_and_call(&request.params, |params: PostMessageParams| async move {
            // PER-035: resolve the channel on the CALLING identity
            // (resolution + verification never reduce), decide whether
            // the terminal write reduces to the family identity, then
            // write through the chosen client. PER-024 threaded path is
            // preserved: when thread_root_id is set we verify the parent
            // exists on the resolved channel (calling identity) before
            // the threaded write.
            let resolved = state
                .client
                .resolve_channel(&params.channel, params.team.as_deref())
                .await?;
            let (writer, posting_identity, reduced) = state.select_writer(&resolved.team_name);
            log_posting_identity("post", state, &resolved, posting_identity, reduced);
            let receipt = if let Some(root_id) = &params.thread_root_id {
                state
                    .client
                    .assert_post_in_channel(&resolved.channel_id, &resolved.channel_name, root_id)
                    .await?;
                writer
                    .post_threaded_reply(&resolved.channel_id, root_id, &params.message)
                    .await?
            } else {
                writer
                    .post_message_by_id(&resolved.channel_id, &params.message)
                    .await?
            };
            // PER-019 (devrev PR #17 finding #2): cursor recording must
            // bind to the same team the post landed on. Pass the
            // operator's --team override through; otherwise a
            // duplicate-name channel could record under the
            // primary-team key while the post went to Ops. Cursor is the
            // calling profile's attention state regardless of which
            // identity authored the post.
            record_channel_cursor(state, &params.channel, &receipt.id, params.team.as_deref())
                .await?;
            Ok(receipt)
        })
        .await
        .map(to_value),
        "react_post" => parse_and_call(&request.params, |params: ReactParams| async move {
            // PER-024 AC #5b: reactions are auth-bound metadata writes
            // with NO cursor side effects; this dispatch does NOT call
            // record_channel_cursor. PER-035: resolve + verify on the
            // calling identity; the reaction itself is bound to the
            // chosen writer's user, so it reduces to the family bot for
            // outside-team channels.
            let resolved = state
                .client
                .resolve_channel(&params.channel, params.team.as_deref())
                .await?;
            let (writer, posting_identity, reduced) = state.select_writer(&resolved.team_name);
            log_posting_identity("react", state, &resolved, posting_identity, reduced);
            state
                .client
                .assert_post_in_channel(
                    &resolved.channel_id,
                    &resolved.channel_name,
                    &params.post_id,
                )
                .await?;
            writer
                .react_by_id(&resolved, &params.post_id, &params.emoji)
                .await
        })
        .await
        .map(to_value),
        "unreact_post" => parse_and_call(&request.params, |params: UnreactParams| async move {
            // PER-024 AC #5b: same cursor-neutral contract as react.
            // PER-035: reduces identity the same way react does.
            let resolved = state
                .client
                .resolve_channel(&params.channel, params.team.as_deref())
                .await?;
            let (writer, posting_identity, reduced) = state.select_writer(&resolved.team_name);
            log_posting_identity("unreact", state, &resolved, posting_identity, reduced);
            state
                .client
                .assert_post_in_channel(
                    &resolved.channel_id,
                    &resolved.channel_name,
                    &params.post_id,
                )
                .await?;
            writer
                .unreact_by_id(&resolved, &params.post_id, &params.emoji)
                .await
        })
        .await
        .map(to_value),
        "pin_post" => parse_and_call(&request.params, |params: PinParams| async move {
            // PER-034: cursor-neutral write verb. PER-035: resolve +
            // pin-state pre-read on the calling identity; the pin write
            // reduces to the family identity for outside-team channels.
            let resolved = state
                .client
                .resolve_channel(&params.channel, params.team.as_deref())
                .await?;
            let (writer, posting_identity, reduced) = state.select_writer(&resolved.team_name);
            log_posting_identity("pin", state, &resolved, posting_identity, reduced);
            let was_already_pinned = state
                .client
                .fetch_post_pinned_state(
                    &resolved.channel_id,
                    &resolved.channel_name,
                    &params.post_id,
                )
                .await?;
            writer
                .pin_by_id(&resolved, &params.post_id, was_already_pinned)
                .await
        })
        .await
        .map(to_value),
        "unpin_post" => parse_and_call(&request.params, |params: UnpinParams| async move {
            // PER-034: symmetric to pin_post. PER-035: reduces identity
            // the same way pin does.
            let resolved = state
                .client
                .resolve_channel(&params.channel, params.team.as_deref())
                .await?;
            let (writer, posting_identity, reduced) = state.select_writer(&resolved.team_name);
            log_posting_identity("unpin", state, &resolved, posting_identity, reduced);
            let was_pinned = state
                .client
                .fetch_post_pinned_state(
                    &resolved.channel_id,
                    &resolved.channel_name,
                    &params.post_id,
                )
                .await?;
            writer
                .unpin_by_id(&resolved, &params.post_id, was_pinned)
                .await
        })
        .await
        .map(to_value),
        "search_channel" => parse_and_call(&request.params, |params: SearchParams| async move {
            // PER-025 primitive 1: pure read, no cursor side effects.
            // Operator-conflict detection ran at the CLI layer; by
            // this point the query is conflict-free relative to
            // chanvoy-owned scopes.
            state
                .client
                .search_channel(
                    &params.channel,
                    &params.query,
                    params.limit.unwrap_or(20),
                    params.from.as_deref(),
                    params.since_secs,
                    params.team.as_deref(),
                )
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
                if params.unread_only {
                    unread_notifications(state)
                        .await
                        .map(NotificationsResponse::Unread)
                } else {
                    // PER-023: prefer second-resolution `since_secs` over
                    // the legacy minute-resolution field. Round up to the
                    // next minute so the underlying minutes-API client
                    // doesn't truncate sub-minute windows to zero.
                    let since_minutes = if let Some(secs) = params.since_secs {
                        secs.div_ceil(60).max(1)
                    } else {
                        params.since_minutes.unwrap_or(1440)
                    };
                    let notifications = state.client.notifications(since_minutes).await?;
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
            // PER-019 (devrev PR #17 finding #1): thread --team into
            // the wait helper so duplicate-name channels wait on the
            // requested team's cursor.
            // PER-023: prefer second-resolution `timeout_secs` so
            // suffixes like `30s` aren't lossily rounded.
            let timeout_secs = params
                .timeout_secs
                .unwrap_or(params.timeout_minutes.saturating_mul(60));
            wait_for_messages(state, &params.channel, timeout_secs, params.team.as_deref()).await
        })
        .await
        .map(to_value),
        "create_channel" => {
            parse_and_call(&request.params, |params: CreateChannelParams| async move {
                state
                    .client
                    .create_channel(
                        &params.name,
                        &params.display_name,
                        params.purpose,
                        params.team.as_deref(),
                    )
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
            let whoami_result =
                chanvoy_core::probe_whoami(&state.client, chanvoy_core::STATUS_PROBE_TIMEOUT_MS)
                    .await;
            // PER-014: keep the drift bit fresh — the post-bind one-shot
            // probe seeds it, but `daemon_status` is the live signal that
            // re-validates each call. A previously-tripped drift can also
            // recover here (e.g., bot identity restored externally).
            if let Ok(ref username) = whoami_result {
                if !state.profile.bot_username.is_empty() {
                    let drifted = *username != state.profile.bot_username;
                    state.identity_drift.store(drifted, Ordering::Relaxed);
                }
            }
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
                Ok::<_, CoreError>(
                    attention_show(state, &params.channel, params.team.as_deref()).await,
                )
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
    timeout_secs: u64,
    team: Option<&str>,
) -> Result<WaitResult, CoreError> {
    // PER-019 (devrev PR #17 finding #1): resolve via the cross-team
    // resolver so duplicate-name channels wait on the requested team.
    // The previous `channel_id_for_name` path went through the legacy
    // `channel_id` helper, which now uses the resolver default chain
    // (primary-first/fallback) but did not honor the operator's
    // explicit `--team` override.
    let channel_id = state
        .client
        .resolve_channel(channel, team)
        .await?
        .channel_id;

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

    let limit = Duration::from_secs(timeout_secs);

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

/// PER-019 (devrev PR #17 second-pass finding): predicate for
/// `wait_push_backed` event matching, extracted for unit testability.
/// Returns true when the inbound event should wake the wait — same
/// resolved `channel_id` (not name; same-named channels on different
/// teams have distinct ids), past the cursor, not authored by us.
fn inbound_event_wakes_wait(
    payload: &chanvoy_core::InboundEventPayload,
    channel_id: &str,
    cursor_id: &str,
    cursor_create_at: i64,
    my_user_id: &str,
) -> bool {
    payload.channel_id == channel_id
        && payload.post_id != cursor_id
        && payload.create_at > cursor_create_at
        && payload.sender_id != my_user_id
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
                        if inbound_event_wakes_wait(
                            p,
                            channel_id,
                            cursor_id,
                            cursor_create_at,
                            &state.my_user_id,
                        ) =>
                    {
                        return Ok(WaitResult {
                            channel: channel.to_string(),
                            messages: vec![chanvoy_core::Message {
                                id: p.post_id.clone(),
                                user_id: p.sender_id.clone(),
                                username: p.sender_username.clone(),
                                message: p.message.clone(),
                                create_at: p.create_at,
                                // The real thread root, carried through
                                // from the push event. A caller can
                                // reply to this message directly; a
                                // fabricated self-root would be wrong
                                // for every reply and the provider
                                // rejects a reply aimed at a reply.
                                root_id: p.root_id.clone(),
                            }],
                        });
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
    team: Option<&str>,
) -> Result<CheckResult, CoreError> {
    let (anchor, anchor_source) = if let Some(after) = explicit_after {
        (Some(after.to_string()), "explicit_after".to_string())
    } else {
        // PER-019 (devrev PR #17 finding #1): lookup uses the operator's
        // --team override so duplicate-name channels read the cursor
        // for the requested team, not the primary-team default.
        let key = qualified_attention_key(state, channel, team).await?;
        let attention = state.attention_state.lock().await;
        let Some(cursor) = attention.channels.get(&key) else {
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
        .read_channel_after(channel, &anchor_post_id, team)
        .await
    {
        Ok(messages) => {
            // Freshness verdict: cursor probed and anchor still present.
            // Persist `last_known_stale=false` + `last_checked_at=now` so
            // `attention list` / `show` can surface the verdict without
            // its own probe (PER-008B D1: cached staleness, cxotech's
            // `last_checked_at` refinement).
            if anchor_source == "daemon_cursor" {
                record_staleness_verdict(state, channel, false, team).await;
            }
            messages
        }
        Err(CoreError::AnchorNotFound(_)) | Err(CoreError::AnchorChannelMismatch { .. })
            if anchor_source == "daemon_cursor" =>
        {
            record_staleness_verdict(state, channel, true, team).await;
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

/// PER-019: resolve a channel argument (possibly `<team>/<channel>` or
/// bare name) plus an optional `--team` override into the qualified
/// `<team_name>/<channel_name>` key used by `AttentionState.channels`.
/// Centralized so every lookup site honors the same resolution chain
/// the read/post/check verbs use.
///
/// **Network call**: invokes `resolve_channel` which hits Mattermost
/// for `team_id`/`channel_id` lookup. Suitable only for handlers that
/// are already in the network-call set (post/read/check/wait); for
/// strict-read-only handlers (attention show/list per PER-008B), use
/// [`local_attention_key`] instead.
async fn qualified_attention_key(
    state: &AppState,
    channel: &str,
    team: Option<&str>,
) -> Result<String, CoreError> {
    let resolved = state.client.resolve_channel(channel, team).await?;
    Ok(chanvoy_core::attention_key_for(
        &resolved.team_name,
        &resolved.channel_name,
    ))
}

/// PER-019 (secrev PR #17 attention-surface finding, 2026-04-29): build
/// an attention-state lookup key without making any network call,
/// preserving the PER-008B strict-read-only contract on the
/// `attention show` / `attention list` RPCs.
///
/// Heuristic mirrors `attention_list`'s `monitored_channels` qualifying
/// pass:
/// - Already-qualified input (`<team>/<channel>`) passes through
///   verbatim, with `#` trimmed from the channel segment.
/// - Explicit `--team <slug>` override qualifies with the requested
///   team (no membership verification — that's the strict-read-only
///   trade-off; an operator pointing at a non-member team simply gets
///   `NoAnchor` rather than a refusal).
/// - Bare name defaults to the profile's primary team.
///
/// Trade-off (consistent with PER-008B): a bare name typed against a
/// channel whose cursor is qualified to a non-primary team will return
/// `NoAnchor` from the lookup. That's the correct strict-read-only
/// behavior — operators disambiguate with `--team` or
/// `<team>/<channel>` when they need to inspect non-primary cursors.
fn local_attention_key(state: &AppState, channel: &str, team: Option<&str>) -> String {
    local_attention_key_for(state.client.primary_team_name(), channel, team)
}

/// Pure-string variant of [`local_attention_key`] for unit testability.
/// Same heuristic, takes the primary-team slug directly instead of
/// extracting it from `AppState`.
fn local_attention_key_for(primary_team: &str, channel: &str, team: Option<&str>) -> String {
    let trimmed = channel.trim_start_matches('#');
    if let Some((team_slug, channel_name)) = trimmed.split_once('/') {
        return chanvoy_core::attention_key_for(team_slug, channel_name.trim_start_matches('#'));
    }
    let resolved_team = team.unwrap_or(primary_team);
    chanvoy_core::attention_key_for(resolved_team, trimmed)
}

async fn record_channel_cursor(
    state: &AppState,
    channel: &str,
    post_id: &str,
    team: Option<&str>,
) -> Result<(), CoreError> {
    // PER-019 (devrev PR #17 finding #2): cursor recording must bind
    // to the same team the side effect (post / read) landed on.
    // Threading the operator's `--team` override here matches the
    // resolver call the surrounding RPC made, so duplicate-name
    // channels record under the right qualified key.
    let resolved = state.client.resolve_channel(channel, team).await?;
    let key = chanvoy_core::attention_key_for(&resolved.team_name, &resolved.channel_name);
    let mut attention = state.attention_state.lock().await;
    // Every cursor-write path is a staleness-clearing event per the
    // PER-008B D1 guardrail: the new cursor value is fresh, by definition
    // not stale, and has not yet been checked.
    attention.channels.insert(
        key,
        chanvoy_core::ChannelCursorState {
            last_seen_post_id: Some(post_id.to_string()),
            updated_at: Some(chanvoy_core::now_unix_millis()),
            last_known_stale: false,
            last_checked_at: None,
            channel_id: resolved.channel_id,
            team_id: resolved.team_id,
            team_name: resolved.team_name,
            channel_name: resolved.channel_name,
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
    // PER-019: resolve to qualified key first; only then check absence
    // under the new key shape. Pre-PER-019 entries with a bare-name key
    // are migrated at daemon `start()` so by the time we get here the
    // map is qualified-keyed.
    let resolved = state.client.resolve_channel(channel, None).await?;
    let key = chanvoy_core::attention_key_for(&resolved.team_name, &resolved.channel_name);
    let mut attention = state.attention_state.lock().await;
    if attention.channels.contains_key(&key) {
        return Ok(false);
    }
    // A freshly-seeded cursor is by definition non-stale and unchecked.
    attention.channels.insert(
        key,
        chanvoy_core::ChannelCursorState {
            last_seen_post_id: Some(post_id.to_string()),
            updated_at: Some(chanvoy_core::now_unix_millis()),
            last_known_stale: false,
            last_checked_at: None,
            channel_id: resolved.channel_id,
            team_id: resolved.team_id,
            team_name: resolved.team_name,
            channel_name: resolved.channel_name,
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
async fn record_staleness_verdict(
    state: &AppState,
    channel: &str,
    stale: bool,
    team: Option<&str>,
) {
    // PER-019 (devrev PR #17 finding #1): lookup uses qualified
    // `<team>/<channel>` key, honoring the operator's --team override
    // so duplicate-name channels persist the verdict on the right team's
    // cursor entry.
    let key = match qualified_attention_key(state, channel, team).await {
        Ok(k) => k,
        Err(err) => {
            tracing::warn!(
                profile = %state.profile.name,
                channel = %channel,
                %err,
                "failed to resolve channel for staleness verdict; skipping persistence"
            );
            return;
        }
    };
    let mut attention = state.attention_state.lock().await;
    let Some(cursor) = attention.channels.get_mut(&key) else {
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
    // PER-019 (secrev PR #17 finding #1): qualify monitored_channels
    // entries against the primary team before unioning with the
    // already-qualified attention.channels keys. Pre-fix, a tracked
    // channel that also had a persisted cursor under the qualified
    // key would emit two rows (a bare `bravo-team` no_anchor + a
    // qualified `org-lanytehq/bravo-team` cursor). The bare form
    // resolves against the primary team because that's the
    // historical interpretation of `monitored_channels`.
    let primary_team = state.client.primary_team_name();
    let mut channel_keys: std::collections::BTreeSet<String> = state
        .profile
        .monitored_channels
        .iter()
        .map(|name| {
            if name.contains('/') {
                name.clone()
            } else {
                chanvoy_core::attention_key_for(primary_team, name)
            }
        })
        .collect();
    channel_keys.extend(attention.channels.keys().cloned());
    let channels = channel_keys
        .into_iter()
        .map(|key| match attention.channels.get(&key) {
            Some(cursor) => chanvoy_core::AttentionChannelEntry {
                channel: key,
                source: attention_source_for_channel(cursor),
                newest_seen: cursor.last_seen_post_id.clone(),
                updated_at: cursor.updated_at,
                last_checked_at: cursor.last_checked_at,
            },
            None => chanvoy_core::AttentionChannelEntry {
                channel: key,
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
    // PER-019 (secrev PR #17 finding #2): surface quarantined legacy
    // records so operators can see them and disambiguate. Cloned out
    // of the locked state for read-only display; the originals
    // remain in attention.quarantined until an operator re-reads /
    // re-posts via --team or <team>/<channel> to re-establish a
    // qualified cursor.
    let quarantined = attention.quarantined.clone();
    chanvoy_core::AttentionListResult {
        profile: state.profile.name.clone(),
        channels,
        mentions,
        quarantined,
    }
}

/// Build `AttentionShowResult` for a specific channel. Returns an entry
/// with `source = NoAnchor` when the channel is not tracked, rather than
/// erroring — operators asking about an untracked channel want that
/// confirmed, not a bare error.
async fn attention_show(
    state: &AppState,
    channel: &str,
    team: Option<&str>,
) -> chanvoy_core::AttentionShowResult {
    // PER-019 (secrev PR #17 attention-surface finding, 2026-04-29):
    // build the lookup key locally without resolving against
    // Mattermost — `attention show` is on the strict-read-only
    // attention prefix per PER-008B and must never make network
    // calls. The earlier qualified_attention_key path violated that
    // contract by going through `resolve_channel`. The local
    // qualifier mirrors `attention_list`'s heuristic: explicit
    // <team>/<channel> or --team wins; bare name defaults to the
    // primary team. Bare name against a non-primary cursor returns
    // NoAnchor — operators disambiguate via --team for cross-team
    // inspection.
    let key = local_attention_key(state, channel, team);
    let attention = state.attention_state.lock().await;
    let entry = match attention.channels.get(&key).map(|c| (key.clone(), c)) {
        Some((key, cursor)) => chanvoy_core::AttentionChannelEntry {
            channel: key,
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

    /// PER-019 AC #11: list channels across every team the bot is a
    /// member of, grouped per team.
    pub async fn list_channels_across_teams(
        &self,
    ) -> Result<Vec<chanvoy_core::TeamChannels>, DaemonError> {
        self.call("list_channels_across_teams", serde_json::json!({}))
            .await
    }

    pub async fn list_dms(&self) -> Result<Vec<DmConversation>, DaemonError> {
        self.call("list_dms", serde_json::json!({})).await
    }

    /// PER-023: full read-channel surface. The CLI populates `since_secs`
    /// (parsed via `parse_time_window`) for `--since` and the bootstrap /
    /// limit / advance flags for the new primitives. `since_minutes` is
    /// retained on the param wire only as a back-compat field a daemon
    /// can still decode from a not-yet-upgraded CLI peer.
    ///
    /// Bidirectional version safety (devrev PR #20 P1): when `since_secs`
    /// is set, the legacy `since_minutes` field is also populated with
    /// the same value rounded up to a minute. A new-CLI → old-daemon
    /// path then falls back to approximate-but-not-silent semantics
    /// (a v0.2.0 daemon reading a `30s` request reads ~1 minute; the
    /// new daemon prefers `since_secs` over `since_minutes` and gets
    /// the precise value). PER-023's new flags (`since_bootstrap`,
    /// `limit`, `advance`) are silently ignored by old daemons — this
    /// is acceptable because the only operator-visible regression is
    /// "behaves like pre-PER-023 read", not "silent zero-result";
    /// `--advance` failing to advance a cursor against an old daemon
    /// is recoverable on the next CLI invocation post-cycle.
    #[allow(clippy::too_many_arguments)]
    pub async fn read_channel(
        &self,
        channel: &str,
        since_secs: Option<u64>,
        after_post_id: Option<String>,
        since_last_mine: bool,
        since_bootstrap: bool,
        limit: Option<u32>,
        advance: bool,
        team: Option<String>,
    ) -> Result<Vec<chanvoy_core::Message>, DaemonError> {
        let since_minutes = since_secs.map(secs_to_minutes_compat);
        self.call(
            "read_channel",
            serde_json::to_value(ReadChannelParams {
                channel: channel.to_string(),
                since_minutes,
                since_secs,
                after_post_id,
                since_last_mine,
                since_bootstrap,
                limit,
                advance,
                team,
            })?,
        )
        .await
    }

    /// PER-023 primitive 1: fetch pinned posts for a channel.
    pub async fn pinned_channel(
        &self,
        channel: &str,
        team: Option<String>,
    ) -> Result<Vec<chanvoy_core::Message>, DaemonError> {
        self.call(
            "pinned_channel",
            serde_json::to_value(PinnedChannelParams {
                channel: channel.to_string(),
                team,
            })?,
        )
        .await
    }

    /// PER-023 primitive 4: advance attention cursor to channel's
    /// current latest post id without surfacing content.
    pub async fn ack_channel(
        &self,
        channel: &str,
        team: Option<String>,
    ) -> Result<AckResult, DaemonError> {
        self.call(
            "ack_channel",
            serde_json::to_value(AckChannelParams {
                channel: channel.to_string(),
                team,
            })?,
        )
        .await
    }

    pub async fn check_channel(
        &self,
        channel: &str,
        after_post_id: Option<String>,
        team: Option<String>,
    ) -> Result<CheckResult, DaemonError> {
        self.call(
            "check_channel",
            serde_json::to_value(CheckChannelParams {
                channel: channel.to_string(),
                after_post_id,
                team,
            })?,
        )
        .await
    }

    /// PER-024: when `thread_root_id` is `Some`, the post is created as
    /// a threaded reply via `post_threaded_reply_in_channel` and the
    /// returned `PostReceipt` carries an additive `parent_id` field.
    /// Otherwise the existing `post_message` path runs unchanged.
    pub async fn post_message(
        &self,
        channel: &str,
        message: &str,
        team: Option<String>,
        thread_root_id: Option<String>,
    ) -> Result<chanvoy_core::PostReceipt, DaemonError> {
        self.call(
            "post_message",
            serde_json::to_value(PostMessageParams {
                channel: channel.to_string(),
                message: message.to_string(),
                team,
                thread_root_id,
            })?,
        )
        .await
    }

    /// PER-024 primitive 2: add an emoji reaction under the bot's
    /// identity. Channel positional for multi-provider portability.
    pub async fn react_post(
        &self,
        channel: &str,
        post_id: &str,
        emoji: &str,
        team: Option<String>,
    ) -> Result<ReactionResult, DaemonError> {
        self.call(
            "react_post",
            serde_json::to_value(ReactParams {
                channel: channel.to_string(),
                post_id: post_id.to_string(),
                emoji: emoji.to_string(),
                team,
            })?,
        )
        .await
    }

    /// PER-024 primitive 2: remove the bot's reaction. Idempotent on
    /// missing-reaction (success exit).
    pub async fn unreact_post(
        &self,
        channel: &str,
        post_id: &str,
        emoji: &str,
        team: Option<String>,
    ) -> Result<ReactionResult, DaemonError> {
        self.call(
            "unreact_post",
            serde_json::to_value(UnreactParams {
                channel: channel.to_string(),
                post_id: post_id.to_string(),
                emoji: emoji.to_string(),
                team,
            })?,
        )
        .await
    }

    /// PER-034: pin a post via MM v4 `POST /posts/{id}/pin`.
    /// Idempotent on already-pinned. Channel positional matches
    /// the cross-team γ hybrid resolver convention.
    pub async fn pin_post(
        &self,
        channel: &str,
        post_id: &str,
        team: Option<String>,
    ) -> Result<PinResult, DaemonError> {
        self.call(
            "pin_post",
            serde_json::to_value(PinParams {
                channel: channel.to_string(),
                post_id: post_id.to_string(),
                team,
            })?,
        )
        .await
    }

    /// PER-034: unpin a post. Symmetric to `pin_post`.
    pub async fn unpin_post(
        &self,
        channel: &str,
        post_id: &str,
        team: Option<String>,
    ) -> Result<UnpinResult, DaemonError> {
        self.call(
            "unpin_post",
            serde_json::to_value(UnpinParams {
                channel: channel.to_string(),
                post_id: post_id.to_string(),
                team,
            })?,
        )
        .await
    }

    /// PER-025 primitive 1: search posts within a channel.
    /// Operator-conflict detection runs at the CLI layer; this client
    /// expects the query to be already-vetted by
    /// `check_search_operator_conflicts`.
    pub async fn search_channel(
        &self,
        channel: &str,
        query: &str,
        limit: Option<u32>,
        from: Option<String>,
        since_secs: Option<u64>,
        team: Option<String>,
    ) -> Result<SearchResult, DaemonError> {
        self.call(
            "search_channel",
            serde_json::to_value(SearchParams {
                channel: channel.to_string(),
                query: query.to_string(),
                limit,
                from,
                since_secs,
                team,
            })?,
        )
        .await
    }

    /// Fetch one post from a named channel. Pure read.
    pub async fn get_post(
        &self,
        channel: &str,
        post_id: &str,
        team: Option<String>,
    ) -> Result<chanvoy_core::Message, DaemonError> {
        self.call(
            "get_post",
            serde_json::to_value(GetPostParams {
                channel: channel.to_string(),
                post_id: post_id.to_string(),
                team,
            })?,
        )
        .await
    }

    /// Read the thread a post belongs to. `post_id` may be the root or
    /// any reply. Pure read; always returns a list.
    pub async fn read_thread(
        &self,
        channel: &str,
        post_id: &str,
        latest: bool,
        team: Option<String>,
    ) -> Result<Vec<chanvoy_core::Message>, DaemonError> {
        self.call(
            "read_thread",
            serde_json::to_value(ReadThreadParams {
                channel: channel.to_string(),
                post_id: post_id.to_string(),
                latest,
                team,
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
        since_secs: u64,
        unread_only: bool,
    ) -> Result<serde_json::Value, DaemonError> {
        // Devrev PR #20 P1 fix: populate legacy minutes field with a
        // rounded-up compatibility value so a new-CLI → old-daemon path
        // doesn't silently fall back to the 1440m default.
        self.call(
            "notifications",
            serde_json::to_value(NotificationsParams {
                since_minutes: Some(secs_to_minutes_compat(since_secs)),
                since_secs: Some(since_secs),
                unread_only,
            })?,
        )
        .await
    }

    pub async fn wait_channel(
        &self,
        channel: &str,
        timeout_secs: u64,
        team: Option<String>,
    ) -> Result<WaitResult, DaemonError> {
        // Devrev PR #20 P1 fix: legacy `timeout_minutes` field is
        // populated with a rounded-up compatibility value so an old
        // daemon waits for an approximate (not zero) window. Without
        // this, `wait --timeout 5m` against a v0.2.0 daemon would
        // immediately return `WaitTimeout` because the legacy field
        // came across as 0. New daemon prefers `timeout_secs`.
        self.call(
            "wait_channel",
            serde_json::to_value(WaitChannelParams {
                channel: channel.to_string(),
                timeout_minutes: secs_to_minutes_compat(timeout_secs),
                timeout_secs: Some(timeout_secs),
                team,
            })?,
        )
        .await
    }

    pub async fn create_channel(
        &self,
        name: &str,
        display_name: &str,
        purpose: Option<String>,
        team: Option<String>,
    ) -> Result<Channel, DaemonError> {
        self.call(
            "create_channel",
            serde_json::to_value(CreateChannelParams {
                name: name.to_string(),
                display_name: display_name.to_string(),
                purpose,
                team,
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
        team: Option<String>,
    ) -> Result<chanvoy_core::AttentionShowResult, DaemonError> {
        self.call(
            "attention_show",
            serde_json::to_value(AttentionShowParams {
                channel: channel.to_string(),
                team,
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

/// Devrev PR #20 P1 helper: round seconds up to the next whole minute,
/// minimum 1 minute. Used when populating legacy `since_minutes` /
/// `timeout_minutes` fields alongside the new `since_secs` /
/// `timeout_secs` fields so a new-CLI → old-daemon path falls back to
/// approximate (rather than silently broken) semantics. Does not
/// affect new-daemon behavior because the daemon prefers the seconds
/// field when present.
fn secs_to_minutes_compat(secs: u64) -> u64 {
    secs.div_ceil(60).max(1)
}

#[cfg(test)]
mod compat_tests {
    use super::*;

    #[test]
    fn secs_to_minutes_rounds_up() {
        assert_eq!(secs_to_minutes_compat(30), 1, "30s rounds up to 1m");
        assert_eq!(secs_to_minutes_compat(59), 1, "59s rounds up to 1m");
        assert_eq!(secs_to_minutes_compat(60), 1, "60s = 1m exactly");
        assert_eq!(secs_to_minutes_compat(61), 2, "61s rounds up to 2m");
        assert_eq!(secs_to_minutes_compat(300), 5, "5m exactly");
        assert_eq!(secs_to_minutes_compat(301), 6, "5m+1s rounds to 6m");
    }

    #[test]
    fn secs_to_minutes_zero_floors_to_one() {
        // Edge case: an operator who somehow ends up with 0 seconds
        // shouldn't trip the old-daemon WaitTimeout-on-0-minutes path.
        // Floor to 1m so a `wait --timeout 0s` pathological invocation
        // at least lasts a minute against an old daemon — pathological
        // either way, but the failure mode is "waited a minute" rather
        // than "instantly timed out".
        assert_eq!(secs_to_minutes_compat(0), 1);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The daemon reads the profile's credential exactly once.
    ///
    /// Reading it a second time for another surface can straddle a
    /// rotation and leave the websocket authenticated as one identity
    /// while the request-response client is another — and the drift
    /// probe only inspects the first. The websocket now derives its
    /// credential from the shared client, so a split is unrepresentable
    /// rather than merely avoided; this guards the remaining way to
    /// reintroduce it, which is a second load in `start`.
    ///
    /// The reduction writer's own load is deliberately excluded: it is
    /// a different profile and a different identity by design.
    #[test]
    fn the_profile_credential_is_loaded_exactly_once() {
        let source = include_str!("lib.rs");
        let start = source
            .find("pub async fn start(")
            .expect("start function present");
        // Stop at the test module: this test's own source mentions the
        // pattern it searches for, and counting those would make the
        // assertion about itself rather than about `start`.
        let end = source.find("\nmod tests {").expect("test module present");
        let body = &source[start..end];
        let loads = body.matches("load_token(&profile)").count();
        assert_eq!(
            loads, 1,
            "the profile credential must be loaded once and reused; a second \
             load can pair surfaces with different identities"
        );
    }

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
                root_id: "p1".to_string(),
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

    /// PER-019 (secrev PR #17 attention-surface finding, 2026-04-29):
    /// the `local_attention_key_for` helper used by `attention show`
    /// must build the lookup key purely from string manipulation —
    /// no network call. This test exercises the heuristic across the
    /// three input shapes (qualified, --team override, bare-name +
    /// primary-team default) and asserts the expected key shape.
    /// The fact that the helper is `fn` (not `async fn`) and takes no
    /// network handle is itself the static guarantee; this test
    /// pins the behavior so a future refactor can't silently
    /// re-introduce a network call without breaking the test.
    #[test]
    fn secrev_pr17_attention_show_local_key_no_network() {
        // Qualified `<team>/<channel>` passes through.
        assert_eq!(
            local_attention_key_for("org-lanytehq", "3-leaps-operations/development", None),
            "3-leaps-operations/development"
        );
        // Qualified with leading `#` on channel segment is normalized.
        assert_eq!(
            local_attention_key_for("org-lanytehq", "3-leaps-operations/#development", None),
            "3-leaps-operations/development"
        );
        // --team override wins over primary-team default.
        assert_eq!(
            local_attention_key_for("org-lanytehq", "general", Some("3-leaps-operations")),
            "3-leaps-operations/general"
        );
        // Bare name defaults to primary team (the strict-read-only
        // trade-off: cross-team cursors require explicit
        // disambiguation here, mirroring `attention_list`).
        assert_eq!(
            local_attention_key_for("org-lanytehq", "bravo-team", None),
            "org-lanytehq/bravo-team"
        );
        // Leading `#` on bare name is also trimmed.
        assert_eq!(
            local_attention_key_for("org-lanytehq", "#bravo-team", None),
            "org-lanytehq/bravo-team"
        );
    }

    #[test]
    fn filter_all_monitored_matches_inbound_message() {
        let event = inbound_event("per-004", false);
        assert!(event_matches_filter(
            &event,
            &SubscriptionFilter::AllMonitored
        ));
    }

    /// PER-019 (devrev PR #17 second-pass regression): when two
    /// channels share a name across different teams (e.g.
    /// `org-lanytehq/general` and `3-leaps-operations/general`),
    /// the push-backed wait must wake only on events for the
    /// resolved `channel_id`, never on a name-collision from the
    /// other team. Pre-fix, the predicate compared by `channel_name`
    /// and would wake on either; post-fix, only the matching id
    /// wakes.
    #[test]
    fn devrev_pr17_finding5_wait_push_backed_filters_by_channel_id() {
        fn payload(channel_id: &str, channel_name: &str, post_id: &str) -> InboundEventPayload {
            InboundEventPayload {
                profile: "test".to_string(),
                provider: Provider::Mattermost,
                channel_id: channel_id.to_string(),
                channel_name: channel_name.to_string(),
                post_id: post_id.to_string(),
                root_id: post_id.to_string(),
                sender_id: "u-other".to_string(),
                sender_username: "alice".to_string(),
                message: "hello".to_string(),
                create_at: 2000,
                received_at: 2001,
                mentioned: false,
            }
        }

        // Wait was set up for the Ops team's #general (id=ch-ops-general).
        let wait_channel_id = "ch-ops-general";
        let cursor_id = "p-cursor";
        let cursor_create_at = 1000;
        let my_user_id = "bot-bravo";

        // Event from the SAME team (matching id) → wake.
        let ops_event = payload("ch-ops-general", "general", "p-ops-1");
        assert!(
            inbound_event_wakes_wait(
                &ops_event,
                wait_channel_id,
                cursor_id,
                cursor_create_at,
                my_user_id,
            ),
            "matching channel_id should wake the wait"
        );

        // Event from the OTHER team (same name, different id) → must NOT wake.
        let lh_event = payload("ch-lanytehq-general", "general", "p-lh-1");
        assert!(
            !inbound_event_wakes_wait(
                &lh_event,
                wait_channel_id,
                cursor_id,
                cursor_create_at,
                my_user_id,
            ),
            "same-named channel on a different team must not wake the wait"
        );

        // Self-authored event (matching id) → must NOT wake (existing
        // contract; preserved by the fix).
        let mut self_event = payload("ch-ops-general", "general", "p-self");
        self_event.sender_id = my_user_id.to_string();
        assert!(
            !inbound_event_wakes_wait(
                &self_event,
                wait_channel_id,
                cursor_id,
                cursor_create_at,
                my_user_id,
            ),
            "self-authored event must not wake the wait"
        );
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
