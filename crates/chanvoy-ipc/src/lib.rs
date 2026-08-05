use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use chanvoy_core::{
    Channel, CoreError, DaemonEvent, DaemonEventKind, DaemonEventPayloadInner, EventBus,
    MattermostClient, Message, Profile, SubscriptionFilter,
};
use ipcprims::frame::Frame;
use ipcprims::peer::{async_connect_with_config, AsyncPeerTx, HandshakeConfig, PeerConfig};
use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

const CHANNEL_260: u16 = 260;
const CHANNEL_3: u16 = 3;

pub const CHAT_CHANNEL: u16 = CHANNEL_260;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ChatErrorCode {
    NotFound,
    PermissionDenied,
    DelegationExpired,
    ProviderError,
    RateLimited,
    InvalidRequest,
    SendBlocked,
    SubscriptionNotFound,
    UnsupportedOperation,
    ChannelNotJoined,
    /// PER-014: post-bind whoami probe (or any later daemon_status call)
    /// caught the bot identity diverging from the configured bot_username.
    /// Network-backed IPC requests refuse with this code while the drift
    /// bit is set; subscription event forwarding is suppressed; local
    /// socket and `daemon_status` remain queryable so operators can
    /// re-run `chanvoy auto-setup` to re-validate identity. Per
    /// @agent-entarch-lanytehq's PR #16 finding (2026-04-28).
    IdentityDrift,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type")]
pub enum ChatFrame {
    #[serde(rename = "chat_channel_list_request")]
    ChannelListRequest {
        request_id: String,
        delegation_id: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        team_id: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        include_archived: Option<bool>,
    },
    #[serde(rename = "chat_channel_list_response")]
    ChannelListResponse {
        request_id: String,
        channels: Vec<ChannelSummary>,
    },
    #[serde(rename = "chat_channel_get_request")]
    ChannelGetRequest {
        request_id: String,
        delegation_id: String,
        channel_id: String,
    },
    #[serde(rename = "chat_channel_get_response")]
    ChannelGetResponse {
        request_id: String,
        channel: ChannelSummary,
    },
    #[serde(rename = "chat_read_request")]
    ReadRequest {
        request_id: String,
        delegation_id: String,
        channel_id: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        thread_root_id: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        limit: Option<u32>,
    },
    #[serde(rename = "chat_read_response")]
    ReadResponse {
        request_id: String,
        channel_id: String,
        posts: Vec<PostSummary>,
        #[serde(skip_serializing_if = "Option::is_none")]
        has_more: Option<bool>,
    },
    #[serde(rename = "chat_post_request")]
    PostRequest {
        request_id: String,
        delegation_id: String,
        channel_id: String,
        message: String,
        gate_token: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        thread_root_id: Option<String>,
    },
    #[serde(rename = "chat_post_response")]
    PostResponse {
        request_id: String,
        post: PostSummary,
    },
    #[serde(rename = "chat_subscribe_request")]
    SubscribeRequest {
        request_id: String,
        delegation_id: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        filter: Option<IpcSubscriptionFilter>,
        #[serde(skip_serializing_if = "Option::is_none")]
        resume_after_seq: Option<u64>,
    },
    #[serde(rename = "chat_subscribe_response")]
    SubscribeResponse {
        request_id: String,
        subscription_id: String,
        starting_seq: u64,
    },
    #[serde(rename = "chat_unsubscribe_request")]
    UnsubscribeRequest {
        request_id: String,
        delegation_id: String,
        subscription_id: String,
    },
    #[serde(rename = "chat_unsubscribe_response")]
    UnsubscribeResponse { request_id: String, success: bool },
    #[serde(rename = "chat_event_notification")]
    EventNotification {
        subscription_id: String,
        seq: u64,
        event_kind: String,
        occurred_at: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        channel_id: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        post: Option<PostSummary>,
        #[serde(skip_serializing_if = "Option::is_none")]
        mentions_bot: Option<bool>,
        #[serde(skip_serializing_if = "Option::is_none")]
        actor: Option<String>,
    },
    #[serde(rename = "chat_subscription_gap")]
    SubscriptionGap {
        subscription_id: String,
        expected_seq: u64,
        next_seq: u64,
        reason: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        message: Option<String>,
    },
    #[serde(rename = "chat_error")]
    Error {
        request_id: String,
        error_code: ChatErrorCode,
        message: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        retryable: Option<bool>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ChannelSummary {
    pub channel_id: String,
    pub name: String,
    pub kind: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub display_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub team_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PostSummary {
    pub post_id: String,
    pub channel_id: String,
    pub author: String,
    pub created_at: String,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub thread_root_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct IpcSubscriptionFilter {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub channel_ids: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub event_kinds: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mentions_only: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AuditEvent {
    #[serde(rename = "type")]
    pub event_type: String,
    pub entry_id: String,
    pub peer_id: String,
    pub timestamp: String,
    pub action: String,
    pub actor: String,
    pub prev_hash: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub details: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub severity: Option<String>,
}

pub fn core_error_to_chat(error: CoreError, request_id: &str) -> ChatFrame {
    let (code, retryable) = match &error {
        CoreError::WaitTimeout(_) => (ChatErrorCode::NotFound, Some(false)),
        CoreError::ProfileNotFound(_) => (ChatErrorCode::NotFound, Some(false)),
        // Every cause of an empty thread body is permanent: the post was
        // deleted, it sits in a channel this identity cannot read, or the
        // id is not a post id. Falling through to the retryable default
        // would tell an automated caller to retry a read that can never
        // succeed — and this surface is consumed by agents, which honor
        // that flag literally.
        CoreError::EmptyThread { .. } => (ChatErrorCode::NotFound, Some(false)),
        // A post that does not exist, and a post that exists in some
        // other channel, are both permanent answers to the question
        // that was asked. Retrying changes neither.
        //
        // Both report not-found rather than distinguishing the two:
        // "this post is not in this channel" is the honest answer to
        // the caller's question, and it does not confirm the existence
        // of a post the caller named a channel it may not be able to
        // read.
        CoreError::AnchorNotFound(_) | CoreError::AnchorChannelMismatch { .. } => {
            (ChatErrorCode::NotFound, Some(false))
        }
        CoreError::RequiresElevatedCapability => (ChatErrorCode::PermissionDenied, Some(false)),
        CoreError::Api { status, .. } => match *status {
            s if s == reqwest::StatusCode::UNAUTHORIZED || s == reqwest::StatusCode::FORBIDDEN => {
                (ChatErrorCode::PermissionDenied, Some(false))
            }
            s if s == reqwest::StatusCode::NOT_FOUND => (ChatErrorCode::NotFound, Some(false)),
            s if s == reqwest::StatusCode::TOO_MANY_REQUESTS => {
                (ChatErrorCode::RateLimited, Some(true))
            }
            _ => (ChatErrorCode::ProviderError, Some(true)),
        },
        _ => (ChatErrorCode::ProviderError, Some(true)),
    };
    ChatFrame::Error {
        request_id: request_id.to_string(),
        error_code: code,
        message: error.to_string(),
        retryable,
    }
}

pub fn translate_m2_filter_to_260(filter: &SubscriptionFilter) -> IpcSubscriptionFilter {
    match filter {
        SubscriptionFilter::AllMonitored => IpcSubscriptionFilter {
            channel_ids: None,
            event_kinds: Some(vec!["message_posted".to_string(), "mention".to_string()]),
            mentions_only: None,
        },
        SubscriptionFilter::ChannelByName(name) => IpcSubscriptionFilter {
            channel_ids: Some(vec![name.clone()]),
            event_kinds: Some(vec!["message_posted".to_string()]),
            mentions_only: None,
        },
        SubscriptionFilter::MentionsOnly => IpcSubscriptionFilter {
            channel_ids: None,
            event_kinds: Some(vec!["mention".to_string()]),
            mentions_only: Some(true),
        },
        SubscriptionFilter::ConnectionState => IpcSubscriptionFilter {
            channel_ids: None,
            event_kinds: Some(vec!["channel_updated".to_string()]),
            mentions_only: None,
        },
    }
}

pub fn translate_260_filter_to_m2(filter: &IpcSubscriptionFilter) -> SubscriptionFilter {
    if filter.mentions_only == Some(true) {
        return SubscriptionFilter::MentionsOnly;
    }
    if let Some(ids) = &filter.channel_ids {
        if ids.len() == 1 {
            return SubscriptionFilter::ChannelByName(ids[0].clone());
        }
    }
    SubscriptionFilter::AllMonitored
}

pub fn daemon_event_to_chat_notification(
    event: &DaemonEvent,
    subscription_id: &str,
) -> Option<ChatFrame> {
    match &event.payload {
        DaemonEventPayloadInner::Inbound(p) => Some(ChatFrame::EventNotification {
            subscription_id: subscription_id.to_string(),
            seq: event.seq,
            event_kind: match event.kind {
                DaemonEventKind::InboundMessage => "message_posted".to_string(),
                DaemonEventKind::InboundMention => "mention".to_string(),
                _ => return None,
            },
            occurred_at: format_rfc3339(p.create_at),
            channel_id: Some(p.channel_id.clone()),
            post: Some(PostSummary {
                post_id: p.post_id.clone(),
                channel_id: p.channel_id.clone(),
                author: p.sender_username.clone(),
                created_at: format_rfc3339(p.create_at),
                message: p.message.clone(),
                // Same non-empty rule as the read path. Push events are
                // how a subscribed caller learns a message exists, so
                // dropping the thread root here would leave it with no
                // way to reply without a second round trip — and no way
                // at all to reply correctly to a reply.
                thread_root_id: non_empty_root(&p.root_id),
            }),
            mentions_bot: Some(p.mentioned),
            actor: Some(p.sender_id.clone()),
        }),
        DaemonEventPayloadInner::ConnectionStateChanged(_) => Some(ChatFrame::EventNotification {
            subscription_id: subscription_id.to_string(),
            seq: event.seq,
            event_kind: "channel_updated".to_string(),
            occurred_at: format_rfc3339(chanvoy_core::now_unix_millis()),
            channel_id: None,
            post: None,
            mentions_bot: None,
            actor: None,
        }),
        DaemonEventPayloadInner::Gap(_) => None,
    }
}

fn event_matches_ipc_filter(event: &DaemonEvent, filter: &IpcSubscriptionFilter) -> bool {
    if let DaemonEventPayloadInner::Gap(_) = &event.payload {
        return true;
    }
    if let Some(kinds) = &filter.event_kinds {
        let kind_str = match event.kind {
            DaemonEventKind::InboundMessage => "message_posted",
            DaemonEventKind::InboundMention => "mention",
            DaemonEventKind::ConnectionStateChanged => "channel_updated",
            _ => return false,
        };
        if !kinds.iter().any(|k| k == kind_str) {
            return false;
        }
    }
    if filter.mentions_only == Some(true) {
        if let DaemonEventPayloadInner::Inbound(p) = &event.payload {
            if !p.mentioned {
                return false;
            }
        }
    }
    if let Some(channel_ids) = &filter.channel_ids {
        if let DaemonEventPayloadInner::Inbound(p) = &event.payload {
            if !channel_ids.iter().any(|c| c == &p.channel_id) {
                return false;
            }
        }
    }
    true
}

fn format_rfc3339(millis: i64) -> String {
    let secs = millis / 1000;
    let subsec_millis = (millis % 1000) as u32;
    chrono::DateTime::from_timestamp(secs, subsec_millis * 1_000_000)
        .map(|dt| dt.to_rfc3339())
        .unwrap_or_else(|| "1970-01-01T00:00:00.000Z".to_string())
}

fn channel_to_summary(ch: &Channel) -> ChannelSummary {
    ChannelSummary {
        channel_id: ch.id.clone(),
        name: ch.name.clone(),
        kind: "public".to_string(),
        display_name: Some(ch.display_name.clone()),
        team_id: None,
    }
}

fn message_to_post_summary(msg: &Message, channel_id: &str) -> PostSummary {
    PostSummary {
        post_id: msg.id.clone(),
        channel_id: channel_id.to_string(),
        author: msg.username.clone(),
        created_at: format_rfc3339(msg.create_at),
        message: msg.message.clone(),
        thread_root_id: thread_root_of(msg),
    }
}

/// The thread a message belongs to, or `None` when we genuinely do not
/// know. A message read from a current chanvoy always names its thread
/// (a top-level post names itself); an empty value only happens on a
/// message that came from an older daemon, and reporting that as
/// `None` is more honest than inventing a root.
fn thread_root_of(msg: &Message) -> Option<String> {
    non_empty_root(&msg.root_id)
}

/// A thread root is absent only when it is genuinely empty, which now
/// means the value came from an older daemon that did not report one.
/// Shared by the read and push paths so the two cannot disagree about
/// what "no thread root" means.
fn non_empty_root(root_id: &str) -> Option<String> {
    if root_id.is_empty() {
        None
    } else {
        Some(root_id.to_string())
    }
}

struct SubscriptionEntry {
    filter: IpcSubscriptionFilter,
}

#[derive(Debug, Clone)]
pub struct IpcPeerState {
    pub connected: bool,
    pub peer_id: Option<String>,
    pub reconnect_count: u64,
}

pub struct IpcPeer {
    client: MattermostClient,
    event_bus: Arc<EventBus>,
    #[allow(dead_code)]
    profile: Profile,
    gateway_socket: String,
    state: Arc<tokio::sync::Mutex<IpcPeerState>>,
    subscriptions: Arc<tokio::sync::Mutex<HashMap<String, SubscriptionEntry>>>,
    /// PER-014: shared drift signal from the daemon's `AppState`. When
    /// set, network-backed IPC requests refuse with `IdentityDrift` and
    /// event forwarding to subscribers is suppressed. Local control /
    /// audit / subscription-management responses still flow. Per
    /// @agent-entarch-lanytehq's PR #16 finding (2026-04-28).
    identity_drift: Arc<AtomicBool>,
}

impl IpcPeer {
    pub fn new(
        profile: &Profile,
        client: MattermostClient,
        event_bus: Arc<EventBus>,
        gateway_socket: String,
        identity_drift: Arc<AtomicBool>,
    ) -> Self {
        Self {
            client,
            event_bus,
            profile: profile.clone(),
            gateway_socket,
            state: Arc::new(tokio::sync::Mutex::new(IpcPeerState {
                connected: false,
                peer_id: None,
                reconnect_count: 0,
            })),
            subscriptions: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
            identity_drift,
        }
    }

    /// PER-014: build a drift-refused error frame for a given request_id.
    /// Used by every network-backed IPC handler at the top of its body
    /// to short-circuit when the drift bit is set.
    fn drift_refusal(request_id: String) -> ChatFrame {
        ChatFrame::Error {
            request_id,
            error_code: ChatErrorCode::IdentityDrift,
            message: "identity drift detected: configured bot_username does not match the \
                Mattermost-returned username for this token; network-backed IPC requests \
                are refused. Inspect daemon_status.mattermost_identity_drift and re-run \
                `chanvoy auto-setup` to re-validate identity."
                .to_string(),
            retryable: Some(true),
        }
    }

    pub fn state(&self) -> Arc<tokio::sync::Mutex<IpcPeerState>> {
        Arc::clone(&self.state)
    }

    pub async fn run(self: Arc<Self>, cancel: CancellationToken) {
        let mut attempt: u64 = 0;
        loop {
            if cancel.is_cancelled() {
                break;
            }
            attempt += 1;

            match self.connect_and_serve(cancel.clone()).await {
                Ok(()) => {
                    info!("ipc peer session ended cleanly");
                }
                Err(e) => {
                    warn!(%e, "ipc peer session error");
                }
            }

            {
                let mut s = self.state.lock().await;
                s.connected = false;
                s.peer_id = None;
                s.reconnect_count += 1;
            }

            if cancel.is_cancelled() {
                break;
            }

            let delay = if attempt <= 3 {
                std::time::Duration::from_secs(1 << attempt.min(3))
            } else {
                std::time::Duration::from_secs(30)
            };

            tokio::select! {
                _ = tokio::time::sleep(delay) => {}
                _ = cancel.cancelled() => {
                    break;
                }
            }
        }
    }

    async fn connect_and_serve(
        &self,
        cancel: CancellationToken,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let handshake_config = HandshakeConfig {
            auth_token: None,
            ..Default::default()
        };
        let peer_config = PeerConfig {
            enable_any_delivery: false,
            ..Default::default()
        };

        let peer = async_connect_with_config(
            &self.gateway_socket,
            &[CHANNEL_260, CHANNEL_3],
            &handshake_config,
            None,
            Some(peer_config),
            None,
        )
        .await?;

        let peer_id = peer.id().to_string();
        info!(%peer_id, "ipc peer connected to gateway");

        {
            let mut s = self.state.lock().await;
            s.connected = true;
            s.peer_id = Some(peer_id.clone());
        }

        let (tx, mut rx) = peer.into_split();
        let mut channel_rx = rx.take_channel_receiver(CHANNEL_260);

        let event_bus = Arc::clone(&self.event_bus);
        let mut event_rx = event_bus.subscribe();
        let tx_clone = tx.clone();
        let subscriptions = Arc::clone(&self.subscriptions);
        let cancel_fwd = cancel.clone();
        let audit_peer_id = peer_id.clone();
        let drift_for_fwd = Arc::clone(&self.identity_drift);

        let forward_handle = tokio::spawn(async move {
            loop {
                tokio::select! {
                    recv_result = event_rx.recv() => {
                        match recv_result {
                            Ok(event) => {
                                // PER-014: drop Mattermost-sourced events
                                // when identity drift is set. Same contract
                                // as the local UDS subscription path —
                                // operators query daemon_status to learn
                                // why events stopped, then re-run
                                // `chanvoy auto-setup`. Per
                                // @agent-entarch-lanytehq's PR #16
                                // finding (2026-04-28).
                                if drift_for_fwd.load(Ordering::Relaxed) {
                                    continue;
                                }
                                let subs = subscriptions.lock().await;
                                for (sub_id, entry) in subs.iter() {
                                    if event_matches_ipc_filter(&event, &entry.filter) {
                                        if let DaemonEventPayloadInner::Gap(g) = &event.payload {
                                            let gap = ChatFrame::SubscriptionGap {
                                                subscription_id: sub_id.clone(),
                                                expected_seq: g.missed_from_seq,
                                                next_seq: g.missed_to_seq,
                                                reason: "overflow".to_string(),
                                                message: Some(format!(
                                                    "missed {} events",
                                                    g.missed_to_seq.saturating_sub(g.missed_from_seq)
                                                )),
                                            };
                                            let payload = serde_json::to_vec(&gap).unwrap_or_default();
                                            if tx_clone.send(CHANNEL_260, &payload).await.is_err() {
                                                return;
                                            }
                                        } else if let Some(notification) = daemon_event_to_chat_notification(&event, sub_id) {
                                            let payload = serde_json::to_vec(&notification).unwrap_or_default();
                                            if tx_clone.send(CHANNEL_260, &payload).await.is_err() {
                                                return;
                                            }
                                        }
                                    }
                                }
                            }
                            Err(_) => return,
                        }
                    }
                    _ = cancel_fwd.cancelled() => return,
                }
            }
        });

        emit_audit(&tx, &audit_peer_id, "chat.peer_connected", None, "info").await;

        loop {
            tokio::select! {
                frame_result = async {
                    match channel_rx.as_mut() {
                        Some(rx) => rx.recv().await,
                        None => std::future::pending().await,
                    }
                } => {
                    match frame_result {
                        Ok(frame) => {
                            self.handle_frame(frame, &tx, &audit_peer_id).await;
                        }
                        Err(_) => {
                            break;
                        }
                    }
                }
                _ = cancel.cancelled() => {
                    break;
                }
            }
        }

        emit_audit(&tx, &audit_peer_id, "chat.peer_disconnected", None, "info").await;

        forward_handle.abort();
        let _ = tx.shutdown().await;
        Ok(())
    }

    async fn handle_frame(&self, frame: Frame, tx: &AsyncPeerTx, peer_id: &str) {
        let Ok(chat_frame) = serde_json::from_slice::<ChatFrame>(&frame.payload) else {
            return;
        };

        match chat_frame {
            ChatFrame::ChannelListRequest {
                request_id,
                delegation_id,
                team_id: _,
                include_archived,
            } => {
                if self.identity_drift.load(Ordering::Relaxed) {
                    let _ = self
                        .send_response(tx, &Self::drift_refusal(request_id))
                        .await;
                    return;
                }
                let _ = &delegation_id;
                let result = self.client.list_channels().await;
                let response = match result {
                    Ok(channels) => {
                        let channels: Vec<ChannelSummary> = channels
                            .iter()
                            .filter(|c| {
                                include_archived.unwrap_or(false) || !c.name.starts_with("__")
                            })
                            .map(channel_to_summary)
                            .collect();
                        ChatFrame::ChannelListResponse {
                            request_id,
                            channels,
                        }
                    }
                    Err(e) => core_error_to_chat(e, &request_id),
                };
                let _ = self.send_response(tx, &response).await;
            }
            ChatFrame::ReadRequest {
                request_id,
                delegation_id,
                channel_id,
                thread_root_id,
                limit,
            } => {
                if self.identity_drift.load(Ordering::Relaxed) {
                    let _ = self
                        .send_response(tx, &Self::drift_refusal(request_id))
                        .await;
                    return;
                }
                let _ = &delegation_id;
                let response = if let Some(root_id) = &thread_root_id {
                    self.thread_read_response(request_id, channel_id, root_id, limit)
                        .await
                } else {
                    let since = chanvoy_core::now_unix_millis() - (30 * 60 * 1000);
                    let result = self
                        .client
                        .read_channel_by_id_since_millis(&channel_id, since)
                        .await;
                    match result {
                        Ok(messages) => {
                            let limit = limit.unwrap_or(50) as usize;
                            let posts: Vec<PostSummary> = messages
                                .into_iter()
                                .take(limit)
                                .map(|m| message_to_post_summary(&m, &channel_id))
                                .collect();
                            ChatFrame::ReadResponse {
                                request_id,
                                channel_id,
                                posts,
                                has_more: None,
                            }
                        }
                        Err(e) => core_error_to_chat(e, &request_id),
                    }
                };
                let _ = self.send_response(tx, &response).await;
            }
            ChatFrame::PostRequest {
                request_id,
                delegation_id,
                channel_id,
                message,
                gate_token,
                thread_root_id,
            } => {
                if self.identity_drift.load(Ordering::Relaxed) {
                    let _ = self
                        .send_response(tx, &Self::drift_refusal(request_id))
                        .await;
                    return;
                }
                let _ = &delegation_id;
                if gate_token.is_empty() {
                    let err = ChatFrame::Error {
                        request_id,
                        error_code: ChatErrorCode::PermissionDenied,
                        message: "gate_token is required for post operations".to_string(),
                        retryable: Some(false),
                    };
                    let _ = self.send_response(tx, &err).await;
                    return;
                }
                let result = if let Some(root_id) = &thread_root_id {
                    self.client
                        .post_threaded_reply(&channel_id, root_id, &message)
                        .await
                } else {
                    self.client.post_message_by_id(&channel_id, &message).await
                };
                let response = match result {
                    Ok(receipt) => {
                        emit_audit(
                            tx,
                            peer_id,
                            if thread_root_id.is_some() {
                                "chat.thread_reply"
                            } else {
                                "chat.post"
                            },
                            Some(serde_json::json!({
                                "channel_id": &channel_id,
                                "post_id": &receipt.id,
                                "has_thread_root": thread_root_id.is_some(),
                            })),
                            "notice",
                        )
                        .await;
                        ChatFrame::PostResponse {
                            request_id,
                            post: PostSummary {
                                post_id: receipt.id,
                                channel_id: channel_id.clone(),
                                author: String::new(),
                                created_at: format_rfc3339(chanvoy_core::now_unix_millis()),
                                message,
                                thread_root_id,
                            },
                        }
                    }
                    Err(e) => core_error_to_chat(e, &request_id),
                };
                let _ = self.send_response(tx, &response).await;
            }
            ChatFrame::ChannelGetRequest {
                request_id,
                delegation_id,
                channel_id,
            } => {
                if self.identity_drift.load(Ordering::Relaxed) {
                    let _ = self
                        .send_response(tx, &Self::drift_refusal(request_id))
                        .await;
                    return;
                }
                let _ = &delegation_id;
                let result = self.client.list_channels().await;
                let response = match result {
                    Ok(channels) => {
                        let found = channels.iter().find(|c| c.id == channel_id);
                        match found {
                            Some(ch) => ChatFrame::ChannelGetResponse {
                                request_id,
                                channel: channel_to_summary(ch),
                            },
                            None => ChatFrame::Error {
                                request_id,
                                error_code: ChatErrorCode::NotFound,
                                message: format!("channel {channel_id} not found"),
                                retryable: Some(false),
                            },
                        }
                    }
                    Err(e) => core_error_to_chat(e, &request_id),
                };
                let _ = self.send_response(tx, &response).await;
            }
            ChatFrame::SubscribeRequest {
                request_id,
                delegation_id,
                filter,
                resume_after_seq,
            } => {
                if self.identity_drift.load(Ordering::Relaxed) {
                    let _ = self
                        .send_response(tx, &Self::drift_refusal(request_id))
                        .await;
                    return;
                }
                let _ = &delegation_id;
                let sub_id = uuid::Uuid::new_v4().to_string();
                let start_seq = self.event_bus.current_seq();
                let ipc_filter = filter.unwrap_or(IpcSubscriptionFilter {
                    channel_ids: None,
                    event_kinds: None,
                    mentions_only: None,
                });

                if let Some(resume_seq) = resume_after_seq {
                    if resume_seq < start_seq {
                        let gap = ChatFrame::SubscriptionGap {
                            subscription_id: sub_id.clone(),
                            expected_seq: resume_seq,
                            next_seq: start_seq,
                            reason: "history_unavailable".to_string(),
                            message: Some(format!(
                                "missed {} events between seq {} and {}",
                                start_seq.saturating_sub(resume_seq),
                                resume_seq,
                                start_seq
                            )),
                        };
                        let _ = self.send_response(tx, &gap).await;
                    }
                }

                {
                    let mut subs = self.subscriptions.lock().await;
                    subs.insert(sub_id.clone(), SubscriptionEntry { filter: ipc_filter });
                }

                emit_audit(
                    tx,
                    peer_id,
                    "chat.subscribe",
                    Some(serde_json::json!({ "subscription_id": &sub_id })),
                    "info",
                )
                .await;

                let response = ChatFrame::SubscribeResponse {
                    request_id,
                    subscription_id: sub_id,
                    starting_seq: start_seq,
                };
                let _ = self.send_response(tx, &response).await;
            }
            ChatFrame::UnsubscribeRequest {
                request_id,
                delegation_id,
                subscription_id,
            } => {
                let _ = &delegation_id;
                let removed = {
                    let mut subs = self.subscriptions.lock().await;
                    subs.remove(&subscription_id).is_some()
                };
                if removed {
                    emit_audit(
                        tx,
                        peer_id,
                        "chat.unsubscribe",
                        Some(serde_json::json!({ "subscription_id": &subscription_id })),
                        "info",
                    )
                    .await;
                }
                let response = ChatFrame::UnsubscribeResponse {
                    request_id,
                    success: removed,
                };
                let _ = self.send_response(tx, &response).await;
            }
            _ => {}
        }
    }

    /// The thread branch of a read request.
    ///
    /// The anchor point-fetch is what binds the thread to the channel
    /// the caller named. Without it the channel id on the request is
    /// decoration — the thread would be read on the strength of the
    /// post id alone, and every summary would then be stamped with
    /// whatever channel the caller claimed, which is a read of any
    /// thread the bot can see. A refusal here issues no thread request
    /// at all.
    ///
    /// Separate from `handle_frame` so that contract can be exercised
    /// without standing up a peer transport.
    async fn thread_read_response(
        &self,
        request_id: String,
        channel_id: String,
        root_id: &str,
        limit: Option<u32>,
    ) -> ChatFrame {
        // This peer speaks in channel ids, so the id doubles as the
        // operator-facing channel name in a refusal.
        let anchor = match self
            .client
            .get_post_in_channel(&channel_id, &channel_id, root_id)
            .await
        {
            Ok(anchor) => anchor,
            Err(e) => return core_error_to_chat(e, &request_id),
        };
        // The anchor's root is canonical, so naming any reply in the
        // thread reads the whole thread.
        let messages = match self.client.read_thread(&anchor.root_id).await {
            Ok(messages) => messages,
            Err(e) => return core_error_to_chat(e, &request_id),
        };
        let limit = limit.unwrap_or(50) as usize;
        let posts: Vec<PostSummary> = messages
            .into_iter()
            .take(limit)
            .map(|m| PostSummary {
                post_id: m.id.clone(),
                channel_id: channel_id.clone(),
                author: m.username.clone(),
                created_at: format_rfc3339(m.create_at),
                message: m.message.clone(),
                thread_root_id: thread_root_of(&m),
            })
            .collect();
        ChatFrame::ReadResponse {
            request_id,
            channel_id,
            posts,
            has_more: None,
        }
    }

    async fn send_response(
        &self,
        tx: &AsyncPeerTx,
        frame: &ChatFrame,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let payload = serde_json::to_vec(frame)?;
        tx.send(CHANNEL_260, &payload).await?;
        Ok(())
    }
}

async fn emit_audit(
    tx: &AsyncPeerTx,
    peer_id: &str,
    action: &str,
    details: Option<serde_json::Value>,
    severity: &str,
) {
    let event = AuditEvent {
        event_type: "audit_event".to_string(),
        entry_id: uuid::Uuid::new_v4().to_string(),
        peer_id: peer_id.to_string(),
        timestamp: format_rfc3339(chanvoy_core::now_unix_millis()),
        action: action.to_string(),
        actor: "chanvoy".to_string(),
        prev_hash: "0".repeat(64),
        details,
        severity: Some(severity.to_string()),
    };
    if let Ok(payload) = serde_json::to_vec(&event) {
        let _ = tx.send(CHANNEL_3, &payload).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn core_error_maps_permission_denied() {
        let err = CoreError::RequiresElevatedCapability;
        let frame = core_error_to_chat(err, "test-1");
        match frame {
            ChatFrame::Error { error_code, .. } => {
                assert_eq!(error_code, ChatErrorCode::PermissionDenied);
            }
            _ => panic!("expected error frame"),
        }
    }

    #[test]
    fn core_error_maps_not_found() {
        let err = CoreError::WaitTimeout("ch".to_string());
        let frame = core_error_to_chat(err, "test-2");
        match frame {
            ChatFrame::Error {
                error_code,
                retryable,
                ..
            } => {
                assert_eq!(error_code, ChatErrorCode::NotFound);
                assert_eq!(retryable, Some(false));
            }
            _ => panic!("expected error frame"),
        }
    }

    /// A channel-binding refusal is permanent, and must not invite a
    /// retry. It is also reported as not-found rather than as a
    /// distinct mismatch, so a refusal does not confirm that the named
    /// post exists somewhere the caller cannot see.
    #[test]
    fn a_binding_refusal_is_terminal_and_indistinguishable_from_not_found() {
        for err in [
            CoreError::AnchorNotFound("p-1".to_string()),
            CoreError::AnchorChannelMismatch {
                post_id: "p-1".to_string(),
                channel: "somewhere-else".to_string(),
            },
        ] {
            let frame = core_error_to_chat(err, "test-bind");
            match frame {
                ChatFrame::Error {
                    error_code,
                    retryable,
                    ..
                } => {
                    assert_eq!(error_code, ChatErrorCode::NotFound);
                    assert_eq!(
                        retryable,
                        Some(false),
                        "a post cannot move into the requested channel on a retry"
                    );
                }
                _ => panic!("expected error frame"),
            }
        }
    }

    /// An empty thread body is permanent, so it must not be advertised
    /// as retryable. Automated callers honor that flag literally, and a
    /// read that can never succeed would be retried forever.
    #[test]
    fn an_empty_thread_is_a_terminal_not_found_not_a_retryable_provider_error() {
        let err = CoreError::EmptyThread {
            root_id: "root-1".to_string(),
        };
        let frame = core_error_to_chat(err, "test-empty-thread");
        match frame {
            ChatFrame::Error {
                error_code,
                retryable,
                ..
            } => {
                assert_eq!(error_code, ChatErrorCode::NotFound);
                assert_eq!(
                    retryable,
                    Some(false),
                    "retrying an empty thread cannot ever change the answer"
                );
            }
            _ => panic!("expected error frame"),
        }
    }

    #[test]
    fn core_error_maps_api_404() {
        let err = CoreError::Api {
            status: reqwest::StatusCode::NOT_FOUND,
            message: "not found".to_string(),
        };
        let frame = core_error_to_chat(err, "test-3");
        match frame {
            ChatFrame::Error { error_code, .. } => {
                assert_eq!(error_code, ChatErrorCode::NotFound);
            }
            _ => panic!("expected error frame"),
        }
    }

    #[test]
    fn core_error_maps_api_429() {
        let err = CoreError::Api {
            status: reqwest::StatusCode::TOO_MANY_REQUESTS,
            message: "rate limited".to_string(),
        };
        let frame = core_error_to_chat(err, "test-4");
        match frame {
            ChatFrame::Error {
                error_code,
                retryable,
                ..
            } => {
                assert_eq!(error_code, ChatErrorCode::RateLimited);
                assert_eq!(retryable, Some(true));
            }
            _ => panic!("expected error frame"),
        }
    }

    #[test]
    fn core_error_maps_generic_as_provider_error() {
        let err = CoreError::Io(std::io::Error::new(std::io::ErrorKind::BrokenPipe, "pipe"));
        let frame = core_error_to_chat(err, "test-5");
        match frame {
            ChatFrame::Error { error_code, .. } => {
                assert_eq!(error_code, ChatErrorCode::ProviderError);
            }
            _ => panic!("expected error frame"),
        }
    }

    #[test]
    fn translate_m2_all_monitored() {
        let f = translate_m2_filter_to_260(&SubscriptionFilter::AllMonitored);
        assert!(f.event_kinds.is_some());
        assert!(f.mentions_only.is_none());
    }

    #[test]
    fn translate_m2_channel_by_name() {
        let f =
            translate_m2_filter_to_260(&SubscriptionFilter::ChannelByName("per-005".to_string()));
        assert_eq!(f.channel_ids, Some(vec!["per-005".to_string()]));
    }

    #[test]
    fn translate_m2_mentions_only() {
        let f = translate_m2_filter_to_260(&SubscriptionFilter::MentionsOnly);
        assert_eq!(f.mentions_only, Some(true));
    }

    #[test]
    fn translate_260_mentions_only_roundtrip() {
        let f = IpcSubscriptionFilter {
            channel_ids: None,
            event_kinds: None,
            mentions_only: Some(true),
        };
        let m2 = translate_260_filter_to_m2(&f);
        assert_eq!(m2, SubscriptionFilter::MentionsOnly);
    }

    #[test]
    fn translate_260_channel_by_name_roundtrip() {
        let f = IpcSubscriptionFilter {
            channel_ids: Some(vec!["per-005".to_string()]),
            event_kinds: Some(vec!["message_posted".to_string()]),
            mentions_only: None,
        };
        let m2 = translate_260_filter_to_m2(&f);
        assert_eq!(m2, SubscriptionFilter::ChannelByName("per-005".to_string()));
    }

    #[test]
    fn translate_260_default_is_all_monitored() {
        let f = IpcSubscriptionFilter {
            channel_ids: None,
            event_kinds: Some(vec!["message_posted".to_string()]),
            mentions_only: None,
        };
        let m2 = translate_260_filter_to_m2(&f);
        assert_eq!(m2, SubscriptionFilter::AllMonitored);
    }

    #[test]
    fn chat_frame_serialization_roundtrip() {
        let frame = ChatFrame::ChannelListRequest {
            request_id: "abc-123".to_string(),
            delegation_id: "del-1".to_string(),
            team_id: None,
            include_archived: None,
        };
        let json = serde_json::to_string(&frame).unwrap();
        assert!(json.contains("\"chat_channel_list_request\""));
        let parsed: ChatFrame = serde_json::from_str(&json).unwrap();
        assert_eq!(frame, parsed);
    }

    /// A pushed reply must arrive carrying the thread it belongs to.
    ///
    /// The regression this guards is narrow and easy to reintroduce:
    /// the root is normalized upstream and carried correctly on the
    /// read path, then dropped here, at the one boundary a subscribed
    /// caller actually receives live events on. A caller that then
    /// replies using the post id is rejected by the provider, because
    /// a reply cannot be the target of another reply — so the failure
    /// surfaces at write time, far from this function.
    #[test]
    fn a_pushed_reply_carries_its_thread_root() {
        let event = DaemonEvent {
            seq: 7,
            kind: DaemonEventKind::InboundMessage,
            payload: DaemonEventPayloadInner::Inbound(chanvoy_core::InboundEventPayload {
                profile: "test".to_string(),
                provider: chanvoy_core::Provider::Mattermost,
                channel_id: "ch1".to_string(),
                channel_name: "general".to_string(),
                post_id: "reply-9".to_string(),
                root_id: "root-1".to_string(),
                sender_id: "u1".to_string(),
                sender_username: "alice".to_string(),
                message: "a reply".to_string(),
                create_at: 1000,
                received_at: 1001,
                mentioned: false,
            }),
        };
        let frame = daemon_event_to_chat_notification(&event, "sub-1").expect("notification frame");
        let ChatFrame::EventNotification { post, .. } = frame else {
            panic!("expected an event notification");
        };
        let post = post.expect("notification carries a post summary");
        assert_eq!(
            post.thread_root_id,
            Some("root-1".to_string()),
            "a pushed reply must name the thread root, not its own id and not nothing"
        );
        assert_ne!(
            post.thread_root_id,
            Some(post.post_id.clone()),
            "a reply must not be reported as its own thread root"
        );
    }

    /// A pushed top-level post is the root of its own thread, so it
    /// still names a usable reply target rather than nothing.
    #[test]
    fn a_pushed_top_level_post_is_its_own_thread_root() {
        let event = DaemonEvent {
            seq: 8,
            kind: DaemonEventKind::InboundMessage,
            payload: DaemonEventPayloadInner::Inbound(chanvoy_core::InboundEventPayload {
                profile: "test".to_string(),
                provider: chanvoy_core::Provider::Mattermost,
                channel_id: "ch1".to_string(),
                channel_name: "general".to_string(),
                post_id: "post-1".to_string(),
                root_id: "post-1".to_string(),
                sender_id: "u1".to_string(),
                sender_username: "alice".to_string(),
                message: "top level".to_string(),
                create_at: 1000,
                received_at: 1001,
                mentioned: false,
            }),
        };
        let frame = daemon_event_to_chat_notification(&event, "sub-1").expect("notification frame");
        let ChatFrame::EventNotification { post, .. } = frame else {
            panic!("expected an event notification");
        };
        assert_eq!(
            post.expect("post summary").thread_root_id,
            Some("post-1".to_string())
        );
    }

    #[test]
    fn ipc_filter_matches_all_event_kinds() {
        let event = DaemonEvent {
            seq: 1,
            kind: DaemonEventKind::InboundMessage,
            payload: DaemonEventPayloadInner::Inbound(chanvoy_core::InboundEventPayload {
                profile: "test".to_string(),
                provider: chanvoy_core::Provider::Mattermost,
                channel_id: "ch1".to_string(),
                channel_name: "general".to_string(),
                post_id: "p1".to_string(),
                root_id: "p1".to_string(),
                sender_id: "u1".to_string(),
                sender_username: "alice".to_string(),
                message: "hi".to_string(),
                create_at: 1000,
                received_at: 1001,
                mentioned: false,
            }),
        };
        let filter = IpcSubscriptionFilter {
            channel_ids: None,
            event_kinds: Some(vec!["message_posted".to_string()]),
            mentions_only: None,
        };
        assert!(event_matches_ipc_filter(&event, &filter));
    }

    #[test]
    fn ipc_filter_rejects_wrong_kind() {
        let event = DaemonEvent {
            seq: 1,
            kind: DaemonEventKind::InboundMessage,
            payload: DaemonEventPayloadInner::Inbound(chanvoy_core::InboundEventPayload {
                profile: "test".to_string(),
                provider: chanvoy_core::Provider::Mattermost,
                channel_id: "ch1".to_string(),
                channel_name: "general".to_string(),
                post_id: "p1".to_string(),
                root_id: "p1".to_string(),
                sender_id: "u1".to_string(),
                sender_username: "alice".to_string(),
                message: "hi".to_string(),
                create_at: 1000,
                received_at: 1001,
                mentioned: false,
            }),
        };
        let filter = IpcSubscriptionFilter {
            channel_ids: None,
            event_kinds: Some(vec!["mention".to_string()]),
            mentions_only: None,
        };
        assert!(!event_matches_ipc_filter(&event, &filter));
    }

    #[test]
    fn ipc_filter_channel_id_scoping() {
        let event = DaemonEvent {
            seq: 1,
            kind: DaemonEventKind::InboundMessage,
            payload: DaemonEventPayloadInner::Inbound(chanvoy_core::InboundEventPayload {
                profile: "test".to_string(),
                provider: chanvoy_core::Provider::Mattermost,
                channel_id: "ch1".to_string(),
                channel_name: "general".to_string(),
                post_id: "p1".to_string(),
                root_id: "p1".to_string(),
                sender_id: "u1".to_string(),
                sender_username: "alice".to_string(),
                message: "hi".to_string(),
                create_at: 1000,
                received_at: 1001,
                mentioned: false,
            }),
        };
        let matching = IpcSubscriptionFilter {
            channel_ids: Some(vec!["ch1".to_string()]),
            event_kinds: None,
            mentions_only: None,
        };
        let non_matching = IpcSubscriptionFilter {
            channel_ids: Some(vec!["ch2".to_string()]),
            event_kinds: None,
            mentions_only: None,
        };
        assert!(event_matches_ipc_filter(&event, &matching));
        assert!(!event_matches_ipc_filter(&event, &non_matching));
    }

    #[test]
    fn ipc_filter_mentions_only_rejects_non_mention() {
        let event = DaemonEvent {
            seq: 1,
            kind: DaemonEventKind::InboundMessage,
            payload: DaemonEventPayloadInner::Inbound(chanvoy_core::InboundEventPayload {
                profile: "test".to_string(),
                provider: chanvoy_core::Provider::Mattermost,
                channel_id: "ch1".to_string(),
                channel_name: "general".to_string(),
                post_id: "p1".to_string(),
                root_id: "p1".to_string(),
                sender_id: "u1".to_string(),
                sender_username: "alice".to_string(),
                message: "hi".to_string(),
                create_at: 1000,
                received_at: 1001,
                mentioned: false,
            }),
        };
        let filter = IpcSubscriptionFilter {
            channel_ids: None,
            event_kinds: None,
            mentions_only: Some(true),
        };
        assert!(!event_matches_ipc_filter(&event, &filter));
    }

    #[test]
    fn audit_event_serialization() {
        let event = AuditEvent {
            event_type: "audit_event".to_string(),
            entry_id: uuid::Uuid::new_v4().to_string(),
            peer_id: "chanvoy-1".to_string(),
            timestamp: "2026-04-08T16:00:00+00:00".to_string(),
            action: "chat.post".to_string(),
            actor: "chanvoy".to_string(),
            prev_hash: "0".repeat(64),
            details: Some(serde_json::json!({"channel_id": "ch1"})),
            severity: Some("notice".to_string()),
        };
        let json = serde_json::to_string(&event).unwrap();
        assert!(json.contains("\"audit_event\""));
        let parsed: AuditEvent = serde_json::from_str(&json).unwrap();
        assert_eq!(event, parsed);
    }

    #[test]
    fn audit_actions_are_not_double_prefixed() {
        let action = "chat.post";
        let event = AuditEvent {
            event_type: "audit_event".to_string(),
            entry_id: uuid::Uuid::new_v4().to_string(),
            peer_id: "chanvoy-1".to_string(),
            timestamp: "2026-04-08T16:00:00+00:00".to_string(),
            action: action.to_string(),
            actor: "chanvoy".to_string(),
            prev_hash: "0".repeat(64),
            details: None,
            severity: Some("info".to_string()),
        };

        assert_eq!(event.action, action);
    }

    #[test]
    fn gap_events_pass_ipc_filter() {
        let event = DaemonEvent {
            seq: 10,
            kind: DaemonEventKind::Gap,
            payload: DaemonEventPayloadInner::Gap(chanvoy_core::GapPayload {
                subscription_id: "m2-sub".to_string(),
                missed_from_seq: 5,
                missed_to_seq: 10,
            }),
        };
        let filter = IpcSubscriptionFilter {
            channel_ids: None,
            event_kinds: Some(vec!["message_posted".to_string()]),
            mentions_only: None,
        };
        assert!(event_matches_ipc_filter(&event, &filter));
    }

    #[test]
    fn subscription_gap_frame_uses_ipc_sub_id() {
        let gap = ChatFrame::SubscriptionGap {
            subscription_id: "ipc-sub-1".to_string(),
            expected_seq: 5,
            next_seq: 10,
            reason: "history_unavailable".to_string(),
            message: Some("missed 5 events between seq 5 and 10".to_string()),
        };
        let json = serde_json::to_string(&gap).unwrap();
        assert!(json.contains("ipc-sub-1"));
        assert!(json.contains("history_unavailable"));
        let parsed: ChatFrame = serde_json::from_str(&json).unwrap();
        assert_eq!(gap, parsed);
    }

    #[test]
    fn drift_refusal_frame_carries_identity_drift_code() {
        // PER-014 (entarch finding #1): IPC drift gate refuses
        // network-backed requests with the new IdentityDrift code.
        // Verify the frame shape, retryable hint, and roundtrip.
        let frame = IpcPeer::drift_refusal("req-123".to_string());
        match &frame {
            ChatFrame::Error {
                request_id,
                error_code,
                message,
                retryable,
            } => {
                assert_eq!(request_id, "req-123");
                assert_eq!(*error_code, ChatErrorCode::IdentityDrift);
                assert!(
                    message.contains("identity drift"),
                    "diagnostic should name the failure mode"
                );
                assert!(
                    message.contains("auto-setup"),
                    "diagnostic should point at the recovery action"
                );
                // retryable=Some(true) because re-running auto-setup
                // can clear the drift bit; this is not a permanent
                // permission denial.
                assert_eq!(*retryable, Some(true));
            }
            other => panic!("expected Error frame, got {other:?}"),
        }
        // Roundtrip the new error code through serde so wire-format
        // consumers see "identity_drift" snake_case.
        let json = serde_json::to_string(&frame).unwrap();
        assert!(json.contains("identity_drift"), "json={json}");
        let parsed: ChatFrame = serde_json::from_str(&json).unwrap();
        assert_eq!(frame, parsed);
    }

    // ------------------------------------------------------------------
    // Thread reads are bound to the channel the caller named
    // ------------------------------------------------------------------

    mod thread_read {
        use super::*;
        use chanvoy_core::{CapabilityClass, CredentialMode, Provider};
        use wiremock::matchers::{method as http_method, path as http_path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        const CALLER_CHANNEL: &str = "channel-the-caller-named";
        const OTHER_CHANNEL: &str = "channel-the-caller-may-not-read";
        const ROOT_ID: &str = "thread-root";

        fn peer_against(mock_url: &str) -> IpcPeer {
            let profile = Profile {
                name: "thread-read".to_string(),
                role: "bravo-devlead".to_string(),
                scope: "lanytehq".to_string(),
                provider: Provider::Mattermost,
                bot_username: "bot-stable".to_string(),
                team_name: "team-slug-stable".to_string(),
                server_url: mock_url.to_string(),
                env_name: "LANYTE_MM_TOKEN".to_string(),
                env_file: None,
                credential_mode: CredentialMode::EnvName,
                capability_class: CapabilityClass::Standard,
                monitored_channels: vec![],
                ipc: None,
                reduce: None,
            };
            let client = MattermostClient::new(&profile, "fixture-token".to_string())
                .expect("build MattermostClient");
            IpcPeer::new(
                &profile,
                client,
                Arc::new(EventBus::new(16)),
                "unused-gateway-socket".to_string(),
                Arc::new(AtomicBool::new(false)),
            )
        }

        /// One post in the shape the provider actually sends.
        fn wire_post(
            id: &str,
            channel_id: &str,
            user_id: &str,
            create_at: i64,
            root_id: &str,
        ) -> serde_json::Value {
            serde_json::json!({
                "id": id,
                "channel_id": channel_id,
                "user_id": user_id,
                "message": format!("body of {id}"),
                "create_at": create_at,
                "root_id": root_id,
            })
        }

        fn posts_envelope(posts: Vec<serde_json::Value>) -> serde_json::Value {
            let order: Vec<String> = posts
                .iter()
                .map(|p| p["id"].as_str().unwrap().to_string())
                .collect();
            let map: serde_json::Map<String, serde_json::Value> = posts
                .into_iter()
                .map(|p| (p["id"].as_str().unwrap().to_string(), p))
                .collect();
            serde_json::json!({ "order": order, "posts": map })
        }

        async fn mount_post(server: &MockServer, post: serde_json::Value) {
            let id = post["id"].as_str().unwrap().to_string();
            Mock::given(http_method("GET"))
                .and(http_path(format!("/api/v4/posts/{id}")))
                .respond_with(ResponseTemplate::new(200).set_body_json(post))
                .mount(server)
                .await;
        }

        async fn mount_thread(server: &MockServer, root_id: &str, body: serde_json::Value) {
            Mock::given(http_method("GET"))
                .and(http_path(format!("/api/v4/posts/{root_id}/thread")))
                .respond_with(ResponseTemplate::new(200).set_body_json(body))
                .mount(server)
                .await;
        }

        async fn mount_user(server: &MockServer, user_id: &str, username: &str) {
            Mock::given(http_method("GET"))
                .and(http_path(format!("/api/v4/users/{user_id}")))
                .respond_with(
                    ResponseTemplate::new(200)
                        .set_body_json(serde_json::json!({"id": user_id, "username": username})),
                )
                .mount(server)
                .await;
        }

        async fn thread_requests(server: &MockServer) -> usize {
            server
                .received_requests()
                .await
                .expect("wiremock received_requests")
                .iter()
                .filter(|req| req.url.path().ends_with("/thread"))
                .count()
        }

        /// A thread that lives in the caller's channel reads normally,
        /// and every summary names the thread it belongs to.
        #[tokio::test]
        async fn a_thread_in_the_named_channel_reads_and_every_post_names_its_thread() {
            let server = MockServer::start().await;
            mount_post(
                &server,
                wire_post(ROOT_ID, CALLER_CHANNEL, "user-a", 1_700_000_000_000, ""),
            )
            .await;
            mount_thread(
                &server,
                ROOT_ID,
                posts_envelope(vec![
                    wire_post(ROOT_ID, CALLER_CHANNEL, "user-a", 1_700_000_000_000, ""),
                    wire_post(
                        "reply-1",
                        CALLER_CHANNEL,
                        "user-b",
                        1_700_000_001_000,
                        ROOT_ID,
                    ),
                ]),
            )
            .await;
            mount_user(&server, "user-a", "alice").await;
            mount_user(&server, "user-b", "bob").await;
            let peer = peer_against(&server.uri());

            let frame = peer
                .thread_read_response(
                    "req-ok".to_string(),
                    CALLER_CHANNEL.to_string(),
                    ROOT_ID,
                    None,
                )
                .await;

            match frame {
                ChatFrame::ReadResponse {
                    request_id,
                    channel_id,
                    posts,
                    ..
                } => {
                    assert_eq!(request_id, "req-ok");
                    assert_eq!(channel_id, CALLER_CHANNEL);
                    assert_eq!(
                        posts.iter().map(|p| p.post_id.as_str()).collect::<Vec<_>>(),
                        vec![ROOT_ID, "reply-1"]
                    );
                    assert_eq!(
                        posts.iter().map(|p| p.author.as_str()).collect::<Vec<_>>(),
                        vec!["alice", "bob"]
                    );
                    for post in &posts {
                        assert_eq!(
                            post.thread_root_id.as_deref(),
                            Some(ROOT_ID),
                            "every summary names the thread it is part of: {post:?}"
                        );
                    }
                }
                other => panic!("expected ReadResponse, got {other:?}"),
            }
        }

        /// Naming a reply reads the same thread as naming the root — the
        /// anchor's root is what the thread request is made against.
        #[tokio::test]
        async fn naming_a_reply_reads_the_whole_thread() {
            let server = MockServer::start().await;
            let reply = wire_post(
                "reply-1",
                CALLER_CHANNEL,
                "user-b",
                1_700_000_001_000,
                ROOT_ID,
            );
            mount_post(&server, reply).await;
            mount_thread(
                &server,
                ROOT_ID,
                posts_envelope(vec![
                    wire_post(ROOT_ID, CALLER_CHANNEL, "user-a", 1_700_000_000_000, ""),
                    wire_post(
                        "reply-1",
                        CALLER_CHANNEL,
                        "user-b",
                        1_700_000_001_000,
                        ROOT_ID,
                    ),
                ]),
            )
            .await;
            // Deliberately NOT mounted: /posts/reply-1/thread. Asking
            // the provider for the thread of the id the caller passed,
            // rather than the canonical root, 404s and fails this test.
            mount_user(&server, "user-a", "alice").await;
            mount_user(&server, "user-b", "bob").await;
            let peer = peer_against(&server.uri());

            let frame = peer
                .thread_read_response(
                    "req-reply".to_string(),
                    CALLER_CHANNEL.to_string(),
                    "reply-1",
                    None,
                )
                .await;

            match frame {
                ChatFrame::ReadResponse { posts, .. } => assert_eq!(
                    posts.iter().map(|p| p.post_id.as_str()).collect::<Vec<_>>(),
                    vec![ROOT_ID, "reply-1"],
                    "a reply id reads the same thread its root does"
                ),
                other => panic!("expected ReadResponse, got {other:?}"),
            }
        }

        /// A post that lives in another channel is refused, and the
        /// thread is never asked for. Asserting only on the error would
        /// pass even if the bind ran after the fetch.
        #[tokio::test]
        async fn a_thread_in_another_channel_is_refused_before_any_thread_request() {
            let server = MockServer::start().await;
            mount_post(
                &server,
                wire_post(ROOT_ID, OTHER_CHANNEL, "user-a", 1_700_000_000_000, ""),
            )
            .await;
            // Mounted and answerable on purpose: the point is that it is
            // never called, not that calling it would fail.
            mount_thread(
                &server,
                ROOT_ID,
                posts_envelope(vec![wire_post(
                    ROOT_ID,
                    OTHER_CHANNEL,
                    "user-a",
                    1_700_000_000_000,
                    "",
                )]),
            )
            .await;
            mount_user(&server, "user-a", "alice").await;
            let peer = peer_against(&server.uri());

            let frame = peer
                .thread_read_response(
                    "req-mismatch".to_string(),
                    CALLER_CHANNEL.to_string(),
                    ROOT_ID,
                    None,
                )
                .await;

            match &frame {
                ChatFrame::Error { message, .. } => assert!(
                    message.contains(ROOT_ID) && message.contains(CALLER_CHANNEL),
                    "refusal names the post and the channel it is not in: {message}"
                ),
                other => panic!("expected Error frame, got {other:?}"),
            }
            let rendered = serde_json::to_string(&frame).expect("serialize frame");
            assert!(
                !rendered.contains("body of"),
                "no post body may leak on a refusal: {rendered}"
            );
            assert_eq!(
                thread_requests(&server).await,
                0,
                "a cross-channel thread read must issue no thread request at all"
            );
        }

        /// A post that does not exist is refused, and again nothing is
        /// asked of the thread endpoint.
        #[tokio::test]
        async fn a_missing_anchor_is_refused_before_any_thread_request() {
            let server = MockServer::start().await;
            Mock::given(http_method("GET"))
                .and(http_path(format!("/api/v4/posts/{ROOT_ID}")))
                .respond_with(
                    ResponseTemplate::new(404)
                        .set_body_json(serde_json::json!({"status_code": 404})),
                )
                .mount(&server)
                .await;
            mount_thread(
                &server,
                ROOT_ID,
                posts_envelope(vec![wire_post(
                    ROOT_ID,
                    CALLER_CHANNEL,
                    "user-a",
                    1_700_000_000_000,
                    "",
                )]),
            )
            .await;
            let peer = peer_against(&server.uri());

            let frame = peer
                .thread_read_response(
                    "req-missing".to_string(),
                    CALLER_CHANNEL.to_string(),
                    ROOT_ID,
                    None,
                )
                .await;

            match &frame {
                // Code and retryability are whatever `core_error_to_chat`
                // already assigns an anchor failure; this test is about
                // the refusal happening at all, and happening first.
                ChatFrame::Error { message, .. } => assert!(
                    message.contains(ROOT_ID),
                    "refusal names the post it could not find: {message}"
                ),
                other => panic!("expected Error frame, got {other:?}"),
            }
            assert_eq!(
                thread_requests(&server).await,
                0,
                "a missing anchor must issue no thread request at all"
            );
        }
    }
}
