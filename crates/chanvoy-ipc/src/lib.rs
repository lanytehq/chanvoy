use std::sync::Arc;

use chanvoy_core::{
    Channel, CoreError, DaemonEvent, DaemonEventKind, DaemonEventPayloadInner, EventBus,
    MattermostClient, Message, Profile, SubscriptionFilter,
};
use ipcprims::peer::{
    async_connect_with_config, AsyncPeerTx, HandshakeConfig, PeerConfig,
};
use ipcprims::frame::Frame;
use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

const CHANNEL_260: u16 = 260;

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
    UnsubscribeResponse {
        request_id: String,
        success: bool,
    },
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

pub fn core_error_to_chat(error: CoreError, request_id: &str) -> ChatFrame {
    let (code, retryable) = match &error {
        CoreError::WaitTimeout(_) => (ChatErrorCode::NotFound, Some(false)),
        CoreError::ProfileNotFound(_) => (ChatErrorCode::NotFound, Some(false)),
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
            event_kinds: Some(vec![
                "message_posted".to_string(),
                "mention".to_string(),
            ]),
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
                thread_root_id: None,
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
        DaemonEventPayloadInner::Gap(g) => Some(ChatFrame::SubscriptionGap {
            subscription_id: g.subscription_id.clone(),
            expected_seq: g.missed_from_seq,
            next_seq: g.missed_to_seq,
            reason: "overflow".to_string(),
            message: Some(format!("missed {} events", g.missed_to_seq.saturating_sub(g.missed_from_seq))),
        }),
    }
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
        thread_root_id: None,
    }
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
}

impl IpcPeer {
    pub fn new(
        profile: &Profile,
        client: MattermostClient,
        event_bus: Arc<EventBus>,
        gateway_socket: String,
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
            &[CHANNEL_260],
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
        let _cancel_fwd = cancel.clone();

        let forward_handle = tokio::spawn(async move {
            let mut ipc_sub_id = String::new();
            while let Ok(event) = event_rx.recv().await {
                if ipc_sub_id.is_empty() {
                    ipc_sub_id = "__ipc_forward__".to_string();
                }
                if let Some(notification) = daemon_event_to_chat_notification(&event, &ipc_sub_id) {
                    let payload = serde_json::to_vec(&notification).unwrap_or_default();
                    if tx_clone.send(CHANNEL_260, &payload).await.is_err() {
                        break;
                    }
                }
            }
        });

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
                            self.handle_frame(frame, &tx).await;
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

        forward_handle.abort();
        let _ = tx.shutdown().await;
        Ok(())
    }

    async fn handle_frame(&self, frame: Frame, tx: &AsyncPeerTx) {
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
                let _ = &delegation_id;
                let result = self.client.list_channels().await;
                let response = match result {
                    Ok(channels) => {
                        let channels: Vec<ChannelSummary> = channels
                            .iter()
                            .filter(|c| include_archived.unwrap_or(false) || !c.name.starts_with("__"))
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
                thread_root_id: _,
                limit,
            } => {
                let _ = &delegation_id;
                let since = chanvoy_core::now_unix_millis() - (30 * 60 * 1000);
                let result = self
                    .client
                    .read_channel_by_id_since_millis(&channel_id, since)
                    .await;
                let response = match result {
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
                let _ = &thread_root_id;
                let result = self.client.post_message_by_id(&channel_id, &message).await;
                let response = match result {
                    Ok(receipt) => ChatFrame::PostResponse {
                        request_id,
                        post: PostSummary {
                            post_id: receipt.id,
                            channel_id: channel_id.clone(),
                            author: String::new(),
                            created_at: format_rfc3339(chanvoy_core::now_unix_millis()),
                            message,
                            thread_root_id,
                        },
                    },
                    Err(e) => core_error_to_chat(e, &request_id),
                };
                let _ = self.send_response(tx, &response).await;
            }
            ChatFrame::ChannelGetRequest {
                request_id,
                delegation_id,
                channel_id,
            } => {
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
                filter: _,
                resume_after_seq: _,
            } => {
                let _ = &delegation_id;
                let sub_id = uuid::Uuid::new_v4().to_string();
                let start_seq = self.event_bus.current_seq();
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
                let _ = &subscription_id;
                let response = ChatFrame::UnsubscribeResponse {
                    request_id,
                    success: true,
                };
                let _ = self.send_response(tx, &response).await;
            }
            _ => {}
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
            ChatFrame::Error { error_code, retryable, .. } => {
                assert_eq!(error_code, ChatErrorCode::NotFound);
                assert_eq!(retryable, Some(false));
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
            ChatFrame::Error { error_code, retryable, .. } => {
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
        let f = translate_m2_filter_to_260(&SubscriptionFilter::ChannelByName("per-005".to_string()));
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
}
