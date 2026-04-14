use std::collections::BTreeMap;
use std::env;
use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
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
    pub since_minutes: u64,
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
    pub since_minutes: u64,
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
    dirs::config_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join("lanytehq/chanvoy")
}

pub fn default_runtime_dir() -> PathBuf {
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
}
