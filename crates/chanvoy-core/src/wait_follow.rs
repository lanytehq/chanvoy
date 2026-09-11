//! `wait_follow_v1` / `wait_follow_v2` daemon-RPC and JSONL stream types.
//!
//! The normative schemas live in Crucible under
//! `schemas/common/chanvoy-daemon-rpc/v0/`. Chanvoy does not git-pin
//! Crucible; these types are the local contract.

use std::collections::HashSet;

use serde::{Deserialize, Serialize};

use crate::Message;

pub const WAIT_FOLLOW_V1_METHOD: &str = "wait_follow_v1";
pub const WAIT_FOLLOW_V1_EVENT_METHOD: &str = "wait_follow_v1.event";
pub const WAIT_FOLLOW_V1_EVENT_SCHEMA: &str = "wait_follow_v1.event";
pub const WAIT_FOLLOW_V2_METHOD: &str = "wait_follow_v2";
pub const WAIT_FOLLOW_V2_EVENT_METHOD: &str = "wait_follow_v2.event";
pub const WAIT_FOLLOW_V2_EVENT_SCHEMA: &str = "wait_follow_v2.event";
pub const WAIT_FOLLOW_COALESCE_MS_MIN: u64 = 1;
pub const WAIT_FOLLOW_COALESCE_MS_MAX: u64 = 10_000;
pub const WAIT_FOLLOW_COALESCE_MAX_MESSAGES: usize = 32;

/// Refuse a v2 `coalesce_ms` outside `1..=10000`.
pub fn validate_coalesce_ms(coalesce_ms: u64) -> Result<(), String> {
    if (WAIT_FOLLOW_COALESCE_MS_MIN..=WAIT_FOLLOW_COALESCE_MS_MAX).contains(&coalesce_ms) {
        Ok(())
    } else {
        Err(format!(
            "coalesce_ms must be {WAIT_FOLLOW_COALESCE_MS_MIN}..={WAIT_FOLLOW_COALESCE_MS_MAX}"
        ))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct WaitFollowV1Params {
    pub channel: String,
    pub timeout_secs: u64,
    #[serde(default)]
    pub team: Option<String>,
    #[serde(default)]
    pub contains: Option<String>,
    #[serde(default)]
    pub pattern: Option<String>,
    #[serde(default)]
    pub after: Option<String>,
    #[serde(default)]
    pub replace_wait_id: Option<String>,
    /// When true, only posts that mention this bot complete the wait.
    #[serde(default)]
    pub mention: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct WaitFollowV2Params {
    pub channel: String,
    pub timeout_secs: u64,
    #[serde(default)]
    pub team: Option<String>,
    #[serde(default)]
    pub contains: Option<String>,
    #[serde(default)]
    pub pattern: Option<String>,
    #[serde(default)]
    pub after: Option<String>,
    #[serde(default)]
    pub replace_wait_id: Option<String>,
    /// When true, only posts that mention this bot complete the wait.
    #[serde(default)]
    pub mention: bool,
    /// Required live coalesce window in milliseconds (`1..=10000`).
    pub coalesce_ms: u64,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum WaitFollowSchema {
    #[serde(rename = "wait_follow_v1.event")]
    V1,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum WaitFollowMode {
    Armed,
    Backlog,
    Live,
    Deadman,
    Canceled,
    Replaced,
    Failed,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct WaitFollowEvent {
    pub schema: WaitFollowSchema,
    pub wait_id: String,
    #[serde(flatten)]
    pub kind: WaitFollowEventKind,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "mode", rename_all = "snake_case", deny_unknown_fields)]
pub enum WaitFollowEventKind {
    Armed {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        replaced_wait_id: Option<String>,
    },
    Backlog {
        tip: String,
        truncated: bool,
        messages: [Message; 1],
    },
    Live {
        tip: String,
        truncated: bool,
        messages: [Message; 1],
    },
    Deadman,
    Canceled,
    Replaced {
        replaced_by_wait_id: String,
    },
    Failed {
        reason_code: WaitFollowFailureReason,
    },
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum WaitFollowFailureReason {
    ProviderFailed,
    ProviderOverflow,
    ProviderOutage,
    CursorUncertain,
    ProviderDegraded,
    OwnershipLost,
    DaemonShutdown,
}

impl WaitFollowEvent {
    pub fn armed(wait_id: impl Into<String>, replaced_wait_id: Option<String>) -> Self {
        Self {
            schema: WaitFollowSchema::V1,
            wait_id: wait_id.into(),
            kind: WaitFollowEventKind::Armed { replaced_wait_id },
        }
    }

    pub fn message(
        wait_id: impl Into<String>,
        mode: WaitFollowMode,
        message: Message,
        truncated: bool,
    ) -> Result<Self, &'static str> {
        if !is_mattermost_post_id(&message.id) {
            return Err("follow message id is not a Mattermost post id");
        }
        let tip = message.id.clone();
        let kind = match mode {
            WaitFollowMode::Backlog => WaitFollowEventKind::Backlog {
                tip,
                truncated,
                messages: [message],
            },
            WaitFollowMode::Live if !truncated => WaitFollowEventKind::Live {
                tip,
                truncated: false,
                messages: [message],
            },
            WaitFollowMode::Live => return Err("live follow records cannot be truncated"),
            _ => return Err("message record requires backlog or live mode"),
        };
        Ok(Self {
            schema: WaitFollowSchema::V1,
            wait_id: wait_id.into(),
            kind,
        })
    }

    pub fn terminal(wait_id: impl Into<String>, kind: WaitFollowEventKind) -> Self {
        debug_assert!(matches!(
            kind,
            WaitFollowEventKind::Deadman
                | WaitFollowEventKind::Canceled
                | WaitFollowEventKind::Replaced { .. }
                | WaitFollowEventKind::Failed { .. }
        ));
        Self {
            schema: WaitFollowSchema::V1,
            wait_id: wait_id.into(),
            kind,
        }
    }

    pub fn mode(&self) -> WaitFollowMode {
        match self.kind {
            WaitFollowEventKind::Armed { .. } => WaitFollowMode::Armed,
            WaitFollowEventKind::Backlog { .. } => WaitFollowMode::Backlog,
            WaitFollowEventKind::Live { .. } => WaitFollowMode::Live,
            WaitFollowEventKind::Deadman => WaitFollowMode::Deadman,
            WaitFollowEventKind::Canceled => WaitFollowMode::Canceled,
            WaitFollowEventKind::Replaced { .. } => WaitFollowMode::Replaced,
            WaitFollowEventKind::Failed { .. } => WaitFollowMode::Failed,
        }
    }

    pub fn tip(&self) -> Option<&str> {
        match &self.kind {
            WaitFollowEventKind::Backlog { tip, .. } | WaitFollowEventKind::Live { tip, .. } => {
                Some(tip)
            }
            _ => None,
        }
    }

    pub fn messages(&self) -> &[Message] {
        match &self.kind {
            WaitFollowEventKind::Backlog { messages, .. }
            | WaitFollowEventKind::Live { messages, .. } => messages,
            _ => &[],
        }
    }

    pub fn validate(&self) -> Result<(), &'static str> {
        validate_wait_id(&self.wait_id)?;
        match &self.kind {
            WaitFollowEventKind::Armed { replaced_wait_id } => {
                if let Some(wait_id) = replaced_wait_id {
                    validate_wait_id(wait_id)?;
                }
            }
            WaitFollowEventKind::Backlog { tip, messages, .. } => {
                validate_message_record(tip, &messages[0])?
            }
            WaitFollowEventKind::Live {
                tip,
                truncated,
                messages,
            } => {
                if *truncated {
                    return Err("live follow records cannot be truncated");
                }
                validate_message_record(tip, &messages[0])?;
            }
            WaitFollowEventKind::Replaced {
                replaced_by_wait_id,
            } => validate_wait_id(replaced_by_wait_id)?,
            WaitFollowEventKind::Deadman
            | WaitFollowEventKind::Canceled
            | WaitFollowEventKind::Failed { .. } => {}
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum WaitFollowV2Schema {
    #[serde(rename = "wait_follow_v2.event")]
    V2,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct WaitFollowV2Event {
    pub schema: WaitFollowV2Schema,
    pub wait_id: String,
    #[serde(flatten)]
    pub kind: WaitFollowV2EventKind,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "mode", rename_all = "snake_case", deny_unknown_fields)]
pub enum WaitFollowV2EventKind {
    Armed {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        replaced_wait_id: Option<String>,
    },
    Backlog {
        tip: String,
        truncated: bool,
        messages: Vec<Message>,
    },
    Live {
        tip: String,
        truncated: bool,
        messages: Vec<Message>,
    },
    Deadman,
    Canceled,
    Replaced {
        replaced_by_wait_id: String,
    },
    Failed {
        reason_code: WaitFollowFailureReason,
    },
}

impl WaitFollowV2Event {
    pub fn armed(wait_id: impl Into<String>, replaced_wait_id: Option<String>) -> Self {
        Self {
            schema: WaitFollowV2Schema::V2,
            wait_id: wait_id.into(),
            kind: WaitFollowV2EventKind::Armed { replaced_wait_id },
        }
    }

    pub fn messages(
        wait_id: impl Into<String>,
        mode: WaitFollowMode,
        messages: Vec<Message>,
        truncated: bool,
    ) -> Result<Self, &'static str> {
        validate_v2_message_record(
            messages
                .last()
                .map(|message| message.id.as_str())
                .unwrap_or_default(),
            &messages,
        )?;
        let tip = messages
            .last()
            .expect("validate_v2_message_record requires 1..=32 messages")
            .id
            .clone();
        let kind = match mode {
            WaitFollowMode::Backlog if !truncated => WaitFollowV2EventKind::Backlog {
                tip,
                truncated: false,
                messages,
            },
            WaitFollowMode::Live if !truncated => WaitFollowV2EventKind::Live {
                tip,
                truncated: false,
                messages,
            },
            WaitFollowMode::Backlog | WaitFollowMode::Live => {
                return Err("v2 follow records cannot be truncated")
            }
            _ => return Err("message record requires backlog or live mode"),
        };
        Ok(Self {
            schema: WaitFollowV2Schema::V2,
            wait_id: wait_id.into(),
            kind,
        })
    }

    pub fn terminal(wait_id: impl Into<String>, kind: WaitFollowV2EventKind) -> Self {
        debug_assert!(matches!(
            kind,
            WaitFollowV2EventKind::Deadman
                | WaitFollowV2EventKind::Canceled
                | WaitFollowV2EventKind::Replaced { .. }
                | WaitFollowV2EventKind::Failed { .. }
        ));
        Self {
            schema: WaitFollowV2Schema::V2,
            wait_id: wait_id.into(),
            kind,
        }
    }

    pub fn mode(&self) -> WaitFollowMode {
        match self.kind {
            WaitFollowV2EventKind::Armed { .. } => WaitFollowMode::Armed,
            WaitFollowV2EventKind::Backlog { .. } => WaitFollowMode::Backlog,
            WaitFollowV2EventKind::Live { .. } => WaitFollowMode::Live,
            WaitFollowV2EventKind::Deadman => WaitFollowMode::Deadman,
            WaitFollowV2EventKind::Canceled => WaitFollowMode::Canceled,
            WaitFollowV2EventKind::Replaced { .. } => WaitFollowMode::Replaced,
            WaitFollowV2EventKind::Failed { .. } => WaitFollowMode::Failed,
        }
    }

    pub fn tip(&self) -> Option<&str> {
        match &self.kind {
            WaitFollowV2EventKind::Backlog { tip, .. }
            | WaitFollowV2EventKind::Live { tip, .. } => Some(tip),
            _ => None,
        }
    }

    pub fn messages_slice(&self) -> &[Message] {
        match &self.kind {
            WaitFollowV2EventKind::Backlog { messages, .. }
            | WaitFollowV2EventKind::Live { messages, .. } => messages,
            _ => &[],
        }
    }

    pub fn validate(&self) -> Result<(), &'static str> {
        validate_wait_id(&self.wait_id)?;
        match &self.kind {
            WaitFollowV2EventKind::Armed { replaced_wait_id } => {
                if let Some(wait_id) = replaced_wait_id {
                    validate_wait_id(wait_id)?;
                }
            }
            WaitFollowV2EventKind::Backlog {
                tip,
                truncated,
                messages,
            }
            | WaitFollowV2EventKind::Live {
                tip,
                truncated,
                messages,
            } => {
                if *truncated {
                    return Err("v2 follow records cannot be truncated");
                }
                validate_v2_message_record(tip, messages)?;
            }
            WaitFollowV2EventKind::Replaced {
                replaced_by_wait_id,
            } => validate_wait_id(replaced_by_wait_id)?,
            WaitFollowV2EventKind::Deadman
            | WaitFollowV2EventKind::Canceled
            | WaitFollowV2EventKind::Failed { .. } => {}
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct WaitFollowResult {
    pub wait_id: String,
    #[serde(flatten)]
    pub kind: WaitFollowResultKind,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "mode", rename_all = "snake_case", deny_unknown_fields)]
pub enum WaitFollowResultKind {
    Deadman {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        tip: Option<String>,
    },
    Replaced {
        replaced_by_wait_id: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        tip: Option<String>,
    },
}

impl WaitFollowResult {
    pub fn mode(&self) -> WaitFollowMode {
        match self.kind {
            WaitFollowResultKind::Deadman { .. } => WaitFollowMode::Deadman,
            WaitFollowResultKind::Replaced { .. } => WaitFollowMode::Replaced,
        }
    }

    pub fn validate(&self) -> Result<(), &'static str> {
        validate_wait_id(&self.wait_id)?;
        match &self.kind {
            WaitFollowResultKind::Deadman { tip } => validate_optional_tip(tip),
            WaitFollowResultKind::Replaced {
                replaced_by_wait_id,
                tip,
            } => {
                validate_wait_id(replaced_by_wait_id)?;
                validate_optional_tip(tip)
            }
        }
    }
}

pub fn is_mattermost_post_id(value: &str) -> bool {
    value.len() == 26
        && value
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit())
}

fn validate_wait_id(value: &str) -> Result<(), &'static str> {
    if value.is_empty() || value.len() > 64 {
        return Err("follow wait id must contain 1 to 64 bytes");
    }
    Ok(())
}

fn validate_optional_tip(tip: &Option<String>) -> Result<(), &'static str> {
    if tip.as_deref().is_none_or(is_mattermost_post_id) {
        Ok(())
    } else {
        Err("follow tip is not a Mattermost post id")
    }
}

fn validate_message_fields(message: &Message) -> Result<(), &'static str> {
    if !is_mattermost_post_id(&message.id) {
        return Err("follow message id is not a Mattermost post id");
    }
    if message.user_id.is_empty()
        || message.username.is_empty()
        || message.root_id.is_empty()
        || message.create_at < 0
    {
        return Err("follow message violates the event document");
    }
    Ok(())
}

fn validate_message_record(tip: &str, message: &Message) -> Result<(), &'static str> {
    validate_message_fields(message)?;
    if !is_mattermost_post_id(tip) || tip != message.id {
        return Err("follow tip must equal its sole Mattermost message id");
    }
    Ok(())
}

/// Strict `(create_at, id)` order and unique ids. Equal keys fail.
pub fn validate_strict_create_at_id_order<'a>(
    keys: impl IntoIterator<Item = (i64, &'a str)>,
) -> Result<(), &'static str> {
    let mut prev: Option<(i64, &'a str)> = None;
    let mut seen = HashSet::new();
    for (create_at, id) in keys {
        if !seen.insert(id) {
            return Err("coalesced messages must have unique ids");
        }
        if let Some((prev_at, prev_id)) = prev {
            if (create_at, id) <= (prev_at, prev_id) {
                return Err("coalesced messages must be in strict (create_at, id) order");
            }
        }
        prev = Some((create_at, id));
    }
    Ok(())
}

fn validate_v2_message_record(tip: &str, messages: &[Message]) -> Result<(), &'static str> {
    if messages.is_empty() || messages.len() > WAIT_FOLLOW_COALESCE_MAX_MESSAGES {
        return Err("follow v2 messages must contain 1 to 32 entries");
    }
    for message in messages {
        validate_message_fields(message)?;
    }
    validate_strict_create_at_id_order(
        messages
            .iter()
            .map(|message| (message.create_at, message.id.as_str())),
    )?;
    let last = &messages[messages.len() - 1];
    if !is_mattermost_post_id(tip) || tip != last.id {
        return Err("follow v2 tip must equal the last Mattermost message id");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn message() -> Message {
        Message {
            id: "postid00000000000000000001".into(),
            user_id: "userid00000000000000000001".into(),
            username: "reviewer".into(),
            message: "ready".into(),
            create_at: 1,
            root_id: "postid00000000000000000001".into(),
            mention_user_ids: None,
        }
    }

    #[test]
    fn params_reject_unknown_fields() {
        let raw = serde_json::json!({
            "channel": "release-floor",
            "timeout_secs": 60,
            "sink_path": "/tmp/must-not-cross-daemon-boundary"
        });
        assert!(serde_json::from_value::<WaitFollowV1Params>(raw).is_err());
    }

    #[test]
    fn armed_is_self_identifying_and_has_no_tip() {
        let value = serde_json::to_value(WaitFollowEvent::armed(
            "wait_0123456789abcdef0123456789abcdef",
            None,
        ))
        .unwrap();
        assert_eq!(value["schema"], WAIT_FOLLOW_V1_EVENT_SCHEMA);
        assert_eq!(value["mode"], "armed");
        assert!(value.get("tip").is_none());
    }

    #[test]
    fn message_tip_is_its_only_message_id() {
        let event = WaitFollowEvent::message(
            "wait_0123456789abcdef0123456789abcdef",
            WaitFollowMode::Live,
            message(),
            false,
        )
        .unwrap();
        let value = serde_json::to_value(event).unwrap();
        assert_eq!(value["tip"], value["messages"][0]["id"]);
        assert_eq!(value["messages"].as_array().unwrap().len(), 1);
    }

    #[test]
    fn live_truncation_and_non_post_ids_are_refused() {
        assert!(WaitFollowEvent::message(
            "wait_0123456789abcdef0123456789abcdef",
            WaitFollowMode::Live,
            message(),
            true,
        )
        .is_err());
        let mut invalid = message();
        invalid.id = "post-1".into();
        assert!(WaitFollowEvent::message(
            "wait_0123456789abcdef0123456789abcdef",
            WaitFollowMode::Backlog,
            invalid,
            false,
        )
        .is_err());
    }

    #[test]
    fn terminal_result_rejects_internal_anchor_tip() {
        let result = WaitFollowResult {
            wait_id: "wait_0123456789abcdef0123456789abcdef".into(),
            kind: WaitFollowResultKind::Deadman {
                tip: Some("anc:empty-at-arm".into()),
            },
        };
        assert!(result.validate().is_err());
    }

    fn message_n(n: u8) -> Message {
        let id = format!("postid000000000000000000{n:02}");
        Message {
            id: id.clone(),
            user_id: "userid00000000000000000001".into(),
            username: "reviewer".into(),
            message: "ready".into(),
            create_at: i64::from(n),
            root_id: id,
            mention_user_ids: None,
        }
    }

    #[test]
    fn v2_params_require_coalesce_ms_and_reject_unknown_fields() {
        let missing = serde_json::json!({
            "channel": "release-floor",
            "timeout_secs": 60
        });
        assert!(serde_json::from_value::<WaitFollowV2Params>(missing).is_err());
        let extra = serde_json::json!({
            "channel": "release-floor",
            "timeout_secs": 60,
            "coalesce_ms": 1000,
            "sink_path": "/tmp/must-not-cross-daemon-boundary"
        });
        assert!(serde_json::from_value::<WaitFollowV2Params>(extra).is_err());
        validate_coalesce_ms(1).unwrap();
        validate_coalesce_ms(8_000).unwrap();
        validate_coalesce_ms(10_000).unwrap();
        assert!(validate_coalesce_ms(0).is_err());
        assert!(validate_coalesce_ms(10_001).is_err());
    }

    #[test]
    fn v1_params_reject_coalesce_ms() {
        let raw = serde_json::json!({
            "channel": "release-floor",
            "timeout_secs": 60,
            "coalesce_ms": 1000
        });
        assert!(serde_json::from_value::<WaitFollowV1Params>(raw).is_err());
    }

    #[test]
    fn v2_message_tip_is_the_last_id() {
        let event = WaitFollowV2Event::messages(
            "wait_0123456789abcdef0123456789abcdef",
            WaitFollowMode::Live,
            vec![message_n(1), message_n(2)],
            false,
        )
        .unwrap();
        let value = serde_json::to_value(&event).unwrap();
        assert_eq!(value["schema"], WAIT_FOLLOW_V2_EVENT_SCHEMA);
        assert_eq!(value["tip"], value["messages"][1]["id"]);
        assert_eq!(value["messages"].as_array().unwrap().len(), 2);
        assert_eq!(value["truncated"], false);
        event.validate().unwrap();
    }

    #[test]
    fn v2_refuses_empty_or_oversize_and_live_truncation() {
        assert!(WaitFollowV2Event::messages(
            "wait_0123456789abcdef0123456789abcdef",
            WaitFollowMode::Live,
            vec![],
            false,
        )
        .is_err());
        let too_many: Vec<Message> = (1..=33).map(message_n).collect();
        assert!(WaitFollowV2Event::messages(
            "wait_0123456789abcdef0123456789abcdef",
            WaitFollowMode::Backlog,
            too_many,
            false,
        )
        .is_err());
        assert!(WaitFollowV2Event::messages(
            "wait_0123456789abcdef0123456789abcdef",
            WaitFollowMode::Live,
            vec![message_n(1)],
            true,
        )
        .is_err());
    }

    #[test]
    fn v2_reversed_pair_fails_validate() {
        let event = WaitFollowV2Event {
            schema: WaitFollowV2Schema::V2,
            wait_id: "wait_0123456789abcdef0123456789abcdef".into(),
            kind: WaitFollowV2EventKind::Live {
                tip: "postid00000000000000000001".into(),
                truncated: false,
                messages: vec![message_n(2), message_n(1)],
            },
        };
        assert!(event.validate().is_err());
        assert!(WaitFollowV2Event::messages(
            "wait_0123456789abcdef0123456789abcdef",
            WaitFollowMode::Live,
            vec![message_n(2), message_n(1)],
            false,
        )
        .is_err());
    }

    #[test]
    fn v2_equal_create_at_must_be_id_ordered() {
        let mut higher_id = message_n(2);
        let mut lower_id = message_n(1);
        higher_id.create_at = 7;
        lower_id.create_at = 7;
        assert!(WaitFollowV2Event::messages(
            "wait_0123456789abcdef0123456789abcdef",
            WaitFollowMode::Live,
            vec![higher_id.clone(), lower_id.clone()],
            false,
        )
        .is_err());
        WaitFollowV2Event::messages(
            "wait_0123456789abcdef0123456789abcdef",
            WaitFollowMode::Live,
            vec![lower_id, higher_id],
            false,
        )
        .unwrap()
        .validate()
        .unwrap();
    }

    #[test]
    fn v2_equal_create_at_and_id_fails_validate() {
        let first = message_n(1);
        let mut dup = message_n(1);
        dup.username = "other".into();
        assert!(WaitFollowV2Event::messages(
            "wait_0123456789abcdef0123456789abcdef",
            WaitFollowMode::Backlog,
            vec![first, dup],
            false,
        )
        .is_err());
    }
}
