//! `wait_follow_v1` daemon-RPC and JSONL stream types.
//!
//! The normative schemas live in Crucible under
//! `schemas/common/chanvoy-daemon-rpc/v0/`.

use serde::{Deserialize, Serialize};

use crate::Message;

pub const WAIT_FOLLOW_V1_METHOD: &str = "wait_follow_v1";
pub const WAIT_FOLLOW_V1_EVENT_METHOD: &str = "wait_follow_v1.event";
pub const WAIT_FOLLOW_V1_EVENT_SCHEMA: &str = "wait_follow_v1.event";

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

fn validate_message_record(tip: &str, message: &Message) -> Result<(), &'static str> {
    if !is_mattermost_post_id(tip) || tip != message.id {
        return Err("follow tip must equal its sole Mattermost message id");
    }
    if message.id.is_empty()
        || message.user_id.is_empty()
        || message.username.is_empty()
        || message.root_id.is_empty()
        || message.create_at < 0
    {
        return Err("follow message violates the event document");
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
}
