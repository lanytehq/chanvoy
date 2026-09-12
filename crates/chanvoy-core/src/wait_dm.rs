//! `wait_dm_v1` / `wait_dm_follow_v1` daemon-RPC types and username gates.
//!
//! Normative schemas live in Crucible under
//! `schemas/common/chanvoy-daemon-rpc/v0/`.

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::wait_channels::WAIT_CHANNELS_UTF8_MAX_BYTES;
use crate::wait_follow::{is_mattermost_post_id, WaitFollowResult, WaitFollowResultKind};
use crate::{CoreError, Message};

pub const WAIT_DM_V1_METHOD: &str = "wait_dm_v1";
pub const WAIT_DM_FOLLOW_V1_METHOD: &str = "wait_dm_follow_v1";
pub const WAIT_DM_FOLLOW_V2_METHOD: &str = "wait_dm_follow_v2";
pub const NOT_A_WAITABLE_PEER: &str = "not a waitable peer";
pub const WAIT_DM_HELP: &str = "wait for a DM from this user; do not pass a channel id.";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct WaitDmV1Params {
    pub username: String,
    pub timeout_secs: u64,
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
pub struct WaitDmFollowV2Params {
    pub username: String,
    pub timeout_secs: u64,
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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct WaitDmV1Result {
    pub peer_username: String,
    pub dm_name: String,
    pub channel: String,
    pub messages: Vec<Message>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub wait_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub replaced_wait_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct WaitDmFollowResult {
    pub wait_id: String,
    pub peer_username: String,
    pub dm_name: String,
    #[serde(flatten)]
    pub kind: WaitFollowResultKind,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DirectChannel {
    pub id: String,
    pub name: String,
}

pub fn not_a_waitable_peer() -> CoreError {
    CoreError::WaitFilterInvalid(NOT_A_WAITABLE_PEER.to_string())
}

/// Fold create/lookup 400/403/404 into the one inaccessible-peer class.
/// Retryable provider failures (429/5xx/transport) are left unchanged.
pub fn map_inaccessible_peer(err: CoreError) -> CoreError {
    match err {
        CoreError::Api { status, .. }
            if status == reqwest::StatusCode::NOT_FOUND
                || status == reqwest::StatusCode::BAD_REQUEST
                || status == reqwest::StatusCode::FORBIDDEN =>
        {
            not_a_waitable_peer()
        }
        other => other,
    }
}

pub fn canonical_dm_name(left_user_id: &str, right_user_id: &str) -> String {
    if left_user_id <= right_user_id {
        format!("{left_user_id}__{right_user_id}")
    } else {
        format!("{right_user_id}__{left_user_id}")
    }
}

/// Refuse empty, RFC UUID, Mattermost id, and `{uid}__{uid}` before any
/// provider I/O. Do not trim or case-fold.
pub fn classify_wait_dm_username(username: &str) -> Result<(), CoreError> {
    if username.is_empty()
        || username.len() > WAIT_CHANNELS_UTF8_MAX_BYTES
        || is_rfc_uuid(username)
        || is_mattermost_post_id(username)
        || is_dm_channel_name(username)
    {
        return Err(not_a_waitable_peer());
    }
    Ok(())
}

pub fn is_rfc_uuid(value: &str) -> bool {
    let trimmed = value
        .strip_prefix('{')
        .and_then(|inner| inner.strip_suffix('}'))
        .unwrap_or(value);
    Uuid::try_parse(trimmed).is_ok()
}

pub fn is_dm_channel_name(value: &str) -> bool {
    let Some((left, right)) = value.split_once("__") else {
        return false;
    };
    if right.contains("__") {
        return false;
    }
    is_mattermost_post_id(left) && is_mattermost_post_id(right)
}

impl WaitDmFollowResult {
    pub fn from_follow(peer_username: String, dm_name: String, result: WaitFollowResult) -> Self {
        Self {
            wait_id: result.wait_id,
            peer_username,
            dm_name,
            kind: result.kind,
        }
    }

    pub fn validate(&self) -> Result<(), &'static str> {
        if self.peer_username.is_empty() || self.dm_name.is_empty() {
            return Err("dm follow result must name peer and dm_name");
        }
        WaitFollowResult {
            wait_id: self.wait_id.clone(),
            kind: self.kind.clone(),
        }
        .validate()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exact_username_is_waitable() {
        classify_wait_dm_username("dave-3leaps").unwrap();
        classify_wait_dm_username("agent-bravo-devrev").unwrap();
    }

    #[test]
    fn empty_uuid_user_id_and_dm_name_are_not_peers() {
        for value in [
            "",
            "550e8400-e29b-41d4-a716-446655440000",
            "550e8400e29b41d4a716446655440000",
            "{550e8400-e29b-41d4-a716-446655440000}",
            "oc55ry3797nu3memz1g9ztauxo",
            "7bz1xwdw1fdx3dpk149b8oyeyc__oc55ry3797nu3memz1g9ztauxo",
        ] {
            let err = classify_wait_dm_username(value).unwrap_err();
            assert!(
                matches!(err, CoreError::WaitFilterInvalid(ref msg) if msg == NOT_A_WAITABLE_PEER),
                "{value:?} => {err}"
            );
        }
    }

    #[test]
    fn does_not_trim_or_casefold_into_another_identity() {
        classify_wait_dm_username(" Dave").unwrap();
        classify_wait_dm_username("DAVE-3LEAPS").unwrap();
    }

    #[test]
    fn params_deny_team_and_channel() {
        let raw = serde_json::json!({
            "username": "dave-3leaps",
            "timeout_secs": 10,
            "team": "org-lanytehq"
        });
        assert!(serde_json::from_value::<WaitDmV1Params>(raw).is_err());
        let raw = serde_json::json!({
            "username": "dave-3leaps",
            "timeout_secs": 10,
            "channel": "release-floor"
        });
        assert!(serde_json::from_value::<WaitDmV1Params>(raw).is_err());
    }

    #[test]
    fn canonical_dm_name_sorts_ids() {
        assert_eq!(
            canonical_dm_name("bbbbbbbbbbbbbbbbbbbbbbbbbb", "aaaaaaaaaaaaaaaaaaaaaaaaaa"),
            "aaaaaaaaaaaaaaaaaaaaaaaaaa__bbbbbbbbbbbbbbbbbbbbbbbbbb"
        );
    }

    #[test]
    fn provider_dm_name_is_not_trusted() {
        let computed =
            canonical_dm_name("aaaaaaaaaaaaaaaaaaaaaaaaaa", "bbbbbbbbbbbbbbbbbbbbbbbbbb");
        let provider = "zzzzzzzzzzzzzzzzzzzzzzzzzz__yyyyyyyyyyyyyyyyyyyyyyyyyy";
        assert_ne!(computed, provider);
        let _ = provider;
        assert!(computed.starts_with("aaaaaaaaaaaaaaaaaaaaaaaaaa__"));
    }

    #[test]
    fn inaccessible_http_classes_fold_to_one_diagnostic() {
        for status in [
            reqwest::StatusCode::NOT_FOUND,
            reqwest::StatusCode::BAD_REQUEST,
            reqwest::StatusCode::FORBIDDEN,
        ] {
            let err = map_inaccessible_peer(CoreError::Api {
                status,
                message: "raw provider body".into(),
            });
            assert!(
                matches!(err, CoreError::WaitFilterInvalid(ref msg) if msg == NOT_A_WAITABLE_PEER)
            );
        }
        let retryable = map_inaccessible_peer(CoreError::Api {
            status: reqwest::StatusCode::TOO_MANY_REQUESTS,
            message: "rate limited".into(),
        });
        assert!(matches!(retryable, CoreError::Api { status, .. } if status.as_u16() == 429));
    }
}
