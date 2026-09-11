//! `wait_inbox_v1` / `wait_inbox_follow_v1` daemon-RPC types and inbox cursor.
//!
//! Normative schemas live in Crucible under
//! `schemas/common/chanvoy-daemon-rpc/v0/`. Do not widen
//! `wait_follow_v1.event` (`tip == sole message.id`).

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

use crate::wait_follow::{is_mattermost_post_id, WaitFollowMode};
use crate::{CoreError, Message};

pub const WAIT_INBOX_V1_METHOD: &str = "wait_inbox_v1";
pub const WAIT_INBOX_FOLLOW_V1_METHOD: &str = "wait_inbox_follow_v1";
pub const WAIT_INBOX_FOLLOW_V1_EVENT_METHOD: &str = "wait_inbox_follow_v1.event";
pub const WAIT_INBOX_FOLLOW_V1_EVENT_SCHEMA: &str = "wait_inbox_follow_v1.event";
pub const WAIT_INBOX_HELP: &str = "wait for any DM to this bot; do not pass a channel id.";
pub const INBOX_CURSOR_PREFIX: &str = "inv1.";
pub const INBOX_CURSOR_MAX_BYTES: usize = 32 * 1024;
pub const INBOX_MAX_DMS: usize = 1024;
pub const INBOX_MAX_BACKFILL: usize = 512;
pub const INBOX_MAX_WATERMARK_IDS: usize = 128;
pub const INBOX_PAGE_SIZE: usize = 200;
pub const DM_CLASS_OWNERSHIP_KEY: &str = "dm-class";
pub const DM_CLASS_TEAM: &str = "direct";
pub const DM_CLASS_CHANNEL: &str = "inbox";
pub const POST_ID_NOT_INBOX_CURSOR: &str = "inbox cursor, not Mattermost post id";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct WaitInboxV1Params {
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
pub struct WaitInboxV1Result {
    pub peer_username: String,
    pub dm_name: String,
    pub matched_post_id: String,
    pub next_inbox_cursor: String,
    pub messages: Vec<Message>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub wait_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub replaced_wait_id: Option<String>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum WaitInboxFollowSchema {
    #[serde(rename = "wait_inbox_follow_v1.event")]
    V1,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct WaitInboxFollowEvent {
    pub schema: WaitInboxFollowSchema,
    pub wait_id: String,
    #[serde(flatten)]
    pub kind: WaitInboxFollowEventKind,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "mode", rename_all = "snake_case", deny_unknown_fields)]
pub enum WaitInboxFollowEventKind {
    Armed {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        replaced_wait_id: Option<String>,
    },
    Backlog {
        matched_post_id: String,
        peer_username: String,
        dm_name: String,
        next_inbox_cursor: String,
        truncated: bool,
        messages: [Message; 1],
    },
    Live {
        matched_post_id: String,
        peer_username: String,
        dm_name: String,
        next_inbox_cursor: String,
        truncated: bool,
        messages: [Message; 1],
    },
    Deadman {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        inbox_cursor: Option<String>,
    },
    Canceled {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        inbox_cursor: Option<String>,
    },
    Replaced {
        replaced_by_wait_id: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        inbox_cursor: Option<String>,
    },
    Failed {
        reason_code: WaitInboxFailureReason,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        inbox_cursor: Option<String>,
    },
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum WaitInboxFailureReason {
    ProviderFailed,
    ProviderOverflow,
    ProviderOutage,
    CursorUncertain,
    Capacity,
    ProviderDegraded,
    OwnershipLost,
    DaemonShutdown,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct WaitInboxFollowResult {
    pub wait_id: String,
    #[serde(flatten)]
    pub kind: WaitInboxFollowResultKind,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "mode", rename_all = "snake_case", deny_unknown_fields)]
pub enum WaitInboxFollowResultKind {
    Deadman {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        inbox_cursor: Option<String>,
    },
    Replaced {
        replaced_by_wait_id: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        inbox_cursor: Option<String>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InboxCursorV1 {
    pub profile: String,
    pub bot_user_id: String,
    pub watermark: i64,
    pub observed_ids: BTreeSet<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DirectCatalogEntry {
    pub id: String,
    pub name: String,
    pub last_post_at: i64,
}

#[derive(Serialize, Deserialize)]
struct CursorWire {
    v: u8,
    p: String,
    b: String,
    w: i64,
    i: Vec<String>,
}

pub fn cursor_uncertain(message: impl Into<String>) -> CoreError {
    CoreError::WaitFilterInvalid(message.into())
}

pub fn inbox_capacity(message: impl Into<String>) -> CoreError {
    CoreError::WaitFilterInvalid(message.into())
}

pub fn peer_user_id_from_dm_name<'a>(name: &'a str, my_id: &str) -> Option<&'a str> {
    let (left, right) = name.split_once("__")?;
    if right.contains("__") {
        return None;
    }
    if left == my_id {
        Some(right)
    } else if right == my_id {
        Some(left)
    } else {
        None
    }
}

pub fn refuse_inbox_after(after: &str) -> Result<(), CoreError> {
    if after.is_empty() {
        return Err(cursor_uncertain("empty --after is refused"));
    }
    if is_mattermost_post_id(after) {
        return Err(CoreError::WaitFilterInvalid(
            POST_ID_NOT_INBOX_CURSOR.to_string(),
        ));
    }
    Ok(())
}

impl InboxCursorV1 {
    pub fn empty(profile: &str, bot_user_id: &str) -> Self {
        Self {
            profile: profile.to_string(),
            bot_user_id: bot_user_id.to_string(),
            watermark: 0,
            observed_ids: BTreeSet::new(),
        }
    }

    pub fn encode(&self) -> Result<String, CoreError> {
        let mut ids: Vec<String> = self.observed_ids.iter().cloned().collect();
        ids.sort();
        let wire = CursorWire {
            v: 1,
            p: self.profile.clone(),
            b: self.bot_user_id.clone(),
            w: self.watermark,
            i: ids,
        };
        let json = serde_json::to_string(&wire)
            .map_err(|err| cursor_uncertain(format!("inbox cursor encode failed: {err}")))?;
        let encoded = format!("{INBOX_CURSOR_PREFIX}{}", b64url_encode(json.as_bytes()));
        if encoded.len() > INBOX_CURSOR_MAX_BYTES {
            return Err(inbox_capacity(
                "inbox cursor exceeds the 32 KiB encoded cap",
            ));
        }
        Ok(encoded)
    }

    pub fn decode(raw: &str, profile: &str, bot_user_id: &str) -> Result<Self, CoreError> {
        refuse_inbox_after(raw)?;
        if raw.len() > INBOX_CURSOR_MAX_BYTES {
            return Err(inbox_capacity(
                "inbox cursor exceeds the 32 KiB encoded cap",
            ));
        }
        let payload = raw
            .strip_prefix(INBOX_CURSOR_PREFIX)
            .ok_or_else(|| cursor_uncertain("malformed inbox cursor (expected inv1. prefix)"))?;
        let bytes = b64url_decode(payload)
            .map_err(|_| cursor_uncertain("malformed inbox cursor (base64)"))?;
        let wire: CursorWire = serde_json::from_slice(&bytes)
            .map_err(|_| cursor_uncertain("malformed inbox cursor (json)"))?;
        if wire.v != 1 {
            return Err(cursor_uncertain("unprovable inbox cursor version"));
        }
        if wire.p != profile || wire.b != bot_user_id {
            return Err(cursor_uncertain(
                "inbox cursor does not bind this profile/bot",
            ));
        }
        if wire.w < 0 {
            return Err(cursor_uncertain("inbox cursor watermark is negative"));
        }
        if wire.i.len() > INBOX_MAX_WATERMARK_IDS {
            return Err(cursor_uncertain(
                "inbox cursor equal-watermark id set exceeds 128",
            ));
        }
        let mut seen = BTreeSet::new();
        for id in &wire.i {
            if !is_mattermost_post_id(id) {
                return Err(cursor_uncertain(
                    "inbox cursor observed id is not a post id",
                ));
            }
            if !seen.insert(id.clone()) {
                return Err(cursor_uncertain(
                    "inbox cursor observed-id set is not canonical",
                ));
            }
        }
        let decoded = Self {
            profile: wire.p,
            bot_user_id: wire.b,
            watermark: wire.w,
            observed_ids: seen,
        };
        let canonical = decoded.encode()?;
        if canonical != raw {
            return Err(cursor_uncertain("inbox cursor encoding is not canonical"));
        }
        Ok(decoded)
    }

    pub fn admits(&self, create_at: i64, post_id: &str) -> bool {
        if create_at > self.watermark {
            return true;
        }
        if create_at == self.watermark {
            return !self.observed_ids.contains(post_id);
        }
        false
    }

    pub fn advance(&self, create_at: i64, post_id: &str) -> Result<Self, CoreError> {
        if !is_mattermost_post_id(post_id) {
            return Err(cursor_uncertain(
                "matched post id is not a Mattermost post id",
            ));
        }
        let mut next = self.clone();
        if create_at > next.watermark {
            next.watermark = create_at;
            next.observed_ids.clear();
            next.observed_ids.insert(post_id.to_string());
        } else if create_at == next.watermark {
            next.observed_ids.insert(post_id.to_string());
            if next.observed_ids.len() > INBOX_MAX_WATERMARK_IDS {
                return Err(cursor_uncertain(
                    "inbox equal-watermark observed-id set exceeds 128",
                ));
            }
        } else {
            return Err(cursor_uncertain(
                "cannot advance inbox cursor with a lower watermark",
            ));
        }
        Ok(next)
    }
}

impl WaitInboxFollowEvent {
    pub fn armed(wait_id: impl Into<String>, replaced_wait_id: Option<String>) -> Self {
        Self {
            schema: WaitInboxFollowSchema::V1,
            wait_id: wait_id.into(),
            kind: WaitInboxFollowEventKind::Armed { replaced_wait_id },
        }
    }

    pub fn message(
        wait_id: impl Into<String>,
        mode: WaitFollowMode,
        peer_username: String,
        dm_name: String,
        next_inbox_cursor: String,
        message: Message,
        truncated: bool,
    ) -> Result<Self, &'static str> {
        if !is_mattermost_post_id(&message.id) {
            return Err("inbox message id is not a Mattermost post id");
        }
        if next_inbox_cursor == message.id || is_mattermost_post_id(&next_inbox_cursor) {
            return Err("next_inbox_cursor must not be a Mattermost post id");
        }
        if !next_inbox_cursor.starts_with(INBOX_CURSOR_PREFIX) {
            return Err("next_inbox_cursor must be an inv1. inbox cursor");
        }
        let matched_post_id = message.id.clone();
        let kind = match mode {
            WaitFollowMode::Backlog => WaitInboxFollowEventKind::Backlog {
                matched_post_id,
                peer_username,
                dm_name,
                next_inbox_cursor,
                truncated,
                messages: [message],
            },
            WaitFollowMode::Live if !truncated => WaitInboxFollowEventKind::Live {
                matched_post_id,
                peer_username,
                dm_name,
                next_inbox_cursor,
                truncated: false,
                messages: [message],
            },
            WaitFollowMode::Live => return Err("live inbox records cannot be truncated"),
            _ => return Err("message record requires backlog or live mode"),
        };
        Ok(Self {
            schema: WaitInboxFollowSchema::V1,
            wait_id: wait_id.into(),
            kind,
        })
    }

    pub fn mode(&self) -> WaitFollowMode {
        match self.kind {
            WaitInboxFollowEventKind::Armed { .. } => WaitFollowMode::Armed,
            WaitInboxFollowEventKind::Backlog { .. } => WaitFollowMode::Backlog,
            WaitInboxFollowEventKind::Live { .. } => WaitFollowMode::Live,
            WaitInboxFollowEventKind::Deadman { .. } => WaitFollowMode::Deadman,
            WaitInboxFollowEventKind::Canceled { .. } => WaitFollowMode::Canceled,
            WaitInboxFollowEventKind::Replaced { .. } => WaitFollowMode::Replaced,
            WaitInboxFollowEventKind::Failed { .. } => WaitFollowMode::Failed,
        }
    }

    pub fn inbox_cursor(&self) -> Option<&str> {
        match &self.kind {
            WaitInboxFollowEventKind::Backlog {
                next_inbox_cursor, ..
            }
            | WaitInboxFollowEventKind::Live {
                next_inbox_cursor, ..
            } => Some(next_inbox_cursor.as_str()),
            WaitInboxFollowEventKind::Deadman { inbox_cursor }
            | WaitInboxFollowEventKind::Canceled { inbox_cursor }
            | WaitInboxFollowEventKind::Failed { inbox_cursor, .. }
            | WaitInboxFollowEventKind::Replaced { inbox_cursor, .. } => inbox_cursor.as_deref(),
            WaitInboxFollowEventKind::Armed { .. } => None,
        }
    }

    pub fn validate(&self) -> Result<(), &'static str> {
        if self.wait_id.is_empty() || self.wait_id.len() > 64 {
            return Err("inbox wait id must contain 1 to 64 bytes");
        }
        match &self.kind {
            WaitInboxFollowEventKind::Backlog {
                matched_post_id,
                next_inbox_cursor,
                messages,
                ..
            }
            | WaitInboxFollowEventKind::Live {
                matched_post_id,
                next_inbox_cursor,
                messages,
                truncated: false,
                ..
            } => {
                if matched_post_id != &messages[0].id {
                    return Err("matched_post_id must equal the sole message id");
                }
                if is_mattermost_post_id(next_inbox_cursor) || next_inbox_cursor == matched_post_id
                {
                    return Err("next_inbox_cursor must not be a Mattermost post id");
                }
                Ok(())
            }
            WaitInboxFollowEventKind::Live {
                truncated: true, ..
            } => Err("live inbox records cannot be truncated"),
            WaitInboxFollowEventKind::Armed { replaced_wait_id } => {
                if let Some(wait_id) = replaced_wait_id {
                    if wait_id.is_empty() || wait_id.len() > 64 {
                        return Err("replaced wait id must contain 1 to 64 bytes");
                    }
                }
                Ok(())
            }
            WaitInboxFollowEventKind::Replaced {
                replaced_by_wait_id,
                inbox_cursor,
            } => {
                if replaced_by_wait_id.is_empty() || replaced_by_wait_id.len() > 64 {
                    return Err("replaced_by wait id must contain 1 to 64 bytes");
                }
                validate_optional_inbox_cursor(inbox_cursor)
            }
            WaitInboxFollowEventKind::Deadman { inbox_cursor }
            | WaitInboxFollowEventKind::Canceled { inbox_cursor }
            | WaitInboxFollowEventKind::Failed { inbox_cursor, .. } => {
                validate_optional_inbox_cursor(inbox_cursor)
            }
        }
    }
}

fn validate_optional_inbox_cursor(cursor: &Option<String>) -> Result<(), &'static str> {
    match cursor {
        None => Ok(()),
        Some(value) if is_mattermost_post_id(value) => {
            Err("inbox_cursor must not be a Mattermost post id")
        }
        Some(value) if !value.starts_with(INBOX_CURSOR_PREFIX) => {
            Err("inbox_cursor must be an inv1. inbox cursor")
        }
        Some(_) => Ok(()),
    }
}

fn b64url_encode(input: &[u8]) -> String {
    const TABLE: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
    let mut out = String::with_capacity(input.len().div_ceil(3) * 4);
    for chunk in input.chunks(3) {
        let a = chunk[0] as u32;
        let b = chunk.get(1).copied().unwrap_or(0) as u32;
        let c = chunk.get(2).copied().unwrap_or(0) as u32;
        let n = (a << 16) | (b << 8) | c;
        out.push(TABLE[((n >> 18) & 63) as usize] as char);
        out.push(TABLE[((n >> 12) & 63) as usize] as char);
        if chunk.len() > 1 {
            out.push(TABLE[((n >> 6) & 63) as usize] as char);
        }
        if chunk.len() > 2 {
            out.push(TABLE[(n & 63) as usize] as char);
        }
    }
    out
}

fn b64url_decode(input: &str) -> Result<Vec<u8>, ()> {
    fn val(c: u8) -> Result<u32, ()> {
        match c {
            b'A'..=b'Z' => Ok((c - b'A') as u32),
            b'a'..=b'z' => Ok((c - b'a' + 26) as u32),
            b'0'..=b'9' => Ok((c - b'0' + 52) as u32),
            b'-' => Ok(62),
            b'_' => Ok(63),
            _ => Err(()),
        }
    }
    let bytes = input.as_bytes();
    if bytes.len() % 4 == 1 {
        return Err(());
    }
    let mut out = Vec::with_capacity(bytes.len().div_ceil(4) * 3);
    let mut idx = 0;
    while idx < bytes.len() {
        let remain = bytes.len() - idx;
        let n = match remain {
            1 => return Err(()),
            2 => (val(bytes[idx])? << 18) | (val(bytes[idx + 1])? << 12),
            3 => {
                (val(bytes[idx])? << 18)
                    | (val(bytes[idx + 1])? << 12)
                    | (val(bytes[idx + 2])? << 6)
            }
            _ => {
                (val(bytes[idx])? << 18)
                    | (val(bytes[idx + 1])? << 12)
                    | (val(bytes[idx + 2])? << 6)
                    | val(bytes[idx + 3])?
            }
        };
        out.push(((n >> 16) & 0xff) as u8);
        if remain >= 3 {
            out.push(((n >> 8) & 0xff) as u8);
        }
        if remain >= 4 {
            out.push((n & 0xff) as u8);
        }
        idx += remain.min(4);
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    const PROFILE: &str = "bravo-devlead-lanytehq";
    const BOT: &str = "oc55ry3797nu3memz1g9ztauxo";
    const POST_HI: &str = "zzzzzz00000000000000000001";
    const POST_LO: &str = "aaaaaa00000000000000000001";

    #[test]
    fn encode_decode_roundtrip_and_prefix() {
        let cursor = InboxCursorV1::empty(PROFILE, BOT)
            .advance(10, POST_HI)
            .unwrap();
        let raw = cursor.encode().unwrap();
        assert!(raw.starts_with(INBOX_CURSOR_PREFIX));
        assert!(!is_mattermost_post_id(&raw));
        let decoded = InboxCursorV1::decode(&raw, PROFILE, BOT).unwrap();
        assert_eq!(decoded, cursor);
    }

    #[test]
    fn post_id_after_is_named_refusal() {
        let err = refuse_inbox_after("postid00000000000000000001").unwrap_err();
        assert!(
            matches!(err, CoreError::WaitFilterInvalid(ref msg) if msg == POST_ID_NOT_INBOX_CURSOR)
        );
    }

    #[test]
    fn wrong_profile_is_uncertain() {
        let raw = InboxCursorV1::empty(PROFILE, BOT).encode().unwrap();
        let err = InboxCursorV1::decode(&raw, "other-profile", BOT).unwrap_err();
        assert!(
            matches!(err, CoreError::WaitFilterInvalid(ref msg) if msg.contains("profile/bot"))
        );
    }

    #[test]
    fn same_ms_lower_id_is_admitted() {
        let cursor = InboxCursorV1::empty(PROFILE, BOT)
            .advance(100, POST_HI)
            .unwrap();
        assert!(cursor.admits(100, POST_LO));
        assert!(!cursor.admits(100, POST_HI));
        assert!(cursor.admits(101, POST_LO));
        assert!(!cursor.admits(99, POST_LO));
    }

    #[test]
    fn equal_watermark_overflow_fails_closed() {
        let mut cursor = InboxCursorV1::empty(PROFILE, BOT)
            .advance(5, "aaaaaa00000000000000000000")
            .unwrap();
        for i in 1..INBOX_MAX_WATERMARK_IDS {
            let id = format!("aaaaaa00000000000000000{i:03}");
            cursor = cursor.advance(5, &id).unwrap();
        }
        let err = cursor.advance(5, "aaaaaa00000000000000000128").unwrap_err();
        assert!(matches!(err, CoreError::WaitFilterInvalid(ref msg) if msg.contains("128")));
    }

    #[test]
    fn params_deny_unknown_selectors() {
        let raw = serde_json::json!({"timeout_secs": 10, "username": "dave-3leaps"});
        assert!(serde_json::from_value::<WaitInboxV1Params>(raw).is_err());
        let raw = serde_json::json!({"timeout_secs": 10, "channel": "town-square"});
        assert!(serde_json::from_value::<WaitInboxV1Params>(raw).is_err());
        let raw = serde_json::json!({"timeout_secs": 10, "team": "org-lanytehq"});
        assert!(serde_json::from_value::<WaitInboxV1Params>(raw).is_err());
    }

    #[test]
    fn encode_is_canonical_across_id_insertion_order() {
        let mut a = InboxCursorV1::empty(PROFILE, BOT);
        a.watermark = 7;
        a.observed_ids.insert(POST_HI.to_string());
        a.observed_ids.insert(POST_LO.to_string());
        let mut b = InboxCursorV1::empty(PROFILE, BOT);
        b.watermark = 7;
        b.observed_ids.insert(POST_LO.to_string());
        b.observed_ids.insert(POST_HI.to_string());
        assert_eq!(a.encode().unwrap(), b.encode().unwrap());
    }

    #[test]
    fn negative_watermark_is_uncertain() {
        let json = serde_json::json!({"v":1,"p":PROFILE,"b":BOT,"w":-1,"i":[]});
        let raw = format!(
            "{INBOX_CURSOR_PREFIX}{}",
            super::b64url_encode(json.to_string().as_bytes())
        );
        let err = InboxCursorV1::decode(&raw, PROFILE, BOT).unwrap_err();
        assert!(matches!(err, CoreError::WaitFilterInvalid(ref msg) if msg.contains("negative")));
    }

    #[test]
    fn duplicate_ids_are_uncertain() {
        let json = serde_json::json!({"v":1,"p":PROFILE,"b":BOT,"w":1,"i":[POST_HI, POST_HI]});
        let raw = format!(
            "{INBOX_CURSOR_PREFIX}{}",
            super::b64url_encode(json.to_string().as_bytes())
        );
        let err = InboxCursorV1::decode(&raw, PROFILE, BOT).unwrap_err();
        assert!(matches!(err, CoreError::WaitFilterInvalid(ref msg) if msg.contains("canonical")));
    }

    #[test]
    fn noncanonical_key_order_is_uncertain() {
        let json = format!(r#"{{"w":1,"v":1,"p":"{PROFILE}","b":"{BOT}","i":[]}}"#);
        let raw = format!(
            "{INBOX_CURSOR_PREFIX}{}",
            super::b64url_encode(json.as_bytes())
        );
        let err = InboxCursorV1::decode(&raw, PROFILE, BOT).unwrap_err();
        assert!(matches!(err, CoreError::WaitFilterInvalid(ref msg) if msg.contains("canonical")));
    }
}
