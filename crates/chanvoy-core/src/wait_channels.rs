//! `wait_channels_v1` daemon-RPC types and pre-provider validation.
//!
//! Wire schemas live in crucible `schemas/common/chanvoy-daemon-rpc/v0/`.
//! This module is the Rust producer/consumer of that contract. It does
//! not change `wait_channel` / `wait_channel_v2`.

use serde::{Deserialize, Serialize};

use crate::CoreError;

/// JSON-RPC method name — capability gate for an older daemon.
pub const WAIT_CHANNELS_V1_METHOD: &str = "wait_channels_v1";

/// Runtime UTF-8 byte cap for team, channel, after, and filter sources.
/// Schema `maxLength` is a code-point bound; this byte check is mandatory
/// before any provider I/O.
pub const WAIT_CHANNELS_UTF8_MAX_BYTES: usize = 256;

/// Inclusive arm-count bounds.
pub const WAIT_CHANNELS_MIN_ARMS: usize = 2;
pub const WAIT_CHANNELS_MAX_ARMS: usize = 8;

/// Canonical team/channel selector on the wire.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct WaitChannelSelector {
    pub team: String,
    pub channel: String,
}

impl WaitChannelSelector {
    pub fn new(team: impl Into<String>, channel: impl Into<String>) -> Self {
        Self {
            team: team.into(),
            channel: channel.into(),
        }
    }

    pub fn qualified(&self) -> String {
        format!("{}/{}", self.team, self.channel)
    }

    /// Case-folded key used to detect duplicate requested arms before
    /// provider resolution. Canonical channel-id uniqueness is a later
    /// daemon invariant after resolve.
    pub fn requested_key(&self) -> String {
        format!(
            "{}/{}",
            self.team.to_ascii_lowercase(),
            self.channel.to_ascii_lowercase()
        )
    }
}

/// One fan-in arm.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct WaitChannelArm {
    pub team: String,
    pub channel: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub after: Option<String>,
}

impl WaitChannelArm {
    pub fn selector(&self) -> WaitChannelSelector {
        WaitChannelSelector::new(&self.team, &self.channel)
    }
}

/// Parameters for `wait_channels_v1`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct WaitChannelsParams {
    pub arms: Vec<WaitChannelArm>,
    pub timeout_secs: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub contains: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pattern: Option<String>,
}

/// Successful first-match result. Clean deadman is a JSON-RPC error, not
/// this shape.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct WaitChannelsResult {
    pub mode: String,
    pub channels: Vec<WaitChannelSelector>,
    pub matched_channel: WaitChannelSelector,
    pub messages: Vec<crate::Message>,
}

impl WaitChannelsResult {
    pub const MODE_FAN_IN: &'static str = "fan_in";

    pub fn match_one(
        channels: Vec<WaitChannelSelector>,
        matched_channel: WaitChannelSelector,
        message: crate::Message,
    ) -> Self {
        Self {
            mode: Self::MODE_FAN_IN.to_string(),
            channels,
            matched_channel,
            messages: vec![message],
        }
    }
}

/// Refuse invalid fan-in input before any subscribe or provider work.
pub fn validate_wait_channels_params(params: &WaitChannelsParams) -> Result<(), CoreError> {
    if params.timeout_secs == 0 {
        return Err(input("wait timeout must be greater than zero"));
    }
    let n = params.arms.len();
    if !(WAIT_CHANNELS_MIN_ARMS..=WAIT_CHANNELS_MAX_ARMS).contains(&n) {
        return Err(input(format!(
            "wait_channels_v1 requires {WAIT_CHANNELS_MIN_ARMS}–{WAIT_CHANNELS_MAX_ARMS} arms, got {n}"
        )));
    }

    let mut seen = Vec::with_capacity(n);
    for arm in &params.arms {
        check_segment("team", &arm.team)?;
        check_segment("channel", &arm.channel)?;
        if let Some(after) = arm.after.as_deref() {
            check_segment("after", after)?;
        }
        let key = arm.selector().requested_key();
        if seen.iter().any(|existing: &String| existing == &key) {
            return Err(input(format!(
                "duplicate wait arm {}",
                arm.selector().qualified()
            )));
        }
        seen.push(key);
    }

    check_optional_filter("contains", params.contains.as_deref())?;
    check_optional_filter("pattern", params.pattern.as_deref())?;
    Ok(())
}

/// Parse a fully-qualified `team/channel` selector. Bare names fail closed.
pub fn parse_qualified_wait_selector(raw: &str) -> Result<WaitChannelSelector, CoreError> {
    let trimmed = raw.trim().trim_start_matches('#');
    let Some((team, channel)) = trimmed.split_once('/') else {
        return Err(input(
            "fan-in --channel requires an explicit team/channel selector",
        ));
    };
    let team = team.trim();
    let channel = channel.trim().trim_start_matches('#');
    if team.is_empty() || channel.is_empty() || team.contains('/') {
        return Err(input(
            "fan-in --channel requires an explicit team/channel selector",
        ));
    }
    check_segment("team", team)?;
    check_segment("channel", channel)?;
    Ok(WaitChannelSelector::new(team, channel))
}

/// Parse `--after-channel team/channel=post-id`.
pub fn parse_after_channel_flag(raw: &str) -> Result<(WaitChannelSelector, String), CoreError> {
    let Some((selector, after)) = raw.split_once('=') else {
        return Err(input(
            "--after-channel requires the form team/channel=post-id",
        ));
    };
    if after.is_empty() {
        return Err(input("empty --after-channel post id is refused"));
    }
    check_segment("after", after)?;
    Ok((parse_qualified_wait_selector(selector)?, after.to_string()))
}

/// Earliest eligible backfill winner: `(create_at, post_id, channel_id)`.
pub fn first_backfill_winner<'a, T>(
    candidates: impl IntoIterator<Item = &'a (T, crate::Message, String)>,
) -> Option<&'a (T, crate::Message, String)> {
    candidates.into_iter().min_by(|a, b| {
        a.1.create_at
            .cmp(&b.1.create_at)
            .then_with(|| a.1.id.cmp(&b.1.id))
            .then_with(|| a.2.cmp(&b.2))
    })
}

fn check_optional_filter(name: &str, value: Option<&str>) -> Result<(), CoreError> {
    match value {
        None => Ok(()),
        Some("") => Err(input(format!("empty --{name} is refused (not match-all)"))),
        Some(s) => check_segment(name, s),
    }
}

fn check_segment(name: &str, value: &str) -> Result<(), CoreError> {
    if value.is_empty() {
        return Err(input(format!("wait {name} must be non-empty")));
    }
    if value.len() > WAIT_CHANNELS_UTF8_MAX_BYTES {
        return Err(input(format!(
            "wait {name} exceeds {WAIT_CHANNELS_UTF8_MAX_BYTES} UTF-8 bytes"
        )));
    }
    Ok(())
}

fn input(message: impl Into<String>) -> CoreError {
    CoreError::WaitFilterInvalid(message.into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Message;

    fn arm(team: &str, channel: &str, after: Option<&str>) -> WaitChannelArm {
        WaitChannelArm {
            team: team.into(),
            channel: channel.into(),
            after: after.map(str::to_string),
        }
    }

    fn params(arms: Vec<WaitChannelArm>) -> WaitChannelsParams {
        WaitChannelsParams {
            arms,
            timeout_secs: 30,
            contains: None,
            pattern: None,
        }
    }

    fn msg(id: &str, create_at: i64) -> Message {
        Message {
            id: id.into(),
            user_id: "u".into(),
            username: "u".into(),
            message: "ASSENT".into(),
            create_at,
            root_id: id.into(),
        }
    }

    #[test]
    fn two_and_eight_arms_ok_one_and_nine_refused() {
        let two = params(vec![arm("t", "a", None), arm("t", "b", None)]);
        assert!(validate_wait_channels_params(&two).is_ok());
        let eight = params((0..8).map(|i| arm("t", &format!("c{i}"), None)).collect());
        assert!(validate_wait_channels_params(&eight).is_ok());
        let one = params(vec![arm("t", "a", None)]);
        assert!(matches!(
            validate_wait_channels_params(&one),
            Err(CoreError::WaitFilterInvalid(_))
        ));
        let nine = params((0..9).map(|i| arm("t", &format!("c{i}"), None)).collect());
        assert!(matches!(
            validate_wait_channels_params(&nine),
            Err(CoreError::WaitFilterInvalid(_))
        ));
    }

    #[test]
    fn zero_timeout_and_empty_filter_are_input() {
        let mut p = params(vec![arm("t", "a", None), arm("t", "b", None)]);
        p.timeout_secs = 0;
        assert!(matches!(
            validate_wait_channels_params(&p),
            Err(CoreError::WaitFilterInvalid(m)) if m.contains("greater than zero")
        ));
        p.timeout_secs = 1;
        p.contains = Some(String::new());
        assert!(matches!(
            validate_wait_channels_params(&p),
            Err(CoreError::WaitFilterInvalid(m)) if m.contains("contains")
        ));
    }

    #[test]
    fn duplicate_requested_arms_refused_even_with_case_fold() {
        let p = params(vec![arm("Org", "Brief", None), arm("org", "brief", None)]);
        assert!(matches!(
            validate_wait_channels_params(&p),
            Err(CoreError::WaitFilterInvalid(m)) if m.contains("duplicate")
        ));
    }

    #[test]
    fn oversized_selector_and_after_refused() {
        let big = "a".repeat(WAIT_CHANNELS_UTF8_MAX_BYTES + 1);
        let p = params(vec![arm("t", &big, None), arm("t", "b", None)]);
        assert!(matches!(
            validate_wait_channels_params(&p),
            Err(CoreError::WaitFilterInvalid(m)) if m.contains("channel")
        ));
        let p = params(vec![arm("t", "a", Some(&big)), arm("t", "b", None)]);
        assert!(matches!(
            validate_wait_channels_params(&p),
            Err(CoreError::WaitFilterInvalid(m)) if m.contains("after")
        ));
    }

    #[test]
    fn bare_selector_refused_qualified_ok() {
        assert!(parse_qualified_wait_selector("brief").is_err());
        let sel = parse_qualified_wait_selector("org-lanytehq/brief-per-039").unwrap();
        assert_eq!(sel.team, "org-lanytehq");
        assert_eq!(sel.channel, "brief-per-039");
        let (sel, after) = parse_after_channel_flag("org-lanytehq/brief-per-039=postabc").unwrap();
        assert_eq!(sel.channel, "brief-per-039");
        assert_eq!(after, "postabc");
        assert!(parse_after_channel_flag("org-lanytehq/brief-per-039").is_err());
    }

    #[test]
    fn backfill_winner_orders_create_at_then_id_then_channel() {
        let a = (
            WaitChannelSelector::new("t", "a"),
            msg("p2", 10),
            "ch-a".into(),
        );
        let b = (
            WaitChannelSelector::new("t", "b"),
            msg("p1", 10),
            "ch-b".into(),
        );
        let c = (
            WaitChannelSelector::new("t", "c"),
            msg("p1", 10),
            "ch-c".into(),
        );
        let win = first_backfill_winner([&a, &b, &c]).unwrap();
        assert_eq!(win.1.id, "p1");
        assert_eq!(win.2, "ch-b");
    }

    #[test]
    fn unknown_top_level_and_arm_fields_are_rejected() {
        let extra_top = serde_json::json!({
            "arms": [
                {"team": "t", "channel": "a"},
                {"team": "t", "channel": "b"}
            ],
            "timeout_secs": 30,
            "unexpected": true
        });
        assert!(serde_json::from_value::<WaitChannelsParams>(extra_top).is_err());
        let extra_arm = serde_json::json!({
            "arms": [
                {"team": "t", "channel": "a", "extra": "no"},
                {"team": "t", "channel": "b"}
            ],
            "timeout_secs": 30
        });
        assert!(serde_json::from_value::<WaitChannelsParams>(extra_arm).is_err());
    }

    #[test]
    fn params_round_trip_matches_contract_shape() {
        let p = WaitChannelsParams {
            arms: vec![
                arm("org-example", "release-floor", None),
                arm(
                    "org-example",
                    "feature-brief",
                    Some("postidabcdefghijklmnopqrstuvwxyz01"),
                ),
            ],
            timeout_secs: 1200,
            contains: None,
            pattern: None,
        };
        let v = serde_json::to_value(&p).unwrap();
        assert_eq!(v["timeout_secs"], 1200);
        assert_eq!(v["arms"].as_array().unwrap().len(), 2);
        assert!(v.get("contains").is_none());
        let back: WaitChannelsParams = serde_json::from_value(v).unwrap();
        assert_eq!(back, p);
    }
}
