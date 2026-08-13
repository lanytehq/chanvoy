//! Pure single-waiter registry decisions for `wait_channel_v3`.
//!
//! Keyed by canonical channel id inside one profile daemon. This module
//! has no I/O and does not invent a host-global lock. Fan-in multi-key
//! acquisition is intentionally out of scope.

use uuid::Uuid;

pub const WAIT_CHANNEL_V3_METHOD: &str = "wait_channel_v3";
pub const REPLACE_CLEANUP_BUDGET_SECS: u64 = 5;
pub const WAIT_ID_PREFIX: &str = "wait_";
pub const WAIT_ID_HEX_LEN: usize = 32;

pub const RPC_WAIT_ALREADY_ACTIVE: i64 = -32009;
pub const RPC_WAIT_CONFLICT_CHANGED: i64 = -32010;
pub const RPC_WAIT_REPLACED: i64 = -32011;
pub const RPC_WAIT_REPLACE_UNCONFIRMED: i64 = -32012;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WaitSlotView {
    pub wait_id: String,
    pub generation: u64,
    pub started_at_ms: i64,
    pub replacing: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WaitAcquireIntent {
    Default,
    Replace { wait_id: String },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WaitAcquireDecision {
    Admit {
        wait_id: String,
        generation: u64,
        started_at_ms: i64,
    },
    RefuseActive {
        existing_wait_id: String,
        started_at_ms: i64,
    },
    ConflictChanged,
    BeginReplace {
        old_wait_id: String,
        old_generation: u64,
        new_wait_id: String,
        new_generation: u64,
        started_at_ms: i64,
    },
}

pub fn new_wait_id() -> String {
    format!("{WAIT_ID_PREFIX}{:032x}", Uuid::new_v4().as_u128())
}

pub fn wait_id_well_formed(raw: &str) -> bool {
    let Some(hex) = raw.strip_prefix(WAIT_ID_PREFIX) else {
        return false;
    };
    hex.len() == WAIT_ID_HEX_LEN && hex.bytes().all(|b| b.is_ascii_hexdigit())
}

pub fn decide_acquire(
    current: Option<&WaitSlotView>,
    intent: &WaitAcquireIntent,
    next_generation: u64,
    now_ms: i64,
    new_wait_id: String,
) -> WaitAcquireDecision {
    if let WaitAcquireIntent::Replace { wait_id } = intent {
        if !wait_id_well_formed(wait_id) {
            return WaitAcquireDecision::ConflictChanged;
        }
    }

    match (current, intent) {
        (None, WaitAcquireIntent::Default) => WaitAcquireDecision::Admit {
            wait_id: new_wait_id,
            generation: next_generation,
            started_at_ms: now_ms,
        },
        (None, WaitAcquireIntent::Replace { .. }) => WaitAcquireDecision::ConflictChanged,
        (Some(slot), WaitAcquireIntent::Default) => WaitAcquireDecision::RefuseActive {
            existing_wait_id: slot.wait_id.clone(),
            started_at_ms: slot.started_at_ms,
        },
        (Some(slot), WaitAcquireIntent::Replace { wait_id }) => {
            if slot.replacing || slot.wait_id != *wait_id {
                WaitAcquireDecision::ConflictChanged
            } else {
                WaitAcquireDecision::BeginReplace {
                    old_wait_id: slot.wait_id.clone(),
                    old_generation: slot.generation,
                    new_wait_id,
                    new_generation: next_generation,
                    started_at_ms: now_ms,
                }
            }
        }
    }
}

/// Generation-checked removal: an old guard must not delete a newer slot.
pub fn should_release(current: Option<&WaitSlotView>, generation: u64) -> bool {
    current.is_some_and(|slot| slot.generation == generation)
}

/// After an unconfirmed replace, a later install is allowed only if the
/// reserved generation is still the next unused value and the old
/// generation is still the visible owner.
pub fn can_install_replacement(
    current: Option<&WaitSlotView>,
    old_generation: u64,
    reserved_generation: u64,
) -> bool {
    match current {
        None => true,
        Some(slot) => {
            slot.generation == old_generation
                && slot.replacing
                && reserved_generation > old_generation
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn slot(id: &str, generation: u64, replacing: bool) -> WaitSlotView {
        WaitSlotView {
            wait_id: id.to_string(),
            generation,
            started_at_ms: 1000,
            replacing,
        }
    }

    fn fresh_id() -> String {
        "wait_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".into()
    }

    #[test]
    fn new_wait_id_is_opaque_128_bit_hex() {
        let id = new_wait_id();
        assert!(wait_id_well_formed(&id), "{id}");
        let other = new_wait_id();
        assert_ne!(id, other);
    }

    #[test]
    fn malformed_wait_id_is_rejected() {
        assert!(!wait_id_well_formed(""));
        assert!(!wait_id_well_formed("wait_short"));
        assert!(!wait_id_well_formed(
            "wait_GGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGG"
        ));
        assert!(!wait_id_well_formed(
            "WAIT_0123456789abcdef0123456789abcdef"
        ));
    }

    #[test]
    fn default_admits_empty_key() {
        let d = decide_acquire(None, &WaitAcquireIntent::Default, 1, 50, fresh_id());
        assert!(matches!(
            d,
            WaitAcquireDecision::Admit {
                generation: 1,
                started_at_ms: 50,
                ..
            }
        ));
    }

    #[test]
    fn default_refuses_occupied_key() {
        let current = slot("wait_bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", 3, false);
        let d = decide_acquire(
            Some(&current),
            &WaitAcquireIntent::Default,
            4,
            99,
            fresh_id(),
        );
        assert_eq!(
            d,
            WaitAcquireDecision::RefuseActive {
                existing_wait_id: current.wait_id,
                started_at_ms: 1000,
            }
        );
    }

    #[test]
    fn replace_exact_id_begins_cas() {
        let current = slot("wait_bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", 3, false);
        let d = decide_acquire(
            Some(&current),
            &WaitAcquireIntent::Replace {
                wait_id: current.wait_id.clone(),
            },
            4,
            200,
            fresh_id(),
        );
        match d {
            WaitAcquireDecision::BeginReplace {
                old_generation,
                new_generation,
                old_wait_id,
                started_at_ms,
                ..
            } => {
                assert_eq!(old_generation, 3);
                assert_eq!(new_generation, 4);
                assert_eq!(old_wait_id, current.wait_id);
                assert_eq!(started_at_ms, 200);
            }
            other => panic!("expected BeginReplace, got {other:?}"),
        }
    }

    #[test]
    fn replace_stale_or_other_channel_id_is_conflict() {
        let current = slot("wait_bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", 3, false);
        let d = decide_acquire(
            Some(&current),
            &WaitAcquireIntent::Replace {
                wait_id: fresh_id(),
            },
            4,
            200,
            fresh_id(),
        );
        assert_eq!(d, WaitAcquireDecision::ConflictChanged);
    }

    #[test]
    fn replace_on_empty_key_is_conflict() {
        let d = decide_acquire(
            None,
            &WaitAcquireIntent::Replace {
                wait_id: fresh_id(),
            },
            1,
            1,
            fresh_id(),
        );
        assert_eq!(d, WaitAcquireDecision::ConflictChanged);
    }

    #[test]
    fn malformed_replace_token_is_conflict() {
        let current = slot("wait_bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", 3, false);
        let d = decide_acquire(
            Some(&current),
            &WaitAcquireIntent::Replace {
                wait_id: "not-a-wait-id".into(),
            },
            4,
            1,
            fresh_id(),
        );
        assert_eq!(d, WaitAcquireDecision::ConflictChanged);
    }

    #[test]
    fn in_flight_replace_consumes_old_token() {
        let current = slot("wait_bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", 3, true);
        let d = decide_acquire(
            Some(&current),
            &WaitAcquireIntent::Replace {
                wait_id: current.wait_id.clone(),
            },
            4,
            1,
            fresh_id(),
        );
        assert_eq!(d, WaitAcquireDecision::ConflictChanged);

        let d = decide_acquire(
            Some(&current),
            &WaitAcquireIntent::Default,
            4,
            1,
            fresh_id(),
        );
        assert!(matches!(d, WaitAcquireDecision::RefuseActive { .. }));
    }

    #[test]
    fn old_guard_cannot_release_newer_generation() {
        let newer = slot("wait_cccccccccccccccccccccccccccccccc", 5, false);
        assert!(!should_release(Some(&newer), 3));
        assert!(should_release(Some(&newer), 5));
        assert!(!should_release(None, 5));
    }

    #[test]
    fn replacement_install_is_generation_safe() {
        let replacing = slot("wait_bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", 3, true);
        assert!(can_install_replacement(Some(&replacing), 3, 4));
        assert!(can_install_replacement(None, 3, 4));
        let newer = slot("wait_cccccccccccccccccccccccccccccccc", 5, false);
        assert!(!can_install_replacement(Some(&newer), 3, 4));
    }
}
