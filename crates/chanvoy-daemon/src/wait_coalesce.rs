//! Fixed-window coalescing buffer for wait follow v2.
//!
//! The first admitted match opens one non-sliding window of `coalesce_ms`.
//! Later matches do not extend that deadline. Flush on window expiry, 32
//! messages, phase change, or terminal. Never drop an admitted match.

use std::time::Duration;

use chanvoy_core::{WaitFollowMode, WAIT_FOLLOW_COALESCE_MAX_MESSAGES};
use tokio::time::Instant;

pub struct CoalesceFlush<T> {
    pub mode: WaitFollowMode,
    pub items: Vec<T>,
}

pub struct CoalesceBuffer<T> {
    coalesce_ms: u64,
    pending: Vec<T>,
    mode: Option<WaitFollowMode>,
    window_start: Option<Instant>,
}

impl<T> CoalesceBuffer<T> {
    pub fn new(coalesce_ms: u64) -> Self {
        Self {
            coalesce_ms,
            pending: Vec::new(),
            mode: None,
            window_start: None,
        }
    }

    pub fn deadline(&self) -> Option<Instant> {
        let start = self.window_start?;
        start.checked_add(Duration::from_millis(self.coalesce_ms))
    }

    /// Push an admitted match. Returns bursts that must be emitted first
    /// (phase change and/or a full 32-cap window that now includes `item`).
    pub fn push(&mut self, mode: WaitFollowMode, item: T) -> Vec<CoalesceFlush<T>> {
        let mut out = Vec::new();
        if self.mode.is_some_and(|current| current != mode) {
            if let Some(flush) = self.take() {
                out.push(flush);
            }
        }
        if self.pending.is_empty() {
            self.window_start = Some(Instant::now());
            self.mode = Some(mode);
        }
        self.pending.push(item);
        if self.pending.len() >= WAIT_FOLLOW_COALESCE_MAX_MESSAGES {
            if let Some(flush) = self.take() {
                out.push(flush);
            }
        }
        out
    }

    pub fn take(&mut self) -> Option<CoalesceFlush<T>> {
        if self.pending.is_empty() {
            return None;
        }
        let mode = self.mode.take().unwrap_or(WaitFollowMode::Live);
        self.window_start = None;
        Some(CoalesceFlush {
            mode,
            items: std::mem::take(&mut self.pending),
        })
    }

    pub fn take_if_expired(&mut self, now: Instant) -> Option<CoalesceFlush<T>> {
        let deadline = self.deadline()?;
        if now >= deadline {
            self.take()
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn thirty_two_flushes_without_dropping_the_next_match() {
        let mut buf = CoalesceBuffer::new(10_000);
        let mut flushes = Vec::new();
        for i in 0..33u8 {
            flushes.extend(buf.push(WaitFollowMode::Live, i));
        }
        assert_eq!(flushes.len(), 1);
        assert_eq!(flushes[0].items.len(), 32);
        assert_eq!(flushes[0].items[0], 0);
        assert_eq!(flushes[0].items[31], 31);
        assert_eq!(buf.take().unwrap().items, vec![32]);
    }

    #[test]
    fn phase_change_flushes_before_mixing() {
        let mut buf = CoalesceBuffer::new(10_000);
        assert!(buf.push(WaitFollowMode::Backlog, 1).is_empty());
        let flushes = buf.push(WaitFollowMode::Live, 2);
        assert_eq!(flushes.len(), 1);
        assert_eq!(flushes[0].mode, WaitFollowMode::Backlog);
        assert_eq!(flushes[0].items, vec![1]);
        assert_eq!(buf.take().unwrap().items, vec![2]);
    }

    #[test]
    fn later_matches_do_not_slide_the_window() {
        let mut buf = CoalesceBuffer::new(1_000);
        assert!(buf.push(WaitFollowMode::Live, 1).is_empty());
        let first_deadline = buf.deadline().unwrap();
        assert!(buf.push(WaitFollowMode::Live, 2).is_empty());
        assert_eq!(buf.deadline().unwrap(), first_deadline);
        assert!(buf.take_if_expired(first_deadline).is_some());
    }
}
