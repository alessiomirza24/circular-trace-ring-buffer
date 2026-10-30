//! Bounded lock-free ring buffer for recent trace spans.
//!
//! The buffer keeps a fixed number of `TraceSpan` records, overwriting the
//! oldest when full. Writes and reads happen through a single
//! `AtomicUsize` head index, which keeps contention minimal and avoids any
//! locking in the hot path of span emission.
//!
//! Interpretation chosen for this crate: a *trace span* is a timestamped
//! name plus an optional free-form `payload` string. The timestamp is an
//! explicit `nanos_since_epoch` field (set by the caller) so that tests can
//! be fully deterministic — the library never reads a wall clock.

use std::cell::UnsafeCell;
use std::sync::atomic::{AtomicUsize, Ordering};

/// A single trace span stored in the ring buffer.
///
/// `nanos_since_epoch` is deliberately a raw `u64` rather than a
/// `std::time::SystemTime` so that callers (and tests) can supply any
/// value they like. The buffer never interprets or compares timestamps.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TraceSpan {
    pub nanos_since_epoch: u64,
    pub name: String,
    pub payload: String,
}

impl TraceSpan {
    /// Convenience constructor mirroring the struct field order.
    pub fn new(nanos_since_epoch: u64, name: impl Into<String>, payload: impl Into<String>) -> Self {
        TraceSpan {
            nanos_since_epoch,
            name: name.into(),
            payload: payload.into(),
        }
    }
}

/// Bounded, single-producer-friendly ring buffer of trace spans.
///
/// `head` always points to the slot that the *next* write will occupy. Reads
/// use a snapshot of `head` to decide which slots currently hold valid data.
/// Capacity is fixed at construction; `len()` reports how many slots are
/// populated and never exceeds `capacity`.
pub struct TraceRingBuffer {
    slots: UnsafeCell<Vec<Option<TraceSpan>>>,
    head: AtomicUsize,
    capacity: usize,
}

// Safety: the buffer uses a single atomic head to coordinate access. It is
// designed for low-contention single-producer or few-producer use; the
// atomic head ensures that distinct writes target distinct slots.
unsafe impl Sync for TraceRingBuffer {}

impl TraceRingBuffer {
    /// Create a buffer holding at most `capacity` spans.
    ///
    /// `capacity == 0` is allowed and produces a buffer that silently drops
    /// every span written to it; `export()` on such a buffer is always empty.
    pub fn new(capacity: usize) -> Self {
        let slots = (0..capacity).map(|_| None::<TraceSpan>).collect::<Vec<_>>();
        TraceRingBuffer {
            slots: UnsafeCell::new(slots),
            head: AtomicUsize::new(0),
            capacity,
        }
    }

    /// Fixed maximum number of spans the buffer retains.
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Append a span, overwriting the oldest entry when full.
    ///
    /// This is the hot path: one relaxed atomic load, one store, and a direct
    /// slot write. No locks, no allocation.
    pub fn push(&self, span: TraceSpan) {
        if self.capacity == 0 {
            return;
        }
        let idx = self.head.fetch_add(1, Ordering::Relaxed) % self.capacity;
        // Safety: idx is in [0, capacity) by construction, and we only ever
        // hand out &mut through this single guarded path.
        let slots = unsafe { &mut *self.slots.get() };
        slots[idx] = Some(span);
    }

    /// Current number of populated slots. Because `push` writes to a slot
    /// before the next `fetch_add` returns, this is a lower bound on the
    /// number of spans written so far, capped at `capacity`.
    pub fn len(&self) -> usize {
        let written = self.head.load(Ordering::Relaxed);
        written.min(self.capacity)
    }

    /// `true` when no spans have been written yet.
    pub fn is_empty(&self) -> bool {
        self.head.load(Ordering::Relaxed) == 0
    }

    /// Return all retained spans in oldest-first order.
    ///
    /// If the buffer has wrapped, the slot at `head % capacity` is the oldest
    /// still-present entry, so we start there and walk `capacity` slots.
    /// On a non-wrapped buffer we start at slot 0 and walk `len()` slots.
    /// Both cases are covered by starting at `min(written, capacity) %
    /// capacity` only when wrapped — but simpler and correct: start at 0 when
    /// not yet wrapped, otherwise start at `head % capacity`.
    pub fn export(&self) -> Vec<TraceSpan> {
        if self.capacity == 0 {
            return Vec::new();
        }
        let written = self.head.load(Ordering::Relaxed);
        if written == 0 {
            return Vec::new();
        }
        let count = written.min(self.capacity);
        let mut out = Vec::with_capacity(count);
        // If the buffer has wrapped, the oldest surviving span lives at the
        // current head index; otherwise it lives at index 0.
        let start = if written > self.capacity {
            written % self.capacity
        } else {
            0
        };
        // Safety: read-only access; no concurrent mutation of slots during export
        // in the single-producer model this crate targets.
        let slots = unsafe { &*self.slots.get() };
        for i in 0..count {
            let idx = (start + i) % self.capacity;
            if let Some(span) = &slots[idx] {
                out.push(span.clone());
            }
        }
        out
    }

    /// Remove every span, resetting the buffer to its empty state.
    pub fn clear(&self) {
        if self.capacity == 0 {
            return;
        }
        let slots = unsafe { &mut *self.slots.get() };
        for i in 0..self.capacity {
            slots[i] = None;
        }
        self.head.store(0, Ordering::Relaxed);
    }
}

impl Default for TraceRingBuffer {
    fn default() -> Self {
        Self::new(256)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_buffer_has_zero_len() {
        let buf = TraceRingBuffer::new(4);
        assert_eq!(buf.len(), 0);
        assert!(buf.is_empty());
        assert_eq!(buf.capacity(), 4);
        assert!(buf.export().is_empty());
    }

    #[test]
    fn push_then_export_returns_span() {
        let buf = TraceRingBuffer::new(4);
        buf.push(TraceSpan::new(100, "db.query", "select 1"));
        assert_eq!(buf.len(), 1);
        assert!(!buf.is_empty());
        let out = buf.export();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].name, "db.query");
        assert_eq!(out[0].payload, "select 1");
        assert_eq!(out[0].nanos_since_epoch, 100);
    }

    #[test]
    fn export_preserves_insertion_order_when_not_wrapped() {
        let buf = TraceRingBuffer::new(8);
        for i in 0..5u64 {
            buf.push(TraceSpan::new(i, format!("op-{i}"), String::new()));
        }
        let out = buf.export();
        assert_eq!(out.len(), 5);
        for (idx, span) in out.iter().enumerate() {
            assert_eq!(span.nanos_since_epoch, idx as u64);
            assert_eq!(span.name, format!("op-{idx}"));
        }
    }

    #[test]
    fn wrap_around_overwrites_oldest() {
        let buf = TraceRingBuffer::new(3);
        for i in 0..7u64 {
            buf.push(TraceSpan::new(i, format!("op-{i}"), String::new()));
        }
        assert_eq!(buf.len(), 3);
        let out = buf.export();
        assert_eq!(out.len(), 3);
        // The last three written are op-4, op-5, op-6, in that order.
        assert_eq!(out[0].name, "op-4");
        assert_eq!(out[1].name, "op-5");
        assert_eq!(out[2].name, "op-6");
    }

    #[test]
    fn exactly_full_buffer_exports_in_order() {
        let buf = TraceRingBuffer::new(4);
        for i in 0..4u64 {
            buf.push(TraceSpan::new(i, format!("op-{i}"), String::new()));
        }
        assert_eq!(buf.len(), 4);
        let out = buf.export();
        assert_eq!(out.len(), 4);
        for (idx, span) in out.iter().enumerate() {
            assert_eq!(span.name, format!("op-{idx}"));
        }
    }

    #[test]
    fn zero_capacity_drops_everything() {
        let buf = TraceRingBuffer::new(0);
        assert_eq!(buf.capacity(), 0);
        buf.push(TraceSpan::new(1, "x", "y"));
        assert_eq!(buf.len(), 0);
        assert!(buf.is_empty());
        assert!(buf.export().is_empty());
    }

    #[test]
    fn one_slot_buffer_keeps_latest() {
        let buf = TraceRingBuffer::new(1);
        buf.push(TraceSpan::new(1, "first", String::new()));
        buf.push(TraceSpan::new(2, "second", String::new()));
        assert_eq!(buf.len(), 1);
        let out = buf.export();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].name, "second");
    }

    #[test]
    fn clear_resets_buffer() {
        let buf = TraceRingBuffer::new(4);
        for i in 0..10u64 {
            buf.push(TraceSpan::new(i, format!("op-{i}"), String::new()));
        }
        assert_eq!(buf.len(), 4);
        buf.clear();
        assert_eq!(buf.len(), 0);
        assert!(buf.is_empty());
        assert!(buf.export().is_empty());
    }

    #[test]
    fn default_capacity_is_256() {
        let buf = TraceRingBuffer::default();
        assert_eq!(buf.capacity(), 256);
        assert!(buf.is_empty());
    }

    #[test]
    fn clear_on_zero_capacity_is_noop() {
        let buf = TraceRingBuffer::new(0);
        buf.clear();
        assert_eq!(buf.len(), 0);
    }

    #[test]
    fn export_does_not_mutate_buffer() {
        let buf = TraceRingBuffer::new(3);
        buf.push(TraceSpan::new(1, "a", String::new()));
        buf.push(TraceSpan::new(2, "b", String::new()));
        let _ = buf.export();
        let _ = buf.export();
        assert_eq!(buf.len(), 2);
        let out = buf.export();
        assert_eq!(out.len(), 2);
        assert_eq!(out[0].name, "a");
        assert_eq!(out[1].name, "b");
    }

    #[test]
    fn many_writes_then_export_latest_window() {
        let buf = TraceRingBuffer::new(5);
        for i in 0..100u64 {
            buf.push(TraceSpan::new(i, format!("span-{i}"), String::new()));
        }
        assert_eq!(buf.len(), 5);
        let out = buf.export();
        assert_eq!(out.len(), 5);
        assert_eq!(out[0].name, "span-95");
        assert_eq!(out[4].name, "span-99");
    }
}
