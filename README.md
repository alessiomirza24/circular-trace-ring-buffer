# circular-trace-ring-buffer

A bounded lock-free ring buffer that keeps the most recent trace spans in memory so they can be exported on demand — on error, on a trigger, or for periodic sampling.

## Usage

```rust
use circular_trace_ring_buffer::{TraceRingBuffer, TraceSpan};

let buf = TraceRingBuffer::new(64);
buf.push(TraceSpan::new(1_000_000, "http.request", "GET /orders"));
buf.push(TraceSpan::new(1_000_500, "db.query", "select * from orders"));

let spans = buf.export();
for s in &spans {
    println!("{} {} {}", s.nanos_since_epoch, s.name, s.payload);
}
```

`TraceSpan` is a plain struct with three public fields: `nanos_since_epoch: u64`, `name: String`, and `payload: String`. Construct it with `TraceSpan::new(nanos, name, payload)` or by struct literal.

`TraceRingBuffer` exposes `new(capacity)`, `default()` (capacity 256), `push(span)`, `export() -> Vec<TraceSpan>`, `len()`, `is_empty()`, `capacity()`, and `clear()`.

## Why

When something goes wrong in a long-running process you usually want the last few spans that led up to the failure, not a firehose of everything that ever happened. Keeping every span forever is wasteful and leaking all of it into a remote collector is noisy. This buffer holds the most recent N spans in fixed memory and lets you drain them only when you actually need them.

The trade-off is that older spans are silently dropped once the buffer wraps. If you need complete traces, this is the wrong tool — use a real collector. This is for the case where "the last 64 spans before the crash" is enough to start debugging.

Timestamps are a caller-supplied `u64` rather than a `SystemTime` so the library reads no clock and stays deterministic for tests. If you want wall-clock timestamps, pass `nanos_since_epoch` from whatever clock your process already trusts.

## Edge cases

- `TraceRingBuffer::new(0)` is valid. Every `push` is a silent no-op and `export()` is always empty. This keeps call sites uniform when the capacity is configured to zero.
- `export()` returns spans in oldest-first order. On a wrapped buffer the oldest surviving span is not at slot 0 — the buffer tracks a head index and `export()` starts there so callers never have to reason about wrap-around themselves.
- `len()` reports populated slots, never the number of writes. After 1 000 writes into a capacity-8 buffer, `len()` is 8, not 1 000.
- The buffer is `Sync` via a single `AtomicUsize` head and interior slot mutation. It is designed for low-contention single-producer or few-producer use; many producers all hammering `push` will contend on the same atomic but will not corrupt state.
