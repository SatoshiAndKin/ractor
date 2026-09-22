# Compact actor queues

This focused repair starts from Ractor 0.16.5 (`f18b15c`). It stores boxed
supervision events and message envelopes in the existing unbounded queues.
The public API, admission counter, drain marker ordering, stop/kill ports,
supervision tree, and receiver cleanup stay unchanged. Thread-local actors
use the same queue representation.

On an ARM64 Mac, the same 1,024 linked actors with the System allocator retain
6,956,540 requested bytes before the change and 3,024,380 after it. The
4,194,304-byte regression budget fails on the release and passes on the repair.
These are requested heap bytes, not RSS or production acceptance. The test
includes actor tasks, ports, properties, links, and any queued start events.

Validation on the repaired source:

- `cargo nextest run -p ractor --features output-port-v2`: 179 passed.
- Tokio with `cluster,monitors,output-port-v2,async-trait,actor-macros,blanket_serde`:
  237 passed.
- No defaults, with `async-std,async-trait,monitors,output-port-v2,cluster`:
  225 passed. The three Tokio-only integration tests do not run in this mode;
  the two new closed-channel ownership tests and existing lifecycle tests run.
- Formatting passes. Strict all-target Clippy on stable 1.98 reports 14 existing
  errors on the unchanged release. The repair passes with only those four
  existing lint classes allowed: `result_large_err`, `unnecessary_map_or`,
  `collapsible_match`, and `unnecessary_to_owned`. No source-level allowances
  were added. Runtime checks use nightly-2026-08-25; the allocation before/after
  pair used the same installed nightly toolchain and Tokio 1.53.1.

The existing `simple_advanced_benchmarks` small-message benchmark used 20
samples, one second of warm-up, and three seconds of measurement per case.
For 8-, 16-, and 32-byte messages, the extra envelope allocation increased time
by 18.0%, 11.4%, and 10.8%, respectively. This is a measured throughput cost,
not a throughput optimization. Application validation and the original bounded
full-catalog capture must establish whether the memory tradeoff is acceptable.

New tests verify exact message delivery and per-producer order across queue
blocks and concurrent drain, original message ownership on closed sends,
original supervision event ownership on closed sends, and exactly one start
and termination event for linked children under drain, stop, and kill.
Existing tests cover cluster serialization, process-group notifications,
thread-local actors, cancellation, supervision failures, and output ports.

OpenAI Codex authored this repair and its regressions at the owner's request.
