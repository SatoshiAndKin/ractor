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

Application integration at flashprofits-monorepo revision `f54f960a` passes
1,917 default tests and 1,963 all-feature tests. All 18 GPU checks run; strict
release Clippy, the GPU benchmark build, and the offline read-only Linux image
check also pass. The application pins repair commit `08db925`.

The repeated diagnostic still exceeds its 7 GiB RSS guard at 7,631,011,840 bytes.
Its jemalloc profile identifies much smaller queue blocks, but captures a
different workload progress point and head. The normal mimalloc capture reaches
the unchanged 45-minute ceiling with no complete corpus or benchmark winner.
Its 8 GiB cgroup records 8,128,217,088 peak bytes and no OOM event. Neither run
proves full-workload memory or timing acceptance. The focused queue repair does
not by itself make application PR113 ready to merge.

## Running actor port storage

The retained application profile also attributes 1,069,082,866 sampled heap
bytes to actor tasks and processing-loop allocations. The private runtime moved
its port set through nested asynchronous frames, retaining storage for completed
moves. The running loop now borrows the task's port set. The task explicitly
drops the receivers before publishing its termination event, preserving the
previous queue-closure and supervision order. No queue, message, scheduling,
public API, or per-message allocation policy changes.

The new regression starts 1,024 linked actors with 464-byte states and exchanges
real messages before measuring. It checks exact state and reply sequence again
after the snapshot and verifies every post-stop hook. System-allocator counts
include nested allocations, ports, queues, tasks, state, and supervision links:

| Tokio configuration | Before | After |
| --- | ---: | ---: |
| Ordinary | 4,213,020 | 4,188,444 |
| Instrumented (`--cfg tokio_unstable`) | 5,654,812 | 4,598,044 |

The instrumented configuration used by Flashprofits retains 18.7% fewer requested
bytes in this fixture. The ordinary configuration saves 0.6%. Both final local
budgets fail before the repair. Cluster builds use a separate bound because
their message variants are larger; that configuration is a passing regression
control before and after. These counts are not RSS or complete-catalog evidence.

On `nightly-2026-08-25`, the default/output-port suite and the same suite with
`RUSTFLAGS='--cfg tokio_unstable'` each pass 180 tests. Expanded Tokio features
`cluster,monitors,output-port-v2,async-trait,actor-macros,blanket_serde` pass 238
tests. The existing no-default-features async-std configuration passes 225.
The new allocation tests are Tokio-only. Formatting passes. Stable all-target,
all-feature Clippy passes with the four previously recorded upstream lint
classes and `io_other_error` allowed; that additional existing error is in
`ractor/src/macros/tests.rs:67`. No source allowances were added.

The instrumented release small-message benchmark compares the previous runtime
and this repair with 20 samples, one second of warm-up, and three seconds of
measurement. The 8-byte case takes 1.85% longer (1.82% lower throughput). The
16-byte case has no statistically significant change; the 32-byte difference
is within Criterion's noise threshold. Preserve this small measured cost.
Application integration and the original bounded production gates remain
required before claiming production acceptance.
