# streamq-bench: frozen CLI contract (M10 plan R9)

This file is part of the M10 contract. The bench tool (package A) and the README (package B) both
follow it. Changes need a CONTRACT ISSUE.

```
go run ./bench/cmd/streamq-bench [flags]
```

## Flags

| flag | default | meaning |
|---|---|---|
| `-preset` | (none) | `quick`, `nfr1`, `nfr2`, `nfr1-nonidem`; explicit flags override preset values |
| `-cluster` | `inprocess:1` | `inprocess:1` or `inprocess:3` (brokers started in this process) |
| `-bootstrap` | (empty) | `host:port,...` of an external cluster; replaces `-cluster` |
| `-data-dir` | temp dir | parent dir for in-process broker data (removed on exit) |
| `-topic` | `bench` | created if missing |
| `-partitions` | `6` | partitions of a created topic |
| `-rf` | `1` for inprocess:1, else `3` | replication factor of a created topic |
| `-min-insync` | `1` for RF 1, else `2` | min.insync.replicas of a created topic |
| `-acks` | `all` | `all`, `leader` or `none` |
| `-idempotent` | `true` | idempotence (applies with acks=all only, D48) |
| `-record-size` | `1024` | value bytes per record (≥ 16 with `-consume`) |
| `-key-cardinality` | `0` | number of distinct keys; 0 = unkeyed (sticky partitioner) |
| `-rate` | `0` | total target msgs/s across all producers (one shared token bucket); 0 = as fast as possible |
| `-producers` | `4` | independent Client+Producer pairs, one goroutine each |
| `-linger` | `5ms` | ProducerConfig.Linger |
| `-batch-size` | `16384` | ProducerConfig.BatchSize in bytes |
| `-duration` | `30s` | measurement window |
| `-warmup` | `5s` | excluded from results |
| `-consume` | `false` | also consume and report end-to-end latency |
| `-json` | (empty) | write the full result as JSON to this path (mode 0600) |
| `-cpuprofile` | (empty) | write a CPU profile of the run |

## Presets

| preset | cluster | rf / min.insync | acks | idempotent | size | rate | partitions | producers | linger | batch | duration |
|---|---|---|---|---|---|---|---|---|---|---|---|
| `quick` | inprocess:1 | 1 / 1 | leader | – | 1 KiB | max | 6 | 4 | 5 ms | 16 KiB | 10 s (warmup 2 s) |
| `nfr1` | inprocess:3 | 3 / 2 | all | yes | 1 KiB | max | 6 | 8 | 10 ms | 512 KiB | 30 s |
| `nfr1-nonidem` | as `nfr1` | 3 / 2 | all | no | 1 KiB | max | 6 | 8 | 10 ms | 512 KiB | 30 s |
| `nfr2` | inprocess:1 | 1 / 1 | leader | – | 1 KiB | 5000 | 6 | 4 | 1 ms | 16 KiB | 30 s |

## `make bench`
Runs the storage microbenchmarks, then `go run ./bench/cmd/streamq-bench -preset quick`.

## Output
- A table on stdout with throughput (msgs/s, MiB/s), latency p50/p99/p999/max, and the
  effective config. Latency is measured from the intended send time when `-rate > 0`; acks=none
  reports send latency.
- Latency counts records whose intended send time is inside the measurement window, whenever
  they are acked. Acks are collected for a bounded drain afterwards (the lesser of the delivery
  timeout and 30 s). Records still unacked are reported as timeouts. Throughput counts acks inside
  the window.
- Status `OK`, or `INVALID` (exit 1) when any partition's leader epoch or partition epoch (an ISR
  change) moved during the window, or the run failed. With `-bootstrap`, only a change still in
  place at the end is detected.
- `-help` states the estimated disk use (rate × size × RF × time). The run refuses to start
  without enough free space.

**JSON top-level fields:** `status`, `config` (effective workload and ProducerConfig),
`environment` (cpu, cores, ram, os, kernel, go, commit, dirty, filesystem, flush_policy,
peak_rss),
`produce` (count, errors_by_kind, msgs_per_sec, mib_per_sec, latency_ms{p50,p99,p999,max},
service_ms{p50,p99,p999,max}, behind_schedule), `end_to_end` (when `-consume` is set, with
latency_ms), and `cluster_checks` (leader_epoch_changes, partition_epoch_changes), and `rate_model`
("shared-token-bucket").

Additional fields, added during implementation and accepted at integration:
- top level: `invalid_reasons`;
- `produce`: `sent`, `acked_in_window`, `timeouts`, `latency_kind`, `window_s`, `drain_s`;
- `cluster_checks`: `source` (`metadata-store` or `admin-describe`), `isr_below_rf`, `problems`;
- `config`: `producer_config`.

In-process single-node runs check stability through `admin-describe`. `internal/node` exposes no
metadata store in single-node mode, and with RF 1 on one broker the leader and ISR cannot change.

Signals: the first SIGINT/SIGTERM drains, closes and cleans up, printing partial results
(INVALID). A second signal exits immediately and leaves the temporary data dir behind. Produce
errors or timeouts also make a run INVALID.

`bench/USAGE.md` is frozen: implementers must not edit it (M10 plan R15).
