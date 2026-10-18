# streamq chaos & correctness report (M9)

This report covers SPEC §9 M9: a Porcupine model for single-partition produce (acks=all) and
fetch, and chaos runs against the Docker Compose cluster. The cluster has kill -9 of random
brokers and of the controller, Toxiproxy cuts and latency, a frozen leader, and a full disk. The
plan is `docs/plans/M9.md`; the decisions are D56–D58.

**Result: every scenario passed. No safety violation was found:**
- no lost acknowledged record;
- no duplicate from the idempotent producer;
- no reordering, phantom or offset mismatch;
- no read integrity anomaly;
- every history linearizable with respect to the D56 log model: prefix reads, which need not be
  monotonic or reflect completed acks; acknowledged offsets in real-time order; durability
  checked separately by `VerifyFinal`.

The run below (seed 835244600) was made by the main session on merged `main` after all M9
review fixes. Earlier main-session runs on pre-fix code also passed: seeds 101242816 and
144549489. The run detailed below shows the same inbound-isolation pattern. Earlier runs from the
implementer's worktree, with seeds 1, 2, 3, 216491142 and 786830496, also passed. They count
only as supporting evidence (plan R18).

Reproduce: `make chaos` (about 11 minutes), or rerun one scenario with the seed below:
`go run ./test/chaos/cmd/chaos -compose deploy/docker-compose.yml -override deploy/docker-compose.chaos.yml -project streamq-chaos -ctl ./bin/streamqctl -out test/chaos/results -seed <seed> -scenarios <name>`.

## What is checked

**`lin` workload** (topic `lin`: 1 partition, RF 3, min.insync 2):
- 4 clients make single-attempt acks=all appends of unique values and whole-partition reads up to
  the HW.
- Failures back off 50–200 ms.
- Outcomes:
  - Appends that fail before the write are dropped from the history.
  - Every other failure is an *unknown* outcome, open-ended in time (D56).
- After healing, sentinel appends settle the cluster, then a final read reaches the sentinel.
- Checks:
  - no read integrity anomaly (gap, offset going backwards, CRC/decode failure, no progress below
    the broker's own HW);
  - `VerifyFinal`: every acknowledged (value, offset) is at that offset of the final log, every
    read is a prefix of it, no phantom, no duplicate;
  - porcupine (`Prepare`d history) is Ok. Illegal is a violation; a timeout counts as
    inconclusive, which also fails.

**`bulk` workload** (topic `bulk`: 6 partitions, RF 3, min.insync 2): the client library's
idempotent producer (acks=all) sends keyed sequence values.
- Every acked value is present exactly once at its acked offset.
- Acked values of a key are in sequence order.
- Failed values appear at most once.
- Nothing appears that was never produced.

**The same model in-process.** `test/linearizability` (run by `make test-int`) applies the same
model to a 3-node in-process cluster with 8 graceful leader and follower restarts, about
3,500–5,500 operations per run.

## Findings across scenarios

- **Safety held everywhere:**
  - leader crashes (kill -9 with no shutdown);
  - controller crashes;
  - a GC-pause zombie (`docker pause` for 20 s), whose in-flight appends all came back unknown
    and never reached the log;
  - inbound isolation;
  - heavy latency;
  - ENOSPC on a leader.
- **acks=all stalls during a follower crash, not only a leader crash (availability).** The dead
  follower stays in the ISR until it is fenced (9 s session timeout) or shrunk (10 s lag), so
  each kill -9 stops acks for about 8–10 s. This matches Kafka's `replica.lag.time.max.ms`
  behaviour; a shorter session timeout trades false fencing for shorter stalls.
- **Inbound-only isolation of any broker causes a cluster-wide outage (availability, D59).**
  Safety held in all 3 rounds. In run `20261018T223753Z` (its `-inbound-isolation.log`), round 1
  isolated broker3, which was **not** the controller (controller 1):
  - About 2 s after its inbound proxies were cut, broker3 stopped receiving Raft heartbeats.
  - Its *outbound* links still worked, so it passed pre-vote (`tally=2`) and won the election at
    term 3. It became controller at 22:42:12.
  - All broker heartbeats, its own included, reach the controller through the isolated broker's
    inbound proxy. Ten seconds later it fenced brokers 1, 2 and 3.
  - Rounds 2 and 3 ended the same way.

  Each round ends with leader −1 and ISR = {one broker}. The cut leader first shrinks the ISR to
  itself (its followers cannot fetch). Fencing it then leaves no clean leader, and there is no
  unclean election (compare Kafka's KIP-966 ELR). `lin` was unavailable for 57.9 s across the 3
  rounds and recovered about 6 s after each heal.

  This is an availability weakness against asymmetric network faults. Kafka KRaft's Raft
  check-quorum would not catch it either, because the isolated node's outbound requests and
  their replies succeed. Possible mitigations are recorded as follow-ups in D59 and
  PROGRESS_LOG.
- **Full disk:**
  - The broker did not crash (no SIGBUS). It marked the affected logs failed and stayed the
    failing leader of its partitions until the disk was replaced, as D57 describes.
  - The other brokers kept serving: thousands of acks on their partitions while broker3's disk
    was full.
  - After the disk replacement (fence, then an empty log volume) broker3 caught up and the
    cluster was healthy within about 5 s.
  - There is no automatic log-dir failover (a Kafka JBOD-style policy). This is a known
    limitation.
- **Latency (200 ± 100 ms plus fragmentation on every broker-to-broker link for 60 s):** there
  were no leader or ISR changes. About 2,100 bulk records exceeded the 45 s delivery timeout
  before being sent, because of one batch in flight per partition (D48). They were reported as
  failed and none were in the log.

Porcupine reports "Ok (0.0s)" in every row by design. After `Prepare`, every append has a fixed
offset (acknowledged, or pinned to the final log) and the never-applied unknowns are gone. The
search is therefore almost deterministic. Porcupine adds two checks: acknowledged and pinned
offsets are consistent with real-time order, and no read shows a value before its append was
invoked. Reads are not required to be monotonic or to reflect completed acks (D56). Durability
and offsets are carried by `VerifyFinal`, so a check that finishes in milliseconds is expected.

## Limitations of this testing

- `docker kill -9` keeps the host page cache, so fsync and power-loss durability are **not**
  exercised (that is FR8's model; storage recovery has its own unit tests from M1).
- `inbound-isolation` cuts inbound traffic only. A true symmetric network partition needs
  NET_ADMIN and iptables in the image and is out of scope. The GC-pause zombie is covered by
  `pause-leader`.
- Each scenario runs once per seed for about 90 s. These are probabilistic tests, not proofs.
  Any failure prints its seed for exact replay.
- The workloads cover one `lin` partition and six `bulk` partitions on a 3-broker laptop
  cluster.

## Run details

- git SHA: `016d47235aa23a97d940b695a85d8fce6310d6d9`
- image: `streamq:chaos` `sha256:b4fe12a6bdb62e18d5273aac58111375adede8768c6c9dcbc95564eaa1d01b64`
- seed: `835244600` (rerun: `go run ./test/chaos/cmd/chaos ... -seed 835244600 -scenarios <name>`)
- environment: 13th Gen Intel(R) Core(TM) i5-1345U, 6 CPUs, RAM 8134792 kB, kernel 6.18.54.1-microsoft-standard-WSL2, Docker 29.5.2, Compose 5.1.4, go1.27.2

| scenario | status | lin acked | lin unknown (in final / pruned) | lin definite | lin reads (failed) | anomalies | porcupine | bulk acked | bulk failed (present) | bulk lost | recovery | lin unavailable |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| kill9-random | PASS | 2528 | 428 (16 / 412) | 0 | 1105 (168) | 0 | Ok (0.0s) | 29776 | 0 (0) | 0 | 4.1s | 35.5s |
| kill9-controller | PASS | 1673 | 183 (20 / 163) | 0 | 738 (75) | 0 | Ok (0.0s) | 20395 | 0 (0) | 0 | 3.1s | 26.5s |
| inbound-isolation | PASS | 1390 | 56 (44 / 12) | 0 | 601 (0) | 0 | Ok (0.0s) | 24719 | 0 (0) | 0 | 3.7s | 55.7s |
| pause-leader | PASS | 1346 | 13 (1 / 12) | 0 | 600 (3) | 0 | Ok (0.0s) | 20136 | 0 (0) | 0 | 1.6s | 25.8s |
| latency | PASS | 1240 | 0 (0 / 0) | 0 | 474 (1) | 0 | Ok (0.0s) | 11839 | 2112 (0) | 0 | 0.0s | 0.0s |
| full-disk | PASS | 1078 | 941 (0 / 941) | 0 | 808 (103) | 0 | Ok (0.0s) | 16129 | 2899 (0) | 0 | 3.7s | 40.5s |

### kill9-random: PASS

**Faults injected.** 4 rounds: after 10–20 s, SIGKILL a random broker other than the previous round's (docker compose kill -s SIGKILL), start it again after 5–10 s.

Results file `20261018T231458Z-kill9-random.json`, broker logs `20261018T231458Z-kill9-random.log`, schedule seed 18380825945804430729, workload 119.2s, total 126.6s.

**Invariants checked.**
- lin: no read integrity anomaly (gap, offset going backwards, CRC/decode failure, no progress below the HW)
- lin: VerifyFinal: every acked (value, offset) is at that offset of the settled final log, every read is a prefix of it, no phantom, no duplicate
- lin: porcupine check of the prepared history is Ok (Illegal = violation, Unknown = inconclusive = failure)
- bulk: every acked value present exactly once at its acked partition/offset; acked values of a key in sequence order; failed values at most once; nothing never produced
- availability (not safety): the cluster passes `streamqctl cluster status --strict` after healing

**Fault timeline** (seconds since the workloads started):

- 17.8s kill -9 broker1: round 1: random broker; lin before: leader 3 epoch 0 isr [3 1 2] (from broker 1)
- 25.9s start broker1: lin while down: leader 3 epoch 0 isr [3 1 2] (from broker 2)
- 45.5s kill -9 broker3: round 2: random broker; lin before: leader 3 epoch 0 isr [3 1 2] (from broker 1)
- 54.3s start broker3: lin while down: leader 3 epoch 0 isr [3 1 2] (from broker 1)
- 72.3s kill -9 broker2: round 3: random broker; lin before: leader 1 epoch 1 isr [3 1 2] (from broker 1)
- 80.3s start broker2: lin while down: leader 1 epoch 1 isr [3 1 2] (from broker 1)
- 96.9s kill -9 broker1: round 4: random broker; lin before: leader 1 epoch 1 isr [3 1 2] (from broker 1)
- 105.1s start broker1: lin while down: leader 1 epoch 1 isr [3 1 2] (from broker 2)

**Safety.** lin: acked 2528, unknown 428 (in final 16, pruned 412), definite 0, reads 1105, failed reads 168, no-leader 0, anomalies 0; final log 2544 entries (sentinel at 2543); VerifyFinal ok; porcupine Ok in 0.0s.
bulk: produced 29776, acked 29776 (present once 29776), failed 0 (present 0), rejected by the producer 0, lost 0, duplicates 0, phantoms 0, per-key order violations 0, offset mismatches 0, final records 29776.
No violations.

**Availability.** healthy after healing: true (4.1s after the last heal). lin appends acked 21.2/s, bulk 249.7/s. Windows of ≥ 2s without an acked lin append: 17.6–25.9, 45.3–55.1, 72.1–80.8, 96.6–105.3 (total 35.5s).

**Observations.**
- 2 of 4 rounds killed the lin leader

### kill9-controller: PASS

**Faults injected.** 3 rounds: after 10–20 s, SIGKILL the current controller (majority answer of Metadata.controller_id), start it again after 5–10 s.

Results file `20261018T231458Z-kill9-controller.json`, broker logs `20261018T231458Z-kill9-controller.log`, schedule seed 239863908227008166, workload 81.6s, total 88.3s.

**Invariants checked.**
- lin: no read integrity anomaly (gap, offset going backwards, CRC/decode failure, no progress below the HW)
- lin: VerifyFinal: every acked (value, offset) is at that offset of the settled final log, every read is a prefix of it, no phantom, no duplicate
- lin: porcupine check of the prepared history is Ok (Illegal = violation, Unknown = inconclusive = failure)
- bulk: every acked value present exactly once at its acked partition/offset; acked values of a key in sequence order; failed values at most once; nothing never produced
- availability (not safety): the cluster passes `streamqctl cluster status --strict` after healing

**Fault timeline** (seconds since the workloads started):

- 17.3s kill -9 broker2: round 1: controller; lin before: leader 3 epoch 0 isr [3 1 2] (from broker 1)
- 25.5s start broker2: lin while down: leader 3 epoch 0 isr [3 1 2] (from broker 1)
- 39.2s kill -9 broker1: round 2: controller; lin before: leader 3 epoch 0 isr [3 1 2] (from broker 1)
- 48.4s start broker1: lin while down: leader 3 epoch 0 isr [3 1 2] (from broker 2)
- 61.5s kill -9 broker3: round 3: controller; lin before: leader 3 epoch 0 isr [3 1 2] (from broker 1)
- 68.4s start broker3: lin while down: leader 3 epoch 0 isr [3 1 2] (from broker 1)

**Safety.** lin: acked 1673, unknown 183 (in final 20, pruned 163), definite 0, reads 738, failed reads 75, no-leader 0, anomalies 0; final log 1693 entries (sentinel at 1692); VerifyFinal ok; porcupine Ok in 0.0s.
bulk: produced 20395, acked 20395 (present once 20395), failed 0 (present 0), rejected by the producer 0, lost 0, duplicates 0, phantoms 0, per-key order violations 0, offset mismatches 0, final records 20395.
No violations.

**Availability.** healthy after healing: true (3.1s after the last heal). lin appends acked 20.5/s, bulk 249.9/s. Windows of ≥ 2s without an acked lin append: 17.1–26.2, 39.0–49.0, 61.3–68.7 (total 26.5s).

**Observations.**
- round 1: controller 2 killed; controller now 1
- round 2: controller 1 killed; controller now 3
- round 3: controller 3 killed; controller now 1

### inbound-isolation: PASS

**Faults injected.** 3 rounds: after 10–15 s, disable both toxiproxy proxies in front of the lin leader (brokerN_internal, brokerN_raft) for 15 s, then re-enable them. This is an inbound-only cut: peers cannot open connections to the leader's internal/Raft ports (existing ones are closed), but the leader's own outbound connections to its peers' proxies still work, and its client port stays reachable.

Results file `20261018T231458Z-inbound-isolation.json`, broker logs `20261018T231458Z-inbound-isolation.log`, schedule seed 16094261746073192690, workload 101.1s, total 112.6s.

**Invariants checked.**
- lin: no read integrity anomaly (gap, offset going backwards, CRC/decode failure, no progress below the HW)
- lin: VerifyFinal: every acked (value, offset) is at that offset of the settled final log, every read is a prefix of it, no phantom, no duplicate
- lin: porcupine check of the prepared history is Ok (Illegal = violation, Unknown = inconclusive = failure)
- bulk: every acked value present exactly once at its acked partition/offset; acked values of a key in sequence order; failed values at most once; nothing never produced
- availability (not safety): the cluster passes `streamqctl cluster status --strict` after healing

**Fault timeline** (seconds since the workloads started):

- 15.4s isolate inbound broker3: round 1: proxies [broker3_internal broker3_raft] disabled; controller 1; lin before: leader 3 epoch 0 isr [3 1 2] (from broker 1)
- 30.4s heal broker3: proxies re-enabled; lin at end of isolation: leader -1 epoch 1 isr [3] (from broker 1)
- 42.9s isolate inbound broker3: round 2: proxies [broker3_internal broker3_raft] disabled; controller 3; lin before: leader 3 epoch 2 isr [3 1 2] (from broker 1)
- 57.9s heal broker3: proxies re-enabled; lin at end of isolation: leader -1 epoch 3 isr [3] (from broker 1)
- 72.4s isolate inbound broker3: round 3: proxies [broker3_internal broker3_raft] disabled; controller 3; lin before: leader 3 epoch 4 isr [3 1 2] (from broker 1)
- 87.4s heal broker3: proxies re-enabled; lin at end of isolation: leader -1 epoch 5 isr [3] (from broker 1)

**Safety.** lin: acked 1390, unknown 56 (in final 44, pruned 12), definite 0, reads 601, failed reads 0, no-leader 761, anomalies 0; final log 1434 entries (sentinel at 1433); VerifyFinal ok; porcupine Ok in 0.0s.
bulk: produced 24719, acked 24719 (present once 24719), failed 0 (present 0), rejected by the producer 0, lost 0, duplicates 0, phantoms 0, per-key order violations 0, offset mismatches 0, final records 24719.
No violations.

**Availability.** healthy after healing: true (3.7s after the last heal). lin appends acked 13.7/s, bulk 244.4/s. Windows of ≥ 2s without an acked lin append: 15.4–36.8, 42.8–60.7, 72.4–88.8 (total 55.7s).

**Observations.**
- round 1: isolated lin leader 3 (epoch 0, controller 1); after 15 s: leader -1 epoch 1 isr [3]
- round 2: isolated lin leader 3 (epoch 2, controller 3); after 15 s: leader -1 epoch 3 isr [3]
- round 3: isolated lin leader 3 (epoch 4, controller 3); after 15 s: leader -1 epoch 5 isr [3]

### pause-leader: PASS

**Faults injected.** 2 rounds: after 10–15 s, docker compose pause the lin leader for 20 s (> 9 s broker session timeout), then unpause it (a GC-pause style zombie; its sockets stay open but nothing answers). A round in which the lin leader epoch did not change is 'fault not exercised'.

Results file `20261018T231458Z-pause-leader.json`, broker logs `20261018T231458Z-pause-leader.log`, schedule seed 16323141810293535992, workload 85.5s, total 91.9s.

**Invariants checked.**
- lin: no read integrity anomaly (gap, offset going backwards, CRC/decode failure, no progress below the HW)
- lin: VerifyFinal: every acked (value, offset) is at that offset of the settled final log, every read is a prefix of it, no phantom, no duplicate
- lin: porcupine check of the prepared history is Ok (Illegal = violation, Unknown = inconclusive = failure)
- bulk: every acked value present exactly once at its acked partition/offset; acked values of a key in sequence order; failed values at most once; nothing never produced
- availability (not safety): the cluster passes `streamqctl cluster status --strict` after healing
- exercised: the lin leader epoch changes during at least one pause (R28)

**Fault timeline** (seconds since the workloads started):

- 19.0s pause broker3: round 1: controller 3; lin before: leader 3 epoch 0 isr [3 1 2] (from broker 1)
- 40.1s unpause broker3: lin at end of pause: leader 1 epoch 1 isr [1 2] (from broker 1)
- 52.8s pause broker1: round 2: controller 1; lin before: leader 1 epoch 1 isr [3 1 2] (from broker 1)
- 74.0s unpause broker1: lin at end of pause: leader 3 epoch 2 isr [3 2] (from broker 2)

**Safety.** lin: acked 1346, unknown 13 (in final 1, pruned 12), definite 0, reads 600, failed reads 3, no-leader 0, anomalies 0; final log 1347 entries (sentinel at 1346); VerifyFinal ok; porcupine Ok in 0.0s.
bulk: produced 20136, acked 20136 (present once 20136), failed 0 (present 0), rejected by the producer 1, lost 0, duplicates 0, phantoms 0, per-key order violations 0, offset mismatches 0, final records 20136.
bulk producer errors: rejected: unknown topic or partition ×1.
No violations.

**Availability.** healthy after healing: true (1.6s after the last heal). lin appends acked 15.7/s, bulk 235.4/s. Windows of ≥ 2s without an acked lin append: 18.9–32.3, 52.8–65.2 (total 25.8s).

**Observations.**
- round 1: paused lin leader 3 (epoch 0, controller 3); leadership moved to 1 at epoch 1 during the pause
- round 2: paused lin leader 1 (epoch 1, controller 1); leadership moved to 3 at epoch 2 during the pause

### latency: PASS

**Faults injected.** after 10 s, on all 6 broker-to-broker proxies for 60 s: latency toxic (downstream, 200 ms ± 100 ms jitter) and slicer toxic (upstream: TCP data cut into ~1 KiB ± 512 B fragments with 50 µs delay between them, i.e. fragmentation plus delay); then all toxics removed.

Results file `20261018T231458Z-latency.json`, broker logs `20261018T231458Z-latency.log`, schedule seed 11005672247281837298, workload 85.0s, total 91.9s.

**Invariants checked.**
- lin: no read integrity anomaly (gap, offset going backwards, CRC/decode failure, no progress below the HW)
- lin: VerifyFinal: every acked (value, offset) is at that offset of the settled final log, every read is a prefix of it, no phantom, no duplicate
- lin: porcupine check of the prepared history is Ok (Illegal = violation, Unknown = inconclusive = failure)
- bulk: every acked value present exactly once at its acked partition/offset; acked values of a key in sequence order; failed values at most once; nothing never produced
- availability (not safety): the cluster passes `streamqctl cluster status --strict` after healing

**Fault timeline** (seconds since the workloads started):

- 15.0s add toxics all: latency downstream 200±100 ms; slicer upstream 1024±512 B, 50 µs
- 75.0s remove toxics all: toxiproxy reset

**Safety.** lin: acked 1240, unknown 0 (in final 0, pruned 0), definite 0, reads 474, failed reads 1, no-leader 0, anomalies 0; final log 1240 entries (sentinel at 1239); VerifyFinal ok; porcupine Ok in 0.0s.
bulk: produced 13951, acked 11839 (present once 11839), failed 2112 (present 0), rejected by the producer 0, lost 0, duplicates 0, phantoms 0, per-key order violations 0, offset mismatches 0, final records 11839.
bulk producer errors: request timed out ×2112.
No violations.

**Availability.** healthy after healing: true (0.0s after the last heal). lin appends acked 14.6/s, bulk 139.2/s. Windows of ≥ 2s without an acked lin append: none (total 0.0s).

**Observations.**
- latency +10s: lin leader 3 epoch 0 isr [3 1 2] (from broker 1); controller 2
- latency +20s: lin leader 3 epoch 0 isr [3 1 2] (from broker 1); controller 2
- latency +30s: lin leader 3 epoch 0 isr [3 1 2] (from broker 1); controller 2
- latency +40s: lin leader 3 epoch 0 isr [3 1 2] (from broker 1); controller 2
- latency +50s: lin leader 3 epoch 0 isr [3 1 2] (from broker 1); controller 2
- latency +60s: lin leader 3 epoch 0 isr [3 1 2] (from broker 1); controller 2

### full-disk: PASS

**Faults injected.** broker3's log dir is a 64 MiB tmpfs volume (Raft state on a separate volume). After 15 s the volume is filled with busybox dd until ENOSPC and held full for 30 s; healing replaces the disk: SIGKILL broker3, wait (bounded) until it is fenced and out of every ISR, remove and recreate the log volume, start broker3 empty.

Results file `20261018T231458Z-full-disk.json`, broker logs `20261018T231458Z-full-disk.log`, schedule seed 9680667983441191715, workload 76.1s, total 87.6s.

**Invariants checked.**
- lin: no read integrity anomaly (gap, offset going backwards, CRC/decode failure, no progress below the HW)
- lin: VerifyFinal: every acked (value, offset) is at that offset of the settled final log, every read is a prefix of it, no phantom, no duplicate
- lin: porcupine check of the prepared history is Ok (Illegal = violation, Unknown = inconclusive = failure)
- bulk: every acked value present exactly once at its acked partition/offset; acked values of a key in sequence order; failed values at most once; nothing never produced
- availability (not safety): the cluster passes `streamqctl cluster status --strict` after healing
- other brokers keep serving: partitions not led by broker3 acknowledge bulk records while the disk is full
- after the disk replacement broker3 catches up and rejoins every ISR (cluster status --strict)

**Fault timeline** (seconds since the workloads started):

- 20.9s fill disk broker3: dd until ENOSPC; 66584576 bytes (63.5MB) copied, 0.030737 seconds, 2.0GB/s; df: tmpfs                    65536     65536         0 100% /d
- 51.7s kill -9 broker3: disk replacement starts; container was running exit=0 oom=false
- 61.2s fenced broker3: broker3 not live and out of every ISR
- 62.4s replace disk broker3: container removed, log volume recreated empty, broker3 started

**Safety.** lin: acked 1078, unknown 941 (in final 0, pruned 941), definite 0, reads 808, failed reads 103, no-leader 0, anomalies 0; final log 1078 entries (sentinel at 1077); VerifyFinal ok; porcupine Ok in 0.0s.
bulk: produced 19028, acked 16129 (present once 16129), failed 2899 (present 0), rejected by the producer 0, lost 0, duplicates 0, phantoms 0, per-key order violations 0, offset mismatches 0, final records 16129.
bulk producer errors: unknown server error ×2899.
No violations.

**Availability.** healthy after healing: true (3.7s after the last heal). lin appends acked 14.2/s, bulk 211.9/s. Windows of ≥ 2s without an acked lin append: 20.5–61.0 (total 40.5s).

**Observations.**
- disk full +5s: broker3 container running exit=0 oom=false; partitions led by broker3: [bulk-1 bulk-4 lin-0]; lin leader 3 epoch 0 isr [3 1 2] (from broker 1)
- disk full +10s: broker3 container running exit=0 oom=false; partitions led by broker3: [bulk-1 bulk-4 lin-0]; lin leader 3 epoch 0 isr [3 1 2] (from broker 1)
- disk full +15s: broker3 container running exit=0 oom=false; partitions led by broker3: [bulk-1 bulk-4 lin-0]; lin leader 3 epoch 0 isr [3 1 2] (from broker 1)
- disk full +20s: broker3 container running exit=0 oom=false; partitions led by broker3: [bulk-1 bulk-4 lin-0]; lin leader 3 epoch 0 isr [3 1 2] (from broker 1)
- disk full +25s: broker3 container running exit=0 oom=false; partitions led by broker3: [bulk-1 bulk-4 lin-0]; lin leader 3 epoch 0 isr [3 1 2] (from broker 1)
- disk full +30s: broker3 container running exit=0 oom=false; partitions led by broker3: [bulk-1 bulk-4 lin-0]; lin leader 3 epoch 0 isr [3 1 2] (from broker 1)
- while full (20.9–51.4s): bulk acks on partitions not led by broker3 at fill time: 4857 (map[0:971 2:1296 3:1295 5:1295]); broker3 led [1 4]
- before healing, broker3 container: running exit=0 oom=false
- broker3 log before the replacement (20261018T231458Z-full-disk-broker3-before-replacement.log): 3002 WARN/ERROR lines, 22 mentioning ENOSPC; first: broker3-1  | 2026-10-18T23:24:32.395214619Z {"time":"2026-10-18T23:24:32.394616123Z","level":"ERROR","msg":"log failed; rejecting further writes","broker_id":3,"topic":"bulk","partition":3,"error":"write batch: write /var/lib/streamq/bulk-3/00000000000000000000.log: no space left on device"}
