# Runner Dispatch

> A CI job scheduler in Go that matches queued jobs to idle runners by label, under per-org limits, without slowing down as the backlog grows.

## What happened

On **5 October 2026**, GitHub Actions had trouble assigning hosted runners to jobs for about four hours (18:48–22:49 UTC). According to [GitHub's incident report](https://www.githubstatus.com/incidents/3q1yb5m7ltvb):

- During the roughly 90-minute peak, **71.9% of hosted jobs didn't start within five minutes**, and 47% of affected workflow runs failed.
- The root cause was a partial network failure between one on-premises datacenter region and cloud database and storage services.
- Recovery meant shifting traffic, adding compute capacity, and letting the queued jobs clear.

[UptimeRobot's timeline](https://uptimerobot.com/is-it-down/github/2026-10-05/) shows the same arc: delays from 19:11 UTC, mitigations at 21:32, and "queued jobs are clearing" by 21:54.

I don't know how GitHub's scheduler works internally. This project isn't a reconstruction of it. It's about the problem every CI scheduler faces in that last phase: thousands of runners asking for work at once while a large backlog sits in the queue.

## The engineering problem

When a runner goes idle, pick a job it can run:

- The runner must have **every label the job asks for**.
- The job's org must be **under its concurrency cap**.
- **No job runs twice and none are lost.**
- If any job could run there, the runner **must get one** (work-conserving).
- One org's burst **shouldn't starve everyone else**.

The trap is the obvious solution: scan the queue from the front. It's fine at 1,000 jobs. But during recovery the front of the queue is full of jobs that can't run yet, such as an org at its cap or a label set no free runner has. Every idle runner walks past all of them again, so the scheduler gets slower exactly when it is furthest behind.

## Architecture

```
                 Enqueue(job)                         Assign(runner labels)
                      │                                        │
                      ▼                                        ▼
  labels ──► Interner (string → bit) ──► Mask        compat cache: runner mask → [pools]
                      │                                        │
                      ▼                                        ▼
        ┌─────────── pool per exact label set ─────────────────────────┐
        │  ubuntu-latest           ubuntu-latest+gpu      windows ...   │
        │  ┌─ ring of ELIGIBLE orgs (non-empty, under cap) ─┐           │
        │  │  org-7 ⇄ org-12 ⇄ org-3 ⇄ ...  (cursor = next)  │           │
        │  └────────────────────────────────────────────────┘           │
        │  each org: FIFO of its jobs     parked orgs: off every ring   │
        └───────────────────────────────────────────────────────────────┘
                      ▲                                        │
                      └──── Complete(job): org below cap ──────┘
                            → relink ("unpark") its queues
```

- `dispatch/naive.go` is the baseline: one FIFO, scanned from the front.
- `dispatch/indexed.go` is the improved design. Jobs are grouped by label set, then by org. Each label-set pool keeps a ring of only the orgs that can run right now. An org that hits its cap is unlinked from every ring until a job of its finishes. Runners look up their compatible pools from a cache and take the oldest head among them, round-robin across orgs within a pool.
- `dispatch/locked.go` is a mutex wrapper for concurrent use.
- `workload/` is a synthetic hosted-CI traffic generator: 2,000 orgs with a Zipf share of jobs, a realistic `runs-on` mix, and an optional bursting org.
- `cmd/bench/` is the benchmark harness.

Tradeoffs, failure modes and the plan at 10× and 100× scale are in **[DESIGN.md](DESIGN.md)**.

## Run it

```bash
go test -race ./...               # unit, property, concurrency and example tests
go test -bench . -run x ./dispatch  # micro-benchmark: 100k blocked jobs
go run ./cmd/bench -quick         # quick comparison (~5 s)
go run ./cmd/bench -out results/bench.md   # full run (~3 min)
```

The only requirement is Go 1.24 or newer; there are no third-party dependencies.

## Results

These are from the full run on a 2-vCPU cloud container with Go 1.24.7 (`results/bench.md`). Absolute numbers will differ on your machine, but the ratios hold.

**Assign latency with the queue held at a fixed depth.** In the noisy scenario, half the traffic comes from one org bursting while at its cap.

| scenario | depth | naive mean | indexed mean | naive p99 | indexed p99 | speed-up (mean) |
|---|---:|---:|---:|---:|---:|---:|
| steady | 1,000 | 14.3 µs | 86 ns | 46.2 µs | 269 ns | 166× |
| steady | 10,000 | 116.7 µs | 178 ns | 329.6 µs | 638 ns | 656× |
| steady | 100,000 | 765.1 µs | 495 ns | 1.38 ms | 826 ns | 1,546× |
| noisy | 1,000 | 16.2 µs | 53 ns | 55.0 µs | 160 ns | 306× |
| noisy | 10,000 | 157.4 µs | 113 ns | 390.2 µs | 518 ns | 1,393× |
| noisy | 100,000 | 934.6 µs | 329 ns | 2.26 ms | 598 ns | 2,841× |

Naive latency grows linearly with depth. Indexed stays under a microsecond.

**Recovery drain.** This measures dispatcher time to clear a backlog with 2,000 runners and no new arrivals, with 40% of the backlog from a capped org:

| backlog | naive | indexed |
|---:|---:|---:|
| 10,000 | 12.3 s | 23.7 ms |
| 50,000 | > 60 s, stopped at 53% drained | 132 ms |
| 200,000 | > 60 s, stopped at 20% drained | 556 ms |

**Worst case (micro-benchmark).** With 100,000 blocked jobs ahead of one small org's job, naive takes **2.04 ms** per assignment and indexed takes **161 ns**.

**Fairness (an honest tradeoff).** Wait is measured in scheduling steps:

| scenario | depth | median wait, long-tail orgs (naive → indexed) | p99 wait, top-10 orgs (naive → indexed) |
|---|---:|---:|---:|
| steady | 1,000 | 650 → 312 | 1,101 → 2,171 |
| steady | 100,000 | 90,718 → 71,124 | 114,348 → 119,441 |
| noisy | 1,000 | 749 → 369 | 1,077 → 1,409 |
| noisy | 10,000 | 5,448 → 4,862 | 10,100 → 12,894 |

Round-robin across orgs cuts small orgs' typical wait. The heaviest orgs pay for it at the tail. DESIGN.md covers why, and how weighted fair queueing would tune it.

## How it's tested

- **Property test:** 300 random seeds × 400 operations each, checked after every step against a brute-force model:
  - every assigned job fits its runner and its org was under cap
  - no job is assigned twice or lost
  - `Assign` returns "nothing" only when nothing is eligible
- **Mutation check:** loosening the cap comparison by one makes three tests fail.
- **Concurrency:** 4 producers and 8 workers go through `Locked` under `-race`; every job is assigned exactly once.
- **Targeted tests:** label coverage, cap release, per-org FIFO, round-robin fairness, parking across pools, interner limits, and a runnable example.

## Limitations

- In-memory only. A real scheduler would rebuild from a durable job store on restart.
- Synthetic traffic. The label mix and Zipf skew are my estimates, not GitHub data.
- Waits at 100k depth are dominated by the initial backlog, because the run is shorter than the queue.
- At most 64 distinct labels per interner.

## Next steps

- Weighted deficit round-robin by plan tier, plus aging to bound worst-case wait.
- Shard by label-set pool behind one API, and benchmark contention with `Locked` versus per-pool locks.
- A long-poll HTTP front end with runners as goroutines, to measure end-to-end time-to-start during a simulated outage.

---
Built with AI pair-programming (Claude); I reviewed the design, tests and results. MIT licensed.
