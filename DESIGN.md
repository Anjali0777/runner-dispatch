# Design: Runner Dispatch

## Problem

A hosted CI service has a queue of jobs and a fleet of runners. Each job names the labels it needs (`runs-on: [self-hosted, linux, gpu]`) and may only run on a runner that has all of them. Each organisation has a concurrency cap. When a runner reports idle, the dispatcher must pick a job for it.

What makes this hard is the feedback loop during an incident. When something upstream breaks (on 5 Oct 2026 for GitHub, a network fault between a datacenter and cloud databases), jobs pile up. When it recovers, every runner asks for work at once. If the cost of picking a job grows with the queue, recovery itself gets slower the longer the outage lasted. This project is about keeping that cost flat.

## Constraints

- **Correctness:** a job only runs on a runner that covers its labels, an org never exceeds its cap, and no job is assigned twice or lost.
- **Work-conserving:** if any queued job can run on an idle runner, that runner gets one. An idle runner next to an eligible job is wasted capacity.
- **Fairness:** one org's burst (a monorepo pushing thousands of jobs) shouldn't push everyone else to the back.
- **Latency:** `Assign` sits on the hot path for every runner. Target sub-microsecond at 100k queued.
- **Workload shape:** a few dozen distinct label sets, thousands of orgs with a long-tailed share of traffic, queue depth from hundreds to hundreds of thousands.

## Approaches considered

### 1. Single FIFO, scan from the front (implemented: `Naive`)

On each idle runner, walk the queue and take the first job whose labels are covered and whose org is under its cap.

- Simple and obviously correct, and strict FIFO across everyone.
- `Assign` is O(Q). Worse, it re-reads the same blocked jobs every call. If the front holds 50k jobs from an org at its cap, every runner walks past all 50k, and a runner that finds nothing (the common case during recovery) always scans the entire queue.
- This is the failure mode: cost grows with backlog, and backlog grows when dispatch is slow.

### 2. Priority queue keyed on enqueue time, with skip-and-requeue

A heap where blocked jobs are popped, set aside and pushed back.

- Gets the oldest eligible job, but blocked jobs are still touched on every call, now at O(log Q) each.
- Doesn't fix the core issue: work is proportional to the number of *ineligible* jobs.
- Rejected.

### 3. Index by label set, then by org, with eligibility rings (implemented: `Indexed`)

Group jobs into **pools** by their exact label set, and inside each pool, one FIFO per org. Each pool keeps a circular linked list (**ring**) containing only the org queues that are eligible right now: non-empty, with the org under its cap.

- When an org hits its cap, it is **parked**: unlinked from every ring it's in. When one of its jobs completes, it is unlinked no more. Blocked jobs are never visited.
- A runner's compatible pools are computed once per distinct runner label set and cached. The cache is cleared when a new label set appears, which is rare.
- `Assign` looks at the head of each compatible pool's ring (k pools, typically 1–4), picks the oldest, serves that org, and moves the ring cursor on. That's round-robin across orgs within a pool.

### 4. Per-runner-type queues (route on enqueue)

Push each job onto the queue of every runner type that could take it.

- Fast `Assign`, but jobs appear in several queues. That needs removal from the others (or tombstones) and breaks down when runners have overlapping, open-ended label sets, as self-hosted runners do.
- Rejected in favour of 3. Approach 3 indexes by what jobs *need*, which is a small, stable set, rather than by what runners *have*.

## Decision

Approach 3. Measured on the same synthetic workload (`results/bench.md`):

- At 100k queued, mean `Assign` falls from about 0.8–0.9 ms to 0.3–0.5 µs. That is roughly 1,500× faster with a normal mix and 2,800× with a capped burst at the front.
- Draining a 200k backlog with 2,000 runners takes 0.56 s of dispatcher time. The baseline cleared only 20% in 60 s.

## Complexity

| | Naive | Indexed |
|---|---|---|
| `Enqueue` | O(1) amortised | O(1) amortised (O(P) once per new label set, to clear the cache) |
| `Assign` | O(Q) scan + O(Q) delete | O(k) + O(p) when the org hits its cap |
| `Complete` | O(1) | O(1) + O(p) when it unparks the org |
| Space | O(Q) | O(Q + orgs × label sets in use) |

Here Q is queued jobs, P is distinct label sets, k is the pools a runner covers, and p is the pools one org uses. k and p are small and do not grow with the backlog, which is the whole point.

## Tradeoffs accepted

- **Round-robin is per org, not per job.** A small org's job no longer waits behind a burst. In the benchmark, median wait for long-tail orgs is 11–52% lower. The cost is that the heaviest orgs wait longer at the tail: p99 wait for the top 10 orgs is 4–31% higher at 10k–100k depth, and about 2× at depth 1,000. That is the intended trade, but it is a trade. A production version would use weighted fair queueing, with weights by plan tier, rather than equal shares.
- **Oldest-across-pools is approximate.** When a runner covers several pools, it compares only each pool's next round-robin candidate, not every job, so global FIFO order is not guaranteed. The other properties (correctness, work conservation, no starvation) still hold.
- **At most 64 distinct labels per interner.** One `uint64` per label set makes subset tests a single AND. More labels would need a `[]uint64` bitset, which adds a constant factor but not a different complexity.
- **One mutex.** `Locked` serialises calls. Critical sections are about 50–500 ns, so a single dispatcher can handle a few hundred thousand assignments per second on this 2-vCPU machine. I didn't add lock-free structures because the measured bottleneck was the algorithm, not contention.

## Failure modes and handling

| Failure | Handling |
|---|---|
| Unknown job ID passed to `Complete` (duplicate or late callback) | Ignored; covered by a test |
| Runner reports labels no job needs | Compatible-pool list is empty; returns "nothing eligible" in O(1) |
| An org's cap is lowered while it has running jobs | Parking checks `running >= cap`, so it simply stays parked until it drops below |
| Label explosion (> 64) | `Interner.Mask` returns an error instead of silently aliasing bits |
| Dispatcher crash | State is in memory only. A real deployment would rebuild from a durable job store on startup; replaying 200k jobs into `Indexed` takes milliseconds |

## At 10× and 100× scale

- **10× (about 1M queued, 50k runners):** the algorithm holds, since cost doesn't depend on Q. The single lock becomes the limit before the data structure does. Next step: shard by label-set pool, with one dispatcher per runner family, because pools rarely overlap. A cross-pool runner type would query its few shards.
- **100×:** the dispatcher becomes a distributed service. Each shard owns a set of pools, backed by a durable log so it can fail over. Org caps become a shared counter, using leases or tokens handed out per shard, so a shard can admit jobs without a network round trip per assignment. Runner long-polls are spread across shards with consistent hashing on label set.
- **Fairness at scale:** move from round-robin to deficit round-robin with per-tier weights, and add aging so that no job waits beyond a bound, whatever its org's weight.
