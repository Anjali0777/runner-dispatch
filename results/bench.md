
### steady (Assign latency, queue held at depth)

| impl | depth | p50 | p99 | mean | assigns/s | median wait, long-tail orgs | p99 wait, top-10 orgs |
|---|---:|---:|---:|---:|---:|---:|---:|
| naive | 1000 | 15.8 µs | 46.2 µs | 14.3 µs | 68963 | 650 | 1101 |
| indexed | 1000 | 49 ns | 269 ns | 86 ns | 3410332 | 312 | 2171 |
| naive | 10000 | 93.9 µs | 329.6 µs | 116.7 µs | 8504 | 5642 | 14802 |
| indexed | 10000 | 43 ns | 638 ns | 178 ns | 1536622 | 5403 | 16924 |
| naive | 100000 | 781.4 µs | 1.38 ms | 765.1 µs | 1299 | 90718 | 114348 |
| indexed | 100000 | 398 ns | 826 ns | 495 ns | 519177 | 71124 | 119441 |

### noisy (Assign latency, queue held at depth)

| impl | depth | p50 | p99 | mean | assigns/s | median wait, long-tail orgs | p99 wait, top-10 orgs |
|---|---:|---:|---:|---:|---:|---:|---:|
| naive | 1000 | 16.9 µs | 55.0 µs | 16.2 µs | 61160 | 749 | 1077 |
| indexed | 1000 | 42 ns | 160 ns | 53 ns | 5622088 | 369 | 1409 |
| naive | 10000 | 191.8 µs | 390.2 µs | 157.4 µs | 6336 | 5448 | 10100 |
| indexed | 10000 | 42 ns | 518 ns | 113 ns | 2686334 | 4862 | 12894 |
| naive | 100000 | 852.1 µs | 2.26 ms | 934.6 µs | 1065 | 73518 | 113770 |
| indexed | 100000 | 258 ns | 598 ns | 329 ns | 722638 | 61085 | 118243 |

### drain (dispatcher time to clear a backlog, 2000 runners, 40% noisy)

| impl | backlog | dispatcher time | drained |
|---|---:|---:|---:|
| naive | 10000 | 12295.47 ms | 100% |
| indexed | 10000 | 23.68 ms | 100% |
| naive | 50000 | > 60484.95 ms (stopped) | 53% |
| indexed | 50000 | 132.17 ms | 100% |
| naive | 200000 | > 60040.76 ms (stopped) | 20% |
| indexed | 200000 | 556.37 ms | 100% |
