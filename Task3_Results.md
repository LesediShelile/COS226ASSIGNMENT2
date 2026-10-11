# Task 3 – Experimental Comparison

**Setup:** 12,000 total jobs for every run; BoundedQueue capacity = 100 (same for all runs); work split evenly across producers.
Each configuration: 3 warm-up runs + 10 measured runs, **median** reported. Every run verified (all jobs processed exactly once, 0 missing, 0 duplicates).
Run with: `javac *.java && java Experiment` (machine used: 2 cores, Java 21).

| Queue | Producers | Consumers | Workload | Jobs/producer | Median time (ms) | Min–Max (ms) | Throughput (jobs/s) |
|---|---|---|---|---|---|---|---|
| bounded | 4 | 4 | balanced | 3000 | 16.57 | 11.79–22.63 | 724,204 |
| lockfree | 4 | 4 | balanced | 3000 | 7.07 | 4.28–15.73 | 1,697,876 |
| bounded | 6 | 2 | producer-heavy | 2000 | 8.48 | 4.94–18.44 | 1,415,503 |
| lockfree | 6 | 2 | producer-heavy | 2000 | 2.89 | 2.52–3.61 | 4,153,625 |
| bounded | 2 | 6 | consumer-heavy | 6000 | 6.03 | 4.89–11.79 | 1,988,800 |
| lockfree | 2 | 6 | consumer-heavy | 6000 | 5.82 | 4.49–7.85 | 2,060,209 |

## Observations

1. **Lock-free was faster in every configuration**, but the gap depends heavily on the workload. It was ~2.3x faster when balanced, ~2.9x faster when producer-heavy, and essentially level (~1.03x) when consumer-heavy.
2. **Producer-heavy (6P/2C):** the bounded queue is capped at 100 items, so the six producers keep filling it and block on `notFull`, then need the signal path (take `deqLock`, `signalAll`) to resume. The lock-free queue is unbounded, so producers never wait and only retry a failed CAS on `tail`; this is where lock-free pulls furthest ahead (and it also has the tightest min–max spread).
3. **Consumer-heavy (2P/6C):** only two producers contend on the enqueue side, so there is little CAS or lock contention for either queue. The bottleneck is how fast the two producers can supply jobs, so both queues finish at nearly the same time. Here the extra consumers mostly wait: on `notEmpty` for the bounded queue, or spin with `Thread.yield()` on a `null` `deq()` for the lock-free queue.
4. **Balanced (4P/4C):** the bounded queue has the most blocking/wake-up traffic here (producers and consumers alternately hitting full and empty), which explains its worst throughput of the three configurations. Lock-free avoids parking threads altogether.
5. **Why bounded is not "just slower":** its two-lock design lets one enqueuer and one dequeuer run in parallel, and blocking threads yield the CPU instead of spinning. Lock-free consumers busy-wait when the queue is empty, which wastes CPU even where throughput looks fine (visible in the consumer-heavy case). Lock-free also has no back-pressure, so a fast producer set could grow memory without limit.

**Caveats:** the test machine had only 2 cores and runs last just a few milliseconds, so there is noticeable run-to-run variance (see Min–Max). Treat the ratios as indicative; re-running `Experiment` on the lab machine will give different absolute numbers, but the trends above (lock-free ahead on producer-heavy and balanced, near-parity on consumer-heavy) are the part to discuss in the demo.
