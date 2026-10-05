import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;

// Removes jobs from the shared queue and records them as processed.

public class Consumer extends Thread {
    private final Queue<Job> queue;
    private final AtomicInteger remaining;        // jobs not yet claimed
    private final AtomicInteger processedCount;   // total processed (shared)
    private final AtomicIntegerArray seen;        // seen[id] = times id was processed
    private int processedByMe = 0;

    public Consumer(Queue<Job> queue, int consumerId, AtomicInteger remaining,
                    AtomicInteger processedCount, AtomicIntegerArray seen) {
        super("Consumer-" + consumerId);
        this.queue = queue;
        this.remaining = remaining;
        this.processedCount = processedCount;
        this.seen = seen;
    }

    @Override
    public void run() {
        while (remaining.getAndDecrement() > 0) {
            Job job = queue.deq();
            while (job == null) {          // lock-free queue was empty: retry
                Thread.yield();
                job = queue.deq();
            }
            process(job);
        }
    }

    /** "Processing" = record that the job was successfully removed. */
    private void process(Job job) {
        seen.incrementAndGet(job.getId());
        processedCount.incrementAndGet();
        processedByMe++;
    }

    public int getProcessedByMe() { return processedByMe; }
}