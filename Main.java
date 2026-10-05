import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;

/**
 * Usage: java Main <bounded|lockfree> [producers] [consumers] [totalJobs] [capacity]
 * Defaults: bounded 4 4 12000 100
 *
 * Assumes (from Task 1):
 *   BoundedQueue<T>(int capacity)  implements Queue<T>
 *   LockFreeQueue<T>()             implements Queue<T>
 */
public class Main {
    public static void main(String[] args) throws InterruptedException {
        String type      = args.length > 0 ? args[0].toLowerCase() : "bounded";
        int numProducers = args.length > 1 ? Integer.parseInt(args[1]) : 4;
        int numConsumers = args.length > 2 ? Integer.parseInt(args[2]) : 4;
        int totalJobs    = args.length > 3 ? Integer.parseInt(args[3]) : 12000;
        int capacity     = args.length > 4 ? Integer.parseInt(args[4]) : 100;

        Queue<Job> queue;
        switch (type) {
            case "bounded":  queue = new BoundedQueue<Job>(capacity); break;
            case "lockfree": queue = new LockFreeQueue<Job>();        break;
            default:
                System.err.println("Queue type must be 'bounded' or 'lockfree'");
                return;
        }

        AtomicInteger remaining = new AtomicInteger(totalJobs);
        AtomicInteger processed = new AtomicInteger(0);
        AtomicIntegerArray seen = new AtomicIntegerArray(totalJobs);

        // Split the workload between producers (spread any remainder).
        Producer[] producers = new Producer[numProducers];
        int base = totalJobs / numProducers, extra = totalJobs % numProducers, nextId = 0;
        for (int i = 0; i < numProducers; i++) {
            int count = base + (i < extra ? 1 : 0);
            producers[i] = new Producer(queue, i, nextId, count);
            nextId += count;
        }
        Consumer[] consumers = new Consumer[numConsumers];
        for (int i = 0; i < numConsumers; i++)
            consumers[i] = new Consumer(queue, i, remaining, processed, seen);

        System.out.printf("Queue: %s | Producers: %d | Consumers: %d | Total jobs: %d%n",
                type, numProducers, numConsumers, totalJobs);

        long start = System.nanoTime();
        for (Consumer c : consumers) c.start();
        for (Producer p : producers) p.start();
        for (Producer p : producers) p.join();
        for (Consumer c : consumers) c.join();
        long elapsed = System.nanoTime() - start;

        // ---- Verification ----
        int totalProduced = 0, totalProcessedPerConsumer = 0;
        for (Producer p : producers) totalProduced += p.getProduced();
        for (Consumer c : consumers) totalProcessedPerConsumer += c.getProcessedByMe();
        int missing = 0, duplicates = 0;
        for (int i = 0; i < totalJobs; i++) {
            int n = seen.get(i);
            if (n == 0) missing++;
            else if (n > 1) duplicates++;
        }

        double seconds = elapsed / 1e9;
        System.out.println("---------------- Results ----------------");
        for (Producer p : producers)
            System.out.printf("%s produced %d jobs%n", p.getName(), p.getProduced());
        for (Consumer c : consumers)
            System.out.printf("%s processed %d jobs%n", c.getName(), c.getProcessedByMe());
        System.out.println("Total produced : " + totalProduced);
        System.out.println("Total processed: " + processed.get()
                + " (sum over consumers: " + totalProcessedPerConsumer + ")");
        System.out.println("Missing jobs   : " + missing);
        System.out.println("Duplicate jobs : " + duplicates);
        System.out.printf("Execution time : %.3f ms%n", elapsed / 1e6);
        System.out.printf("Throughput     : %.0f jobs/sec%n", totalJobs / seconds);
        boolean ok = totalProduced == totalJobs && processed.get() == totalProduced
                && missing == 0 && duplicates == 0;
        System.out.println(ok ? "CORRECT: all produced jobs were processed exactly once."
                              : "ERROR: produced/processed mismatch!");
    }
}