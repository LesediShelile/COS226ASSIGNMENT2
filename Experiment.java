import java.io.PrintWriter;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;

/**
 * Task 3 - Experimental comparison of BoundedQueue and LockFreeQueue.
 *
 * Usage: java Experiment [totalJobs] [capacity] [runs]
 * Defaults: 12000 jobs, capacity 100, 10 measured runs (+3 warm-up runs per config).
 *
 * Every configuration uses the SAME total workload and the SAME bounded capacity.
 * Each configuration is run several times; the median time is reported (plus min/max)
 * to dampen JIT / scheduler noise. Every run is verified (no missing/duplicate jobs).
 * Output: table on stdout, results.csv and results.md in the working directory.
 */
public class Experiment {

    static final int[][] CONFIGS = { {4, 4}, {6, 2}, {2, 6} };
    static final String[] LABELS = { "balanced", "producer-heavy", "consumer-heavy" };
    static final String[] QUEUES = { "bounded", "lockfree" };

    /** One verified run; returns elapsed nanoseconds, or -1 if verification failed. */
    static long runOnce(String type, int np, int nc, int totalJobs, int capacity)
            throws InterruptedException {
        Queue<Job> queue = type.equals("bounded")
                ? new BoundedQueue<Job>(capacity) : new LockFreeQueue<Job>();

        AtomicInteger remaining = new AtomicInteger(totalJobs);
        AtomicInteger processed = new AtomicInteger(0);
        AtomicIntegerArray seen = new AtomicIntegerArray(totalJobs);

        Producer[] producers = new Producer[np];
        int base = totalJobs / np, extra = totalJobs % np, nextId = 0;
        for (int i = 0; i < np; i++) {
            int count = base + (i < extra ? 1 : 0);
            producers[i] = new Producer(queue, i, nextId, count);
            nextId += count;
        }
        Consumer[] consumers = new Consumer[nc];
        for (int i = 0; i < nc; i++)
            consumers[i] = new Consumer(queue, i, remaining, processed, seen);

        long start = System.nanoTime();
        for (Consumer c : consumers) c.start();
        for (Producer p : producers) p.start();
        for (Producer p : producers) p.join();
        for (Consumer c : consumers) c.join();
        long elapsed = System.nanoTime() - start;

        int produced = 0, bad = 0;
        for (Producer p : producers) produced += p.getProduced();
        for (int i = 0; i < totalJobs; i++) if (seen.get(i) != 1) bad++;
        boolean ok = produced == totalJobs && processed.get() == totalJobs && bad == 0;
        return ok ? elapsed : -1;
    }

    public static void main(String[] args) throws Exception {
        int totalJobs = args.length > 0 ? Integer.parseInt(args[0]) : 12000;
        int capacity  = args.length > 1 ? Integer.parseInt(args[1]) : 100;
        int runs      = args.length > 2 ? Integer.parseInt(args[2]) : 10;
        int warmup    = 3;

        System.out.printf("Total jobs: %d | Bounded capacity: %d | Runs/config: %d (+%d warm-up)%n%n",
                totalJobs, capacity, runs, warmup);

        // global JIT warm-up on both queues
        for (String q : QUEUES) for (int[] c : CONFIGS) runOnce(q, c[0], c[1], totalJobs, capacity);

        StringBuilder csv = new StringBuilder(
                "queue,producers,consumers,workload,jobs_per_producer,median_ms,min_ms,max_ms,throughput_jobs_per_sec\n");
        StringBuilder md = new StringBuilder();
        md.append("| Queue | Producers | Consumers | Workload | Jobs/producer | Median time (ms) | Min–Max (ms) | Throughput (jobs/s) |\n");
        md.append("|---|---|---|---|---|---|---|---|\n");

        String hdr = String.format("%-9s %-5s %-5s %-15s %-10s %-12s %-18s %-14s",
                "Queue", "Prod", "Cons", "Workload", "Jobs/prod", "Median (ms)", "Min-Max (ms)", "Throughput/s");
        System.out.println(hdr);
        StringBuilder line = new StringBuilder();
        for (int i = 0; i < hdr.length(); i++) line.append('-');
        System.out.println(line);

        for (int ci = 0; ci < CONFIGS.length; ci++) {
            for (String q : QUEUES) {
                int np = CONFIGS[ci][0], nc = CONFIGS[ci][1];
                for (int w = 0; w < warmup; w++) runOnce(q, np, nc, totalJobs, capacity);
                double[] ms = new double[runs];
                for (int r = 0; r < runs; r++) {
                    long t = runOnce(q, np, nc, totalJobs, capacity);
                    if (t < 0) { System.err.println("VERIFICATION FAILED for " + q + " " + np + "P/" + nc + "C"); System.exit(1); }
                    ms[r] = t / 1e6;
                }
                Arrays.sort(ms);
                double median = (ms[(runs - 1) / 2] + ms[runs / 2]) / 2.0;
                double thr = totalJobs / (median / 1000.0);
                String perProd = (totalJobs % np == 0) ? String.valueOf(totalJobs / np)
                        : (totalJobs / np) + "-" + (totalJobs / np + 1);
                System.out.printf("%-9s %-5d %-5d %-15s %-10s %-12.2f %-18s %-14.0f%n",
                        q, np, nc, LABELS[ci], perProd, median,
                        String.format("%.2f-%.2f", ms[0], ms[runs - 1]), thr);
                csv.append(String.format("%s,%d,%d,%s,%s,%.3f,%.3f,%.3f,%.0f%n",
                        q, np, nc, LABELS[ci], perProd, median, ms[0], ms[runs - 1], thr));
                md.append(String.format("| %s | %d | %d | %s | %s | %.2f | %.2f–%.2f | %.0f |%n",
                        q, np, nc, LABELS[ci], perProd, median, ms[0], ms[runs - 1], thr));
            }
        }
        try (PrintWriter pw = new PrintWriter("results.csv")) { pw.print(csv); }
        try (PrintWriter pw = new PrintWriter("results.md"))  { pw.print(md); }
        System.out.println("\nAll runs verified (every job processed exactly once). Wrote results.csv and results.md");
    }
}
