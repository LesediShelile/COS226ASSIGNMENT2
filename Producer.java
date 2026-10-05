/** Generates a fixed number of uniquely identified jobs and enqueues them. */
public class Producer extends Thread {
    private final Queue<Job> queue;
    private final int producerId;
    private final int firstId;      // first job id this producer generates
    private final int jobsToProduce;
    private int produced = 0;

    public Producer(Queue<Job> queue, int producerId, int firstId, int jobsToProduce) {
        super("Producer-" + producerId);
        this.queue = queue;
        this.producerId = producerId;
        this.firstId = firstId;
        this.jobsToProduce = jobsToProduce;
    }

    @Override
    public void run() {
        for (int i = 0; i < jobsToProduce; i++) {
            // ids are unique because each producer owns a distinct id range
            queue.enq(new Job(firstId + i, producerId));
            produced++;
        }
    }

    public int getProduced() { return produced; }
}