/** A single unit of work passed from a producer to a consumer. */
public class Job {
    private final int id;          // globally unique identifier
    private final int producerId;  // which producer created it

    public Job(int id, int producerId) {
        this.id = id;
        this.producerId = producerId;
    }

    public int getId() { return id; }
    public int getProducerId() { return producerId; }

    @Override
    public String toString() {
        return "Job[" + id + " from producer " + producerId + "]";
    }
}