import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class BoundedQueue<T> implements Queue<T> {

    private final int capacity;

    private final ReentrantLock enqLock;
    private final ReentrantLock deqLock;

    private final Condition notFull;
    private final Condition notEmpty;

    private final AtomicInteger size;

    private Node head;
    private Node tail;

    public BoundedQueue(int capacity) {

        this.capacity = capacity;

        head = new Node(null);
        tail = head;

        size = new AtomicInteger(0);

        enqLock = new ReentrantLock();
        deqLock = new ReentrantLock();

        notFull = enqLock.newCondition();
        notEmpty = deqLock.newCondition();
    }

    @Override
    public void enq(T item) {

        boolean wakeConsumers = false;

        enqLock.lock();

        try {

            while (size.get() == capacity) {
                notFull.await();
            }

            Node newNode = new Node(item);

            tail.next = newNode;
            tail = newNode;

            if (size.getAndIncrement() == 0) {
                wakeConsumers = true;
            }

        } catch (InterruptedException e) {

            Thread.currentThread().interrupt();

        } finally {

            enqLock.unlock();
        }

        if (wakeConsumers) {

            deqLock.lock();

            try {
                notEmpty.signalAll();
            } finally {
                deqLock.unlock();
            }
        }
    }

    @Override
    public T deq() {

        T result;
        boolean wakeProducers = false;

        deqLock.lock();

        try {

            while (size.get() == 0) {
                notEmpty.await();
            }

            result = head.next.value;
            head = head.next;

            if (size.getAndDecrement() == capacity) {
                wakeProducers = true;
            }

        } catch (InterruptedException e) {

            Thread.currentThread().interrupt();
            return null;

        } finally {

            deqLock.unlock();
        }

        if (wakeProducers) {

            enqLock.lock();

            try {
                notFull.signalAll();
            } finally {
                enqLock.unlock();
            }
        }

        return result;
    }

    private class Node {

        private T value;
        private Node next;

        public Node(T value) {
            this.value = value;
            this.next = null;
        }
    }
}