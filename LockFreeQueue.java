import java.util.concurrent.atomic.AtomicReference;

public class LockFreeQueue<T> implements Queue<T> {

    private class Node {

        private T value;
        private AtomicReference<Node> next;

        public Node(T value) {
            this.value = value;
            this.next = new AtomicReference<Node>(null);
        }
    }

    private AtomicReference<Node> head;
    private AtomicReference<Node> tail;

    public LockFreeQueue() {

        Node sentinel = new Node(null);

        head = new AtomicReference<Node>(sentinel);
        tail = new AtomicReference<Node>(sentinel);
    }

    @Override
    public void enq(T item) {

        Node newNode = new Node(item);

        while (true) {

            Node last = tail.get();
            Node next = last.next.get();

            if (last == tail.get()) {

                if (next == null) {

                    if (last.next.compareAndSet(null, newNode)) {

                        tail.compareAndSet(last, newNode);

                        return;
                    }

                } else {

                    tail.compareAndSet(last, next);
                }
            }
        }
    }

    @Override
    public T deq() {

        while (true) {

            Node first = head.get();
            Node last = tail.get();
            Node next = first.next.get();

            if (first == head.get()) {

                if (first == last) {

                    if (next == null) {
                        return null;
                    }

                    tail.compareAndSet(last, next);

                } else {

                    T value = next.value;

                    if (head.compareAndSet(first, next)) {
                        return value;
                    }
                }
            }
        }
    }
}