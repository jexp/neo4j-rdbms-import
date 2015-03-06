package org.neo4j.imports;

import java.util.concurrent.ArrayBlockingQueue;

/**
* @author mh
* @since 01.03.15
*/
public class Queues<T> {
    private int rounds;
    private final int capacity;
    private ArrayBlockingQueue<T> current;
    private ArrayBlockingQueue<T> next;

    public Queues(int rounds, int capacity) {
        this.rounds = rounds;
        this.capacity = capacity;
        current = new ArrayBlockingQueue<T>(capacity);
    }

    public void put(T element) throws InterruptedException {
        current.put(element);
    }

    public T take() throws InterruptedException {
        T next = current.take();
        dump(false);
        if (this.next != null) {
            this.next.put(next);
        }
        return next;
    }

    int counter = 0;
    private void dump(boolean force) {
//        ++counter;
//        if (force || counter % 100_000 == 0) {
//            System.out.println("\nRound " + rounds
//                    + " current " + current.size() + " next " + (this.next == null ? "n.a" : this.next.size()));
//        }
    }

    public ArrayBlockingQueue<T> next() {
        dump(true);
        rounds--;
        if (rounds < 0) {
            throw new IllegalStateException("More queue usages than indicated");
        }
        if (next==null) {
            if (rounds > 0)
                next = new ArrayBlockingQueue<T>(capacity);
            return current;
        }
        if (!current.isEmpty()) throw new IllegalStateException("Queue not empty");
        current.clear();
        ArrayBlockingQueue<T> tmp=current;
        current = next;
        next = tmp;
        if (rounds == 0 && next != null) {
            next.clear();
            next = null;
        }
        return current;
    }
}
