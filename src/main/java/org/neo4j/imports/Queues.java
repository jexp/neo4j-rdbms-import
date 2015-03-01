package org.neo4j.imports;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

/**
* @author mh
* @since 01.03.15
*/
public class Queues<T> {
    private final ArrayBlockingQueue<T>[] queues;

    public Queues(int count, int capacity) {
        queues = new ArrayBlockingQueue[count];
        for (int i = 0; i < count; i++) {
            queues[i] = new ArrayBlockingQueue<T>(capacity);
        }
    }

    public void put(T element) throws InterruptedException {
        for (ArrayBlockingQueue<T> queue : queues) {
            queue.put(element);
        }
    }

    public BlockingQueue<T> queue(int i) {
        if (i >= queues.length) throw new IndexOutOfBoundsException("No more queues: "+i);
        return queues[i];
        // todo clear and discard old queues???
    }
    public int queues() {
        return queues.length;
    }
}
