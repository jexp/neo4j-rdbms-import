package org.neo4j.imports;

import org.neo4j.unsafe.impl.batchimport.InputIterable;
import org.neo4j.unsafe.impl.batchimport.InputIterator;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.stream.Stream;

/**
 * @author tbaum
 * @since 15.03.2015
 */
public class AsyncInputIterable<T> implements InputIterable<T> {
    private static final Object END = new Object();
    private final AbstractGenerator<T> source;

    public AsyncInputIterable(AbstractGenerator<T> generator) {
        this.source = generator;
    }

    @Override
    public boolean supportsMultiplePasses() {
        return source.supportsMultiplePasses();
    }

    @Override
    public InputIterator<T> iterator() {
        BlockingQueue<Object> queue = new ArrayBlockingQueue<>(10_000);

        //TODO should be source.iterator() here!
        final Stream<T> iterator = source.stream();

        new Thread(() -> {
            try {

                iterator.forEach(s -> {
                    try {
                        queue.put(s);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });
                queue.put(END);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }).start();

        return new InputIterator.Adapter<T>() {
            private T element = nextElement();

            private T nextElement() {
                try {
                    Object take = queue.take();
                    //noinspection unchecked
                    return END.equals(take) ? null : (T) take;
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                }
            }

            @Override
            public boolean hasNext() {
                return element != null;
            }

            @Override
            public T next() {
                T current = element;
                element = nextElement();
                return current;
            }
        };
    }
}
