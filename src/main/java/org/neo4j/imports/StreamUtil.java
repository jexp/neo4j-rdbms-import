package org.neo4j.imports;

import java.util.Iterator;
import java.util.stream.Stream;

import static java.util.Spliterator.ORDERED;
import static java.util.Spliterators.spliteratorUnknownSize;
import static java.util.stream.StreamSupport.stream;

/**
 * @author tbaum
 * @since 15.03.2015
 */
public class StreamUtil {

    public static <T> Stream<T> streamOf(Supplier<T> s) {
        return stream(spliteratorUnknownSize(
                new Iterator<T>() {
                    T next = nextValue();

                    @Override public boolean hasNext() {
                        return next != null;
                    }

                    @Override public T next() {
                        T result = next;
                        next = nextValue();
                        return result;
                    }

                    T nextValue() {
                        try {
                            return s.get();
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    }
                }, ORDERED), false);
    }

    interface Supplier<T> {
        T get() throws Exception;
    }
}
