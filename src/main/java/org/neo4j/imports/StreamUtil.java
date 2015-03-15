package org.neo4j.imports;

import java.util.Iterator;

/**
 * @author tbaum
 * @since 15.03.2015
 */
public class StreamUtil {

    public static <T> Iterator<T> iteratorOf(final Supplier<T> s) {
        return new Iterator<T>() {
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
        };
    }

    static <T> Iterator<T> emptyIterator() {
        return new Iterator<T>() {
            @Override public boolean hasNext() {
                return false;
            }

            @Override public T next() {
                return null;
            }
        };
    }

    interface Supplier<T> {
        T get() throws Exception;
    }
}
