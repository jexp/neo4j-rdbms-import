/**
 * Copyright (c) 2015 Michael Hunger
 *
 * This file is part of Relational to Neo4j Importer.
 *
 *  Relational to Neo4j Importer is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation, either version 3 of the License, or
 *  (at your option) any later version.
 *
 *  Relational to Neo4j Importer is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with Relational to Neo4j Importer.  If not, see <http://www.gnu.org/licenses/>.
 */
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
            public void remove() {}

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
            public void remove() {}
        };
    }

    interface Supplier<T> {
        T get() throws Exception;
    }
}
