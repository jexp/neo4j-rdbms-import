package org.neo4j.imports;

import org.neo4j.unsafe.impl.batchimport.InputIterable;
import org.neo4j.unsafe.impl.batchimport.InputIterator;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;

/**
 * @author tbaum
 * @since 15.03.2015
 */
public abstract class AbstractGenerator<T> implements InputIterable<T> {
    static final Object[] NO_PROPS = new Object[0];
    private final String source;
    private final TableInfo[] tables;

    protected AbstractGenerator(String source, TableInfo[] tables) {
        this.source = source;
        this.tables = tables;
    }

    @Override public boolean supportsMultiplePasses() {
        return false;
    }

    Stream<T> stream() {
        return Stream.of(tables).map(table -> {
            try {
                return streamNodes(table);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }).flatMap(s -> s);
    }

    abstract Stream<T> streamNodes(TableInfo table) throws Exception;

    @Override
    public InputIterator<T> iterator() {
        Iterator<T> iterator = stream().iterator();
        return new InputIterator<T>() {
            long position;

            @Override
            public String sourceDescription() {
                return source;
            }

            @Override
            public long lineNumber() {
                return position;
            }

            @Override
            public long position() {
                return position;
            }

            @Override
            public void close() {
            }

            @Override
            public boolean hasNext() {
                return iterator.hasNext();
            }

            @Override
            public void remove() {
            }

            @Override
            public T next() {
                position++;
                return iterator.next();
            }
        };
    }

    protected Object extractPrimaryKeys(Rules rules, ResultSet rs, List<String> pks) throws SQLException {
        // todo prepend table name instead of using groups (hint from MP)
        StringBuilder sb = new StringBuilder(32);

        for (String pk : pks) {
            Object value = rules.transformPk(rs.getObject(pk));
            if (value == null) return null; // if one fk-part is null then the whole fk is invalid, or??

            sb.append(value);
            sb.append((char) 0);
        }
        return sb.toString();
    }

    protected Object[] prepareProps(TableInfo table, Rules rules) {
        List<String> propNames = rules.propertyNamesFor(table);
        if (propNames.size() == 0) return null;
        Object[] props = new Object[propNames.size() * 2];
        for (int i = 0; i < propNames.size(); i++) {
            props[i * 2] = propNames.get(i);//[i];
        }
        return props;
    }

    protected Object[] extractProps(TableInfo table, Rules rules, ResultSet rs, Object[] props) throws SQLException, IOException {
        if (table.fieldCount() == 0) return NO_PROPS;

        int length = props.length / 2;
        Object[] newProps = Arrays.copyOf(props, length * 2);
        for (int i = 0; i < length; i++) {
            String field = table.fields.get(i);
            newProps[1 + i * 2] = rules.convertValue(table, field, rs.getObject(field));
        }
        return newProps;
    }

    public String sourceDescription() {
        return source;
    }
}
