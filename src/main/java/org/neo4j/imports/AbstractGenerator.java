package org.neo4j.imports;

import org.neo4j.unsafe.impl.batchimport.InputIterable;
import org.neo4j.unsafe.impl.batchimport.InputIterator;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import static java.util.Arrays.asList;

/**
 * @author tbaum
 * @since 15.03.2015
 */
public abstract class AbstractGenerator<T> implements InputIterable<T> {
    static final Object[] NO_PROPS = new Object[0];
    private final TableInfo[] tables;

    protected AbstractGenerator(TableInfo[] tables) {
        this.tables = tables;
    }

    @Override public boolean supportsMultiplePasses() {
        return false;
    }

    public InputIterator<T> iterator() {
        final Iterator<TableInfo> iterator = asList(tables).iterator();

        return new InputIterator.Adapter<T>() {
            boolean hasNext = true;
            Iterator<T> current = null;
            T next = fetchNext();

            @Override public boolean hasNext() {
                return hasNext;
            }

            @Override public T next() {
                T result = next;
                next = fetchNext();
                return result;
            }

            private T fetchNext() {
                while (true) {
                    if (current == null || !current.hasNext()) {
                        if (iterator.hasNext())
                            try {
                                current = streamNodes(iterator.next());
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                        else {
                            hasNext = false;
                            return null;
                        }
                    }
                    if (current.hasNext()) {
                        return current.next();
                    }
                }
            }
        };
    }

    abstract Iterator<T> streamNodes(TableInfo table) throws Exception;

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
}
