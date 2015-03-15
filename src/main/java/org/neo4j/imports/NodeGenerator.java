package org.neo4j.imports;

import org.neo4j.unsafe.impl.batchimport.input.Group;
import org.neo4j.unsafe.impl.batchimport.input.InputNode;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Iterator;

import static org.neo4j.imports.StreamUtil.iteratorOf;

/**
 * @author mh
 * @since 01.03.15
 */
public class NodeGenerator extends AbstractGenerator<InputNode> {

    private final Rules rules;
    private final DataReader reader;

    public NodeGenerator(DataReader reader, Rules rules, TableInfo[] tables) {
        super(tables);
        this.rules = rules;
        this.reader = reader;
    }

    Iterator<InputNode> streamNodes(final TableInfo table) throws SQLException {
        System.out.println("\nCreating nodes for " + table);
        if (rules.isNode(table)) {
            final ResultSet rs = reader.readTableData(table);
            final Group group = new Group.Adapter(table.index, table.table);
            final String[] labels = rules.labelsFor(table);
            final Object[] props = prepareProps(table, rules);

            return iteratorOf(new StreamUtil.Supplier<InputNode>() {
                @Override public InputNode get() throws Exception {
                    if (rs.next()) {
                        return new InputNode(table.table, rs.getRow(), 0, group,
                                extractPrimaryKeys(rules, rs, table.pk), extractProps(table, rules, rs, props),
                                null, labels, null);
                    }
                    rs.getStatement().close();
                    return null;
                }
            });
        }
        return StreamUtil.emptyIterator();
    }
}
