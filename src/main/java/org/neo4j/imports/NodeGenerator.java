package org.neo4j.imports;

import org.neo4j.unsafe.impl.batchimport.input.Group;
import org.neo4j.unsafe.impl.batchimport.input.InputNode;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.stream.Stream;

import static org.neo4j.imports.StreamUtil.streamOf;

/**
 * @author mh
 * @since 01.03.15
 */
public class NodeGenerator extends AbstractGenerator<InputNode> {

    private final Rules rules;
    private final DataReader reader;

    public NodeGenerator(DataReader reader, Rules rules, TableInfo[] tables, String source) {
        super(source, tables);
        this.rules = rules;
        this.reader = reader;
    }

    Stream<InputNode> streamNodes(TableInfo table) throws SQLException {
        System.out.println("\nCreating nodes for " + table);
        if (rules.isNode(table)) {
            ResultSet rs = reader.readTableData(table);
            Group group = new Group.Adapter(table.index, table.table);
            String[] labels = rules.labelsFor(table);
            Object[] props = prepareProps(table, rules);

            return streamOf(() -> {
                if (rs.next()) {
                    return new InputNode(table.table, rs.getRow(), 0, group, extractPrimaryKeys(rules, rs, table.pk),
                            extractProps(table, rules, rs, props), null, labels, null);
                }
                rs.getStatement().close();
                return null;
            });
        }
        return Stream.empty();
    }
}
