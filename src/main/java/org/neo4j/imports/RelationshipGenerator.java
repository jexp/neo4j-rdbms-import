package org.neo4j.imports;

import org.neo4j.unsafe.impl.batchimport.input.Group;
import org.neo4j.unsafe.impl.batchimport.input.InputRelationship;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;

import static org.neo4j.imports.StreamUtil.streamOf;

/**
 * @author mh
 * @since 01.03.15
 */
public class RelationshipGenerator extends AbstractGenerator<InputRelationship> {

    private final Rules rules;
    private final DataReader reader;

    public RelationshipGenerator(DataReader reader, Rules rules, TableInfo[] tables, String source) {
        super(source, tables);
        this.reader = reader;
        this.rules = rules;
    }

    Stream<InputRelationship> streamNodes(TableInfo table) throws SQLException {
        System.out.println("\nCreating relationships for " + table);
        if (rules.isNode(table)) {
            return importPkRels(table);
        } else {
            return importFkRels(table);
        }
    }

    private Stream<InputRelationship> importPkRels(TableInfo table) throws SQLException {
        ResultSet rs = reader.readTableData(table);
        Group group = new Group.Adapter(table.index, table.table);
        List<RelInfo> relInfos = rules.relsFor(table);

        return streamOf(new StreamUtil.Supplier<InputRelationship>() {
            private Object id;
            private Iterator<RelInfo> iterator;

            @Override public InputRelationship get() throws Exception {
                while (true) {
                    if (iterator == null || !iterator.hasNext()) {  // outer loop, fetch next row
                        if (rs.next()) {
                            id = extractPrimaryKeys(rules, rs, table.pk);
                            iterator = relInfos.iterator();
                        } else {
                            rs.getStatement().close();
                            return null;
                        }
                    }

                    while (iterator.hasNext()) {
                        RelInfo relInfo = iterator.next();
                        Object fkId = extractPrimaryKeys(rules, rs, relInfo.fields);
                        if (fkId != null) {
                            return new InputRelationship(table.table + "." + relInfo.fieldsString, rs.getRow(), 0,
                                    NO_PROPS, null, group, id, relInfo.group, fkId, relInfo.type, null);
                        }
                    }
                }
            }
        });
    }

    private Stream<InputRelationship> importFkRels(TableInfo table) throws SQLException {
        ResultSet rs = reader.readTableData(table);
        String relType = rules.relTypeFor(table);
        Object[] props = prepareProps(table, rules);
        List<RelInfo> relInfos = rules.relsFor(table);

        return streamOf(() -> {
            while (rs.next()) {
                Object fk1 = extractPrimaryKeys(rules, rs, relInfos.get(0).fields);
                Object fk2 = extractPrimaryKeys(rules, rs, relInfos.get(1).fields);
                if (fk1 != null && fk2 != null) {
                    return new InputRelationship(table.table, rs.getRow(), 0, extractProps(table, rules, rs, props),
                            null, relInfos.get(0).group, fk1, relInfos.get(1).group, fk2, relType, null);
                }
            }
            rs.getStatement().close();
            return null;
        });
    }
}
