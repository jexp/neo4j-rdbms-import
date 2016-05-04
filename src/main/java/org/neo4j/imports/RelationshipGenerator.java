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

import org.neo4j.unsafe.impl.batchimport.input.Group;
import org.neo4j.unsafe.impl.batchimport.input.InputRelationship;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.List;

import static org.neo4j.imports.StreamUtil.iteratorOf;

/**
 * @author mh
 * @since 01.03.15
 */
public class RelationshipGenerator extends AbstractGenerator<InputRelationship> {

    private final Rules rules;
    private final DataReader reader;

    public RelationshipGenerator(DataReader reader, Rules rules, TableInfo[] tables) {
        super(tables);
        this.reader = reader;
        this.rules = rules;
    }

    Iterator<InputRelationship> streamNodes(TableInfo table) throws SQLException {
        System.out.println("\nCreating relationships for " + table);
        if (rules.isNode(table)) {
            return importPkRels(table);
        } else {
            return importFkRels(table);
        }
    }

    private Iterator<InputRelationship> importPkRels(final TableInfo table) throws SQLException {
        final ResultSet rs = reader.readTableData(table);
        final Group group = new Group.Adapter(table.index, table.table);
        final List<RelInfo> relInfos = rules.relsFor(table);

        return iteratorOf(new StreamUtil.Supplier<InputRelationship>() {
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

    private Iterator<InputRelationship> importFkRels(final TableInfo table) throws SQLException {
        final ResultSet rs = reader.readTableData(table);
        final String relType = rules.relTypeFor(table);
        final Object[] props = prepareProps(table, rules);
        final List<RelInfo> relInfos = rules.relsFor(table);

        return iteratorOf(new StreamUtil.Supplier<InputRelationship>() {
            @Override public InputRelationship get() throws Exception {
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
            }
        });
    }
}
