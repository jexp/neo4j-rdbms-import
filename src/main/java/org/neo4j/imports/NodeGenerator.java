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
