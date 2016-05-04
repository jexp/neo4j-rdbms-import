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

import org.apache.commons.io.IOUtils;
import org.neo4j.unsafe.impl.batchimport.input.Group;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Blob;
import java.sql.SQLException;
import java.util.*;

import static java.util.Arrays.asList;

/**
 * @author mh
 * @since 01.03.15
 */
public class Rules {

    private final Set<String> skipTables = new HashSet<>();

    public Rules(String... skipTables) {
        this.skipTables.addAll((asList(skipTables)));
    }

    String propertyNameFor(TableInfo table, String field) {
        return field;
    }

    List<String> propertyNamesFor(TableInfo table) {
        List<String> result = new ArrayList<>();
        for (String field : table.fields) {
            result.add(propertyNameFor(table, field));
        }
        return result;
    }

    String[] labelsFor(TableInfo table) {
        return new String[]{labelFor(table.table)};
    }

    public String labelFor(String table) {
        return unquote(table);
    }

    private String unquote(String name) {
        boolean quoted = name.charAt(0) == '`';
        return quoted ? name.substring(1, name.length() - 1) : name;
    }

    String relTypeFor(TableInfo table) {
        return unquote(table.table).replaceAll("([a-z]) ?([A-Z])", "$1_$2").toUpperCase().replace(' ', '_');
    }

    boolean isNode(TableInfo table) {
        return table.hasPk();
    }

    List<RelInfo> relsFor(TableInfo table) {
        if (table.fks == null) {
            return Collections.emptyList();
        }
        List<RelInfo> result = new ArrayList<>();
        for (Map.Entry<List<String>, String> entry : table.fks.entrySet()) {

            TableInfo tableInfo = TableInfo.get(entry.getValue());
            // todo fix
            result.add(new RelInfo(new Group.Adapter(tableInfo.index, tableInfo.table),
                    relTypeFor(tableInfo), entry.getKey()));
        }
        return result;
    }

    public Object transformPk(Object pk) {
        if (pk == null) return null;
        else return pk.toString();
    }

    public Object convertValue(TableInfo table, String field, Object value) throws SQLException, IOException {
        if (value instanceof Date) {
//            return 0;
            return ((Date) value).getTime();
        }
        if (value instanceof BigDecimal) return ((BigDecimal) value).doubleValue(); // or string??
        if (value instanceof Blob) {
            ByteArrayOutputStream bo = new ByteArrayOutputStream();
            IOUtils.copy(((Blob) value).getBinaryStream(), bo);
            return bo.toByteArray(); // or string??
        }
        // todo importer should ignore null values
        if (value == null) return "";
        return value;
    }


    public boolean skipTable(String tableName) {
        return skipTables.contains(tableName);
    }

    public boolean skipPrimaryKey(String tableName, String columnName) {
        return false;
    }
}
