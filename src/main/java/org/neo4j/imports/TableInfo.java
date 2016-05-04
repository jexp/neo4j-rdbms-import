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

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
* @author mh
* @since 01.03.15
*/
public class TableInfo {
    public static AtomicInteger TABLE_INDEX = new AtomicInteger();
    private static final Map<String, TableInfo> TABLES = new LinkedHashMap<>();

    public final int index = TABLE_INDEX.getAndIncrement();
    public final String table;
    public final List<String> pk;
    public final List<String> fields;
    public final Map<List<String>, String> fks;

    public TableInfo(String table,List< String> pk, List<String> fields, Map<List<String>, String> fks) {
        this.table = table;
        this.pk = pk;
        this.fields = fields;
        this.fks = fks;
    }

    public boolean hasPk() {
        return pk != null && pk.size() > 0;
    }

    public int fieldCount() {
        return fields.size();
    }

    public static TableInfo get(String table) {
        return TABLES.get(table);
    }

    public static TableInfo add(String table, List<String> pks, List<String> fields, Map<List<String>, String> fks) {
        TableInfo tableInfo = new TableInfo(table, pks, fields, fks);
        TABLES.put(table, tableInfo);
        return tableInfo;
    }

    @Override
    public String toString() {
        return "TableInfo{" +
                "index=" + index +
                ", table='" + table + '\'' +
                ", pk='" + pk + '\'' +
                ", fields=" + fields +
                ", fks=" + fks +
                '}';
    }
}
