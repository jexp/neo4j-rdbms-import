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
    public final String[] pk;
    public final String[] fields;
    public final Map<List<String>, String> fks;

    public TableInfo(String table, String[] pk, String[] fields, Map<List<String>, String> fks) {
        this.table = table;
        this.pk = pk;
        this.fields = fields;
        this.fks = fks;
    }

    public boolean hasPk() {
        return pk != null && pk.length > 0;
    }

    public int fieldCount() {
        return fields.length;
    }

    public static TableInfo get(String table) {
        return TABLES.get(table);
    }

    public static TableInfo add(String table, String[] pks, String[] fields, Map<List<String>, String> fks) {
        TableInfo tableInfo = new TableInfo(table, pks, fields, fks);
        TABLES.put(table, tableInfo);
        return tableInfo;
    }

    @Override
    public String toString() {
        return "TableInfo{" +
                "index=" + index +
                ", table='" + table + '\'' +
                ", pk='" + Arrays.toString(pk) + '\'' +
                ", fields=" + Arrays.toString(fields) +
                ", fks=" + fks +
                '}';
    }
}
