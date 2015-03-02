package org.neo4j.imports;

import org.neo4j.unsafe.impl.batchimport.input.Group;

import java.math.BigDecimal;
import java.util.*;

/**
 * @author mh
 * @since 01.03.15
 */
public class Rules {

    public static final RelInfo[] NO_REL_INFOS = new RelInfo[0];
    private final Set<String> skipTables = new HashSet<>();

    public Rules() {
        this(null);
    }
    public Rules(Collection<String> skipTables) {
        if (skipTables!=null)
            this.skipTables.addAll(skipTables);
    }

    String propertyNameFor(TableInfo table, String field) {
        return field;
    }

    String[] propertyNamesFor(TableInfo table) {
// different rules
//            return table.fields;
//            return Arrays.copyOf(table.fields,table.fields.length);
        int length = table.fieldCount();
        String[] propNames = new String[length];
        for (int i = 0; i < length; i++) {
            propNames[i] = propertyNameFor(table, table.fields[i]);
        }
        ;
        return propNames;
    }

    String[] labelsFor(TableInfo table) {
        return new String[]{labelFor(table.table)};
    }

    public String labelFor(String table) {
        return unquote(table);
    }

    private String unquote(String name) {
        boolean quoted = name.charAt(0) == '`';
        return quoted ? name.substring(1,name.length()-1) : name;
    }

    String relTypeFor(TableInfo table) {
        return unquote(table.table).replaceAll("([a-z]) ?([A-Z])","$1_$2").toUpperCase().replace(' ','_');
    }

    boolean isNode(TableInfo table) {
        return table.hasPk();
    }

    RelInfo[] relsFor(TableInfo table) {
        if (table.fks == null) return NO_REL_INFOS;
        RelInfo[] result = new RelInfo[table.fks.size()];
        int i = 0;
        for (Map.Entry<List<String>, String> entry : table.fks.entrySet()) {
            TableInfo tableInfo = TableInfo.get(entry.getValue());
            Group.Adapter group = new Group.Adapter(tableInfo.index, tableInfo.table);
            List<String> fks = entry.getKey();
            // todo fix
            String[] fields = fks.toArray(new String[fks.size()]);
            result[i++] = new RelInfo(group, relTypeFor(tableInfo), fields);
        }
        return result;
    }

    public Object transformPk(Object pk) {
        if (pk==null) return null; else return pk.toString();
    }

    public Object convertValue(TableInfo table, String field, Object value) {
        if (value instanceof Date) return ((Date)value).getTime();
        if (value instanceof BigDecimal) return ((BigDecimal)value).doubleValue(); // or string??
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
