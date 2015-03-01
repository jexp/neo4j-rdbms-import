package org.neo4j.imports;

import org.neo4j.unsafe.impl.batchimport.input.Group;

import java.math.BigDecimal;
import java.util.Date;
import java.util.Map;

/**
 * @author mh
 * @since 01.03.15
 */
public class Rules {

    public static final RelInfo[] NO_REL_INFOS = new RelInfo[0];

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
        return new String[]{table.table};
    }

    String relTypeFor(TableInfo table) {
        return table.table;
    }

    boolean isNode(TableInfo table) {
        return table.hasPk();
    }

    RelInfo[] relsFor(TableInfo table) {
        if (table.fks == null) return NO_REL_INFOS;
        RelInfo[] result = new RelInfo[table.fks.size()];
        int i = 0;
        for (Map.Entry<String, String> entry : table.fks.entrySet()) {
            TableInfo tableInfo = TableInfo.get(entry.getValue());
            result[i++] = new RelInfo(new Group.Adapter(tableInfo.index, tableInfo.table), relTypeFor(tableInfo), entry.getKey());
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
}
