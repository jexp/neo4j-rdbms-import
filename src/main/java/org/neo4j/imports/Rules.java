package org.neo4j.imports;

import org.apache.commons.io.IOUtils;
import org.neo4j.unsafe.impl.batchimport.input.Group;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Blob;
import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;

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
        return table.fields.stream().map((field) -> propertyNameFor(table, field)).collect(Collectors.toList());
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

        return table.fks.entrySet().stream().map((entry) -> {
            TableInfo tableInfo = TableInfo.get(entry.getValue());
            // todo fix
            return new RelInfo(new Group.Adapter(tableInfo.index, tableInfo.table),
                    relTypeFor(tableInfo), entry.getKey());
        }).collect(Collectors.toList());
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
