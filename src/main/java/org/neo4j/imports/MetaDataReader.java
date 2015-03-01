package org.neo4j.imports;

import schemacrawler.schema.*;
import schemacrawler.schemacrawler.SchemaCrawlerException;
import schemacrawler.schemacrawler.SchemaCrawlerOptions;
import schemacrawler.schemacrawler.SchemaInfoLevel;
import schemacrawler.utility.SchemaCrawlerUtility;

import java.sql.Connection;
import java.util.*;

/**
 * @author mh
 * @since 01.03.15
 */
public class MetaDataReader {
    static TableInfo[] extractTables(Connection conn) throws Exception {
        ArrayList<TableInfo> tableList = new ArrayList<TableInfo>(100);

        final SchemaCrawlerOptions options = new SchemaCrawlerOptions();
        options.setSchemaInfoLevel(SchemaInfoLevel.standard());

        final Catalog catalog = SchemaCrawlerUtility.getCatalog(conn, options);

        for (final Schema schema: catalog.getSchemas())
        {
            System.out.println(schema);
            for (final Table table: catalog.getTables(schema))
            {
                System.out.println("o--> " + table + " pk " + table.getPrimaryKey() + " fks "+table.getForeignKeys()+ " type "+table.getTableType());
                if (table.getTableType().isView()) continue;
                List<Column> columns = table.getColumns();
                List<String> fields = new ArrayList<>(columns.size());
                for (final Column column: columns)
                {
//                    System.out.println("     o--> " + column + " pk: "+ column.isPartOfPrimaryKey() + " fk: " + column.isPartOfForeignKey());
                    if (column.isPartOfPrimaryKey() || column.isPartOfForeignKey()) {
                        // skip, todo strategy
                    } else {
                        fields.add(column.getName());
                    }
                }
                Map<String, String> fks = extractForeignKeys(table);
                String pk = extractPrimaryKeys(table, fks);

                tableList.add(TableInfo.add(table.getName(), pk, fields.toArray(new String[fields.size()]), fks));
            }
        }

        return tableList.toArray(new TableInfo[tableList.size()]);
    }

    private static Map<String, String> extractForeignKeys(Table table) {
        Collection<ForeignKey> foreignKeys = table.getForeignKeys();
        Map<String, String> fks = fks = new LinkedHashMap<>(10);
        if (foreignKeys!=null) {
            for (ForeignKey foreignKey : foreignKeys) {
                // todo handle composite keys
                ForeignKeyColumnReference reference = foreignKey.getColumnReferences().iterator().next();
                if (reference.getPrimaryKeyColumn().getParent().equals(table) &&
                        !reference.getForeignKeyColumn().getParent().equals(table)
                        ) continue;
                fks.put(reference.getForeignKeyColumn().getName(), reference.getPrimaryKeyColumn().getParent().getName());
            }
        }
        return fks;
    }

    private static String extractPrimaryKeys(Table table, Map<String, String> fks) {
        // todo handle composite keys
        List<String> pks = new ArrayList<>();
        String pk = null;
        if (table.getPrimaryKey() != null) {
            List<IndexColumn> pkColumns = table.getPrimaryKey().getColumns();
            for (IndexColumn pkColumn : pkColumns) {
                if (fks.containsKey(pkColumn.getName())) continue;
                pks.add(pkColumn.getName());
            }
            System.out.println("Real? Primary Keys "+pks);
            pk = pks.size() == 1 ? pks.get(0) : null;
        }
        return pk;
    }
}
