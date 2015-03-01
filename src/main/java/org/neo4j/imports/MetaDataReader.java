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
                    System.out.println("     o--> " + column + " pk: "+ column.isPartOfPrimaryKey() + " fk: " + column.isPartOfForeignKey());
                    if (column.isPartOfPrimaryKey() || column.isPartOfForeignKey()) {
                        // skip, todo strategy
                    } else {
                        fields.add(column.getName());
                    }
                }
                // todo handle composite keys
                String pk = table.getPrimaryKey() == null ?  null :
                        table.getPrimaryKey().getColumns().iterator().next().getName();
                Collection<ForeignKey> foreignKeys = table.getForeignKeys();
                Map<String, String> fks = null;
                if (foreignKeys!=null) {
                    fks = new LinkedHashMap<>(foreignKeys.size());
                    for (ForeignKey foreignKey : foreignKeys) {
                        // todo handle composite keys
                        ForeignKeyColumnReference reference = foreignKey.getColumnReferences().iterator().next();
                        if (reference.getPrimaryKeyColumn().getParent().equals(table) &&
                           !reference.getForeignKeyColumn().getParent().equals(table)
                                ) continue;
                        fks.put(reference.getForeignKeyColumn().getName(), reference.getPrimaryKeyColumn().getParent().getName());
                    }
                }
                tableList.add(TableInfo.add(table.getName(), pk, fields.toArray(new String[fields.size()]), fks));
            }
        }

        return tableList.toArray(new TableInfo[tableList.size()]);
    }
}
