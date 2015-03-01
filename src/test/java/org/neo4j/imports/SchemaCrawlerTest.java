package org.neo4j.imports;

import org.apache.derby.jdbc.AutoloadedDriver;
import org.apache.derby.jdbc.Driver42;
import org.apache.derby.jdbc.EmbeddedDriver;
import org.apache.derby.jdbc.InternalDriver;
import schemacrawler.schema.Catalog;
import schemacrawler.schema.Column;
import schemacrawler.schema.Schema;
import schemacrawler.schema.Table;
import schemacrawler.schemacrawler.SchemaCrawlerException;
import schemacrawler.schemacrawler.SchemaCrawlerOptions;
import schemacrawler.schemacrawler.SchemaInfoLevel;
import schemacrawler.utility.SchemaCrawlerUtility;

import java.sql.*;
import java.util.Properties;

/**
 * @author mh
 * @since 01.03.15
 */
public class SchemaCrawlerTest {

    public static void main(String[] args) throws SQLException, SchemaCrawlerException, ClassNotFoundException {

        Driver driver = DriverManager.getDriver("jdbc:derby:memory:test;create=true");
        Connection connection = DriverManager.getConnection("jdbc:derby:memory:test;create=true", new Properties());
        Statement statement = connection.createStatement();
        // id INT NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 0, INCREMENT BY 1)
        statement.execute("CREATE TABLE USERS (id INT NOT NULL, name varchar(20), constraint users_pk_id primary key(id))");
        statement.execute("CREATE TABLE FRIENDS (id1 INT, id2 INT, " +
                " constraint fk_users_id1 foreign key(id1) references users(id)," +
                " constraint fk_users_id2 foreign key(id2) references users(id)" +
                ")");

        final SchemaCrawlerOptions options = new SchemaCrawlerOptions();
        options.setSchemaInfoLevel(SchemaInfoLevel.standard());

        final Catalog catalog = SchemaCrawlerUtility.getCatalog(connection, options);

        for (final Schema schema: catalog.getSchemas())
        {
            System.out.println(schema);
            for (final Table table: catalog.getTables(schema))
            {
                System.out.println("o--> " + table + " pk " + table.getPrimaryKey() + " fks "+table.getForeignKeys()+ " type "+table.getTableType());
                for (final Column column: table.getColumns())
                {
                    System.out.println("     o--> " + column + " pk: "+ column.isPartOfPrimaryKey() + " fk: " + column.isPartOfForeignKey());
                }
            }
        }
    }
}
