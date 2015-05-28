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

        for (final Schema schema : catalog.getSchemas()) {
            System.out.println(schema);
            for (final Table table : catalog.getTables(schema)) {
                System.out.println("o--> " + table + " pk " + table.getPrimaryKey() + " fks " + table.getForeignKeys() + " type " + table.getTableType());
                for (final Column column : table.getColumns()) {
                    System.out.println("     o--> " + column + " pk: " + column.isPartOfPrimaryKey() + " fk: " + column.isPartOfForeignKey());
                }
            }
        }
    }
}
