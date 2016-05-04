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

import org.junit.Assert;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.helpers.collection.IteratorUtil;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.tooling.GlobalGraphOperations;

import java.io.File;
import java.sql.*;
import java.util.Properties;

/**
 * @author mh
 * @since 01.03.15
 */
public class DataImporterTest {

    public static final String STORE_DIR = "target/test.db";
    public static final int USERS = 100000;
    public static final int FRIENDSHIPS = 100000;

    public static void main(String[] args) throws Exception {
        setupDatabase();

        FileUtils.deleteRecursively(new File(STORE_DIR));
        long time = System.currentTimeMillis();
        new DatabaseImporter("jdbc:derby:memory:test", null, STORE_DIR, new Rules()).run();

        String result = assertImport();
        System.out.println(result + " in " + (System.currentTimeMillis() - time) / 1000 + " seconds");
    }

    private static void setupDatabase() throws SQLException {
        Connection connection = DriverManager.getConnection("jdbc:derby:memory:test;create=true", new Properties());
        Statement statement = connection.createStatement();
        // id INT NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 0, INCREMENT BY 1)
        statement.execute("CREATE TABLE USERS (id INT NOT NULL, name varchar(20), constraint users_pk_id primary key(id))");
        statement.execute("CREATE TABLE FRIENDS (id1 INT, id2 INT, " +
                " constraint fk_users_id1 foreign key(id1) references users(id)," +
                " constraint fk_users_id2 foreign key(id2) references users(id)" +
                ")");
        insertUsers(connection, USERS);
        insertFriendships(connection, FRIENDSHIPS);
//        statement.execute("INSERT INTO USERS (id,name) values(0,'Alice')");
//        statement.execute("INSERT INTO USERS (id,name) values(1,'Bob')");
//        statement.execute("INSERT INTO FRIENDS (id1,id2) values(0,1)");
    }

    private static String assertImport() {
        GraphDatabaseService db = new GraphDatabaseFactory().newEmbeddedDatabase(STORE_DIR);

        try (Transaction tx = db.beginTx()) {
            int nodes = IteratorUtil.count(db.getAllNodes());
            Assert.assertEquals(USERS, nodes);
            int rels = IteratorUtil.count(GlobalGraphOperations.at(db).getAllRelationships());
            Assert.assertEquals(FRIENDSHIPS, rels);
            return "Imported nodes " + nodes + " rels " + rels;
        } finally {
            db.shutdown();
        }
    }

    private static void insertUsers(Connection connection, int max) throws SQLException {
        PreparedStatement pstmt = connection.prepareStatement("INSERT INTO USERS (id,name) values(?,?)");
        for (int i = 0; i < max; i++) {
            pstmt.setInt(1, i);
            pstmt.setString(2, "Name " + i);
            pstmt.addBatch();
        }
        pstmt.executeBatch();
        pstmt.close();
    }

    private static void insertFriendships(Connection connection, int max) throws SQLException {
        PreparedStatement pstmt = connection.prepareStatement("INSERT INTO FRIENDS (id1,id2) values(?,?)");
        for (int i = 0; i < max; i++) {
            pstmt.setInt(1, i);
            pstmt.setInt(2, (i + 100) % max);
            pstmt.addBatch();
        }
        pstmt.executeBatch();
        pstmt.close();
    }
}
