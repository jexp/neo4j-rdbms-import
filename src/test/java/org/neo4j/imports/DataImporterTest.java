package org.neo4j.imports;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.tooling.GlobalGraphOperations;
import schemacrawler.schema.Catalog;
import schemacrawler.schema.Column;
import schemacrawler.schema.Schema;
import schemacrawler.schema.Table;
import schemacrawler.schemacrawler.SchemaCrawlerException;
import schemacrawler.schemacrawler.SchemaCrawlerOptions;
import schemacrawler.schemacrawler.SchemaInfoLevel;
import schemacrawler.utility.SchemaCrawlerUtility;

import java.io.File;
import java.io.PrintWriter;
import java.sql.*;
import java.util.Properties;

/**
 * @author mh
 * @since 01.03.15
 */
public class DataImporterTest {

    public static void main(String[] args) throws Exception {
        Connection connection = DriverManager.getConnection("jdbc:derby:memory:test;create=true", new Properties());
        Statement statement = connection.createStatement();
        // id INT NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 0, INCREMENT BY 1)
        statement.execute("CREATE TABLE USERS (id INT NOT NULL, name varchar(20), constraint users_pk_id primary key(id))");
        statement.execute("CREATE TABLE FRIENDS (id1 INT, id2 INT, " +
                " constraint fk_users_id1 foreign key(id1) references users(id)," +
                " constraint fk_users_id2 foreign key(id2) references users(id)" +
                ")");
        statement.execute("INSERT INTO USERS (id,name) values(0,'Alice')");
        statement.execute("INSERT INTO USERS (id,name) values(1,'Bob')");
        statement.execute("INSERT INTO FRIENDS (id1,id2) values(0,1)");


        FileUtils.deleteRecursively(new File("target/test.db"));
        new DatabaseImporter("jdbc:derby:memory:test","target/test.db").run();

        GraphDatabaseService db = new GraphDatabaseFactory().newEmbeddedDatabase("target/test.db");
        try (Transaction tx = db.beginTx()) {
            for (Node node : db.getAllNodes()) {
                System.out.println(node);
            }
            for (Relationship relationship : GlobalGraphOperations.at(db).getAllRelationships()) {
                System.out.println(relationship);
            }
        }
        db.shutdown();
    }
}
