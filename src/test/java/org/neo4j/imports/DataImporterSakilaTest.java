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
public class DataImporterSakilaTest {

    public static final String STORE_DIR = "target/sakila.db";

    public static void main(String[] args) throws Exception {
        FileUtils.deleteRecursively(new File(STORE_DIR));
        long time = System.currentTimeMillis();
        new DatabaseImporter("jdbc:mysql://localhost:3306/sakila?user=root", STORE_DIR).run();

        String result = assertImport();
        System.out.println(result+ " in "+(System.currentTimeMillis()-time)/1000+ " seconds");
    }

    private static String assertImport() {
        GraphDatabaseService db = new GraphDatabaseFactory().newEmbeddedDatabase(STORE_DIR);

        try (Transaction tx = db.beginTx()) {
            int nodes = IteratorUtil.count(db.getAllNodes());
//            Assert.assertEquals(USERS, nodes);
            int rels = IteratorUtil.count(GlobalGraphOperations.at(db).getAllRelationships());
//            Assert.assertEquals(FRIENDSHIPS, rels);
            return "Imported nodes "+nodes+" rels "+rels;
        } finally {
            db.shutdown();
        }
    }
}
