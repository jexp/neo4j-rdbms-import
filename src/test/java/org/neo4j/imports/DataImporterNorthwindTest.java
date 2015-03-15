package org.neo4j.imports;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.helpers.collection.IteratorUtil;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.tooling.GlobalGraphOperations;

import java.io.File;

/**
 * @author mh
 * @since 01.03.15
 * Download: https://code.google.com/p/northwindextended/downloads/detail?name=Northwind.MySQL5.sql
 */
public class DataImporterNorthwindTest {

    public static final String STORE_DIR = "target/northwind.db";

    public static void main(String[] args) throws Exception {
        FileUtils.deleteRecursively(new File(STORE_DIR));
        long time = System.currentTimeMillis();
        Rules rules = new Rules(); // asList("titles")
        new DatabaseImporter("jdbc:mysql://localhost:3306/northwind?user=root", "northwind", STORE_DIR, rules).run();
        long delta = (System.currentTimeMillis() - time) / 1000;
        String result = importInfo();
        System.out.println(result + " in " + delta + " seconds");
    }

    private static String importInfo() {
        GraphDatabaseService db = new GraphDatabaseFactory().newEmbeddedDatabase(STORE_DIR);
        try (Transaction tx = db.beginTx()) {
            int nodes = IteratorUtil.count(db.getAllNodes());
            int rels = IteratorUtil.count(GlobalGraphOperations.at(db).getAllRelationships());
            return "Imported nodes " + nodes + " rels " + rels;
        } finally {
            db.shutdown();
        }
    }
}
