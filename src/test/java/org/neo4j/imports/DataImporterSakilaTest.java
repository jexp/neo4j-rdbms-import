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
 * Install: http://dev.mysql.com/doc/sakila/en/sakila-installation.html
 * Structure: http://dev.mysql.com/doc/sakila/en/sakila-structure.html
 * Download: http://dev.mysql.com/doc/index-other.html
 */
public class DataImporterSakilaTest {

    public static final String STORE_DIR = "target/sakila.db";

    public static void main(String[] args) throws Exception {
        FileUtils.deleteRecursively(new File(STORE_DIR));
        long time = System.currentTimeMillis();
        new DatabaseImporter("jdbc:mysql://localhost:3306/sakila?user=root", "sakila", STORE_DIR, new Rules()).run();

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
