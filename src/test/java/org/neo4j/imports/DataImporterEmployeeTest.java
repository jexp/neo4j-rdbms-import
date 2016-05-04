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
import org.perf4j.StopWatch;

import java.io.File;

/**
 * @author mh
 * @since 01.03.15
 * jdbc: http://dev.mysql.com/doc/connector-j/en/connector-j-reference-configuration-properties.html
 * Source: https://dev.mysql.com/doc/employee/en/employees-installation.html
 * Structure: https://dev.mysql.com/doc/employee/en/sakila-structure.html
 * Download: http://dev.mysql.com/doc/index-other.html
 */
public class DataImporterEmployeeTest {

    public static final String STORE_DIR = "target/employee.db";

    public static void main(String[] args) throws Exception {
        FileUtils.deleteRecursively(new File(STORE_DIR));
        StopWatch watch = new StopWatch();
        Rules rules = new Rules(); // asList("titles")
        new DatabaseImporter("jdbc:mysql://localhost:3306/employees?user=root", "employees", STORE_DIR, rules).run();

        watch.lap("import");
        System.err.println(watch.stop("import", importInfo()));
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
