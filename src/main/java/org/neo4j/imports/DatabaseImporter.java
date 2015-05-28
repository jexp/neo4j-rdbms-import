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

import org.neo4j.kernel.logging.SystemOutLogging;
import org.neo4j.unsafe.impl.batchimport.InputIterable;
import org.neo4j.unsafe.impl.batchimport.ParallelBatchImporter;
import org.neo4j.unsafe.impl.batchimport.cache.NumberArrayFactory;
import org.neo4j.unsafe.impl.batchimport.cache.idmapping.IdGenerator;
import org.neo4j.unsafe.impl.batchimport.cache.idmapping.IdGenerators;
import org.neo4j.unsafe.impl.batchimport.cache.idmapping.IdMapper;
import org.neo4j.unsafe.impl.batchimport.cache.idmapping.IdMappers;
import org.neo4j.unsafe.impl.batchimport.input.Collector;
import org.neo4j.unsafe.impl.batchimport.input.Input;
import org.neo4j.unsafe.impl.batchimport.input.InputNode;
import org.neo4j.unsafe.impl.batchimport.input.InputRelationship;
import schemacrawler.schemacrawler.SchemaCrawlerException;

import java.io.IOException;
import java.io.OutputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import static org.neo4j.unsafe.impl.batchimport.Configuration.DEFAULT;
import static org.neo4j.unsafe.impl.batchimport.staging.ExecutionMonitors.defaultVisible;

/**
 * @author mh
 * @since 01.03.15
 */
public class DatabaseImporter {

    private final String storeDir;
    private final DataReader reader;
    private final Rules rules;
    private final TableInfo[] tables;

    public DatabaseImporter(String jdbcUrl, String schema, String storeDir, Rules rules) throws SQLException, SchemaCrawlerException {
        this.storeDir = storeDir;
        this.rules = rules;
        Connection conn = DriverManager.getConnection(jdbcUrl);
        tables = MetaDataReader.extractTables(conn, schema, rules);
        reader = new DataReader(conn);
    }

    public static void main(String[] args) throws Exception {
        String[] skip = new String[args.length - 3];
        System.arraycopy(args, 3, skip, 0, args.length - 3);
        new DatabaseImporter(args[0], args[1], args[2], new Rules(skip)).run();
    }

    public void run() throws IOException {
        ParallelBatchImporter importer = new ParallelBatchImporter(storeDir, DEFAULT, new SystemOutLogging(), defaultVisible());

        importer.doImport(new Input() {
            @Override
            public InputIterable<InputNode> nodes() {
                return new NodeGenerator(reader, rules, tables);
            }

            @Override
            public InputIterable<InputRelationship> relationships() {
                return new RelationshipGenerator(reader, rules, tables);
            }

            @Override
            public IdMapper idMapper() {
                return IdMappers.strings(NumberArrayFactory.AUTO);
            }

            @Override
            public IdGenerator idGenerator() {
                return IdGenerators.startingFromTheBeginning();
            }

            @Override
            public boolean specificRelationshipIds() {
                return false;
            }

            @Override
            public Collector<InputRelationship> badRelationshipsCollector(OutputStream outputStream) {
                return new Collector.Adapter<InputRelationship>() {
                    @Override
                    public void collect(InputRelationship inputRelationship, Object o) {
                        System.err.println("Bad Rel: " + inputRelationship);
                        throw new RuntimeException("Bad Rel");
                    }
                };
            }
        });
    }
}
