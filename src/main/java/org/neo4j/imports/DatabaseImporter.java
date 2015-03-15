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
    private final String source;
    private final Rules rules;
    private final TableInfo[] tables;

    public DatabaseImporter(String jdbcUrl, String schema, String storeDir, Rules rules) throws SQLException, SchemaCrawlerException {
        this.storeDir = storeDir;
        this.rules = rules;
        Connection conn = DriverManager.getConnection(jdbcUrl);
        tables = MetaDataReader.extractTables(conn, schema, rules);
        reader = new DataReader(conn);
        source = jdbcUrl + " schema: " + schema;
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
                return new NodeGenerator(reader, rules, tables, source);
            }

            @Override
            public InputIterable<InputRelationship> relationships() {
                return new AsyncInputIterable<>(new RelationshipGenerator(reader, rules, tables, source));
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
