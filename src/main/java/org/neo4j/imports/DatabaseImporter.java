package org.neo4j.imports;

import org.neo4j.kernel.logging.Logging;
import org.neo4j.kernel.logging.SystemOutLogging;
import org.neo4j.unsafe.impl.batchimport.Configuration;
import org.neo4j.unsafe.impl.batchimport.InputIterable;
import org.neo4j.unsafe.impl.batchimport.InputIterator;
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
import org.neo4j.unsafe.impl.batchimport.staging.ExecutionMonitors;

import java.io.IOException;
import java.io.OutputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author mh
 * @since 01.03.15
 */
public class DatabaseImporter {

    String jdbcUrl;
    String storeDir;
    private String schema;

    public static void main(String[] args) throws Exception {
        new DatabaseImporter(args[0],args[1],args[2]).run(new Rules());
    }


    public DatabaseImporter(String jdbcUrl, String schema, String storeDir) {
        this.jdbcUrl = jdbcUrl;
        this.schema = schema;
        this.storeDir = storeDir;
    }

    public void run(Rules rules) throws Exception {
        Connection conn = DriverManager.getConnection(jdbcUrl);

        Logging logging = new SystemOutLogging();
        final ParallelBatchImporter importer = new ParallelBatchImporter(storeDir, Configuration.DEFAULT, logging, ExecutionMonitors.defaultVisible());
        TableInfo[] tables = MetaDataReader.extractTables(conn, schema, rules);

        final BlockingQueue<InputNode> nodes = new ArrayBlockingQueue<>(1_000_000);
        final BlockingQueue<InputRelationship> rels = new ArrayBlockingQueue<>(5_000_000);
        final AtomicBoolean done = new AtomicBoolean();
        Thread thread = new Thread() {
            public void run() {
                try {
                    importer.doImport(createInput(nodes, rels));
                    done.set(true);
                } catch (IOException ioe) {
                    throw new RuntimeException("Error Importing Data", ioe);
                }
            }
        };
        thread.start();

        for (TableInfo table : tables) {
            ResultSet rs = DataReader.readTableData(conn, table);
            new Transformer().stream(table, rules, rs, nodes, rels);
            rs.getStatement().close();
        }
        nodes.put(Transformer.END_NODE);
        rels.put(Transformer.END_REL);
        while (!done.get()) {
//            Thread.sleep(200);
            Thread.yield();
        }
        thread.join();
    }

    private Input createInput(final BlockingQueue<InputNode> nodes, final BlockingQueue<InputRelationship> rels) {
        return new Input() {
            @Override
            public InputIterable<InputNode> nodes() {
                return new QueueInputIterable<>(Transformer.END_NODE, nodes, jdbcUrl+" schema: "+schema);
            }

            @Override
            public InputIterable<InputRelationship> relationships() {
                return new QueueInputIterable<>(Transformer.END_REL, rels, jdbcUrl+" schema: "+schema);
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
                        System.err.println("Bad Rel: "+inputRelationship);
                        throw new RuntimeException("Bad Rel");
                    }
                };
            }
        };
    }

    private static class QueueInputIterable<T> implements InputIterable<T> {
        private final BlockingQueue<T> queues;
        private final T tombstone;
        private int index = 0;
        private String source;

        public QueueInputIterable(T tombstone, BlockingQueue<T> queues, String source) {
            this.queues = queues;
            this.tombstone = tombstone;
            this.source = source;
        }


        @Override
        public boolean supportsMultiplePasses()
        {
            return false;
        }

        @Override
        public InputIterator<T> iterator() {

//            System.out.println("iterator "+ type());
//            queues.next();
            return new InputIterator<T>() {
                long position;
                T element = nextElement();

                private T nextElement() {
                    try {
                        position++;
                        T next = queues.take();
                        return next.equals(tombstone) ? null : next;
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException(e);
                    }
                }

                @Override
                public String sourceDescription() {
                    return source;
                }

                @Override
                public long lineNumber() {
                    return position;
                }

                @Override
                public long position() {
                    return position;
                }

                @Override
                public void close() {
//                                Thread.currentThread().interrupt();
                }

                @Override
                public boolean hasNext() {
                    return element != null;
                }

                @Override
                public void remove() {
                }

                @Override
                public T next() {
                    T current = element;
                    element = nextElement();
                    return current;
                }
            };
        }

        private String type() {
            return tombstone.getClass().getSimpleName();
        }

    }
}
