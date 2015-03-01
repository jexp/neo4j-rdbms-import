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
import org.neo4j.unsafe.impl.batchimport.input.Input;
import org.neo4j.unsafe.impl.batchimport.input.InputNode;
import org.neo4j.unsafe.impl.batchimport.input.InputRelationship;
import org.neo4j.unsafe.impl.batchimport.staging.ExecutionMonitors;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.util.concurrent.BlockingQueue;

/**
 * @author mh
 * @since 01.03.15
 */
public class DatabaseImporter {

    String jdbcUrl;
    String storeDir;

    public static void main(String[] args) throws Exception {
        new DatabaseImporter(args[0],args[1]).run();
    }


    public DatabaseImporter(String jdbcUrl, String storeDir) {
        this.jdbcUrl = jdbcUrl;
        this.storeDir = storeDir;
    }

    public void run() throws Exception {
        Connection conn = DriverManager.getConnection(jdbcUrl);
        Rules rules = new Rules();

        Logging logging = new SystemOutLogging();
        ParallelBatchImporter importer = new ParallelBatchImporter(storeDir, Configuration.DEFAULT, logging, ExecutionMonitors.defaultVisible());
        TableInfo[] tables = MetaDataReader.extractTables(conn);

        Queues<InputNode> nodes = new Queues<InputNode>(1,1_000_000);
        Queues<InputRelationship> rels = new Queues<InputRelationship>(2,10_000_000);
        for (TableInfo table : tables) {
            ResultSet rs = DataReader.readTableData(conn, table);
            new Transformer().stream(table, rules,rs,nodes,rels);
        }
        nodes.put(Transformer.END_NODE);
        rels.put(Transformer.END_REL);
        importer.doImport(createInput(nodes,rels));
    }

    private Input createInput(final Queues<InputNode> nodes, final Queues<InputRelationship> rels) {
        return new Input() {
            @Override
            public InputIterable<InputNode> nodes() {
                return new QueueInputIterable<>(Transformer.END_NODE, nodes);
            }

            @Override
            public InputIterable<InputRelationship> relationships() {
                return new QueueInputIterable<>(Transformer.END_REL, rels);
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
        };
    }

    private static class QueueInputIterable<T> implements InputIterable<T> {
        private final Queues<T> queues;
        private final T tombstone;
        private int index = 0;

        public QueueInputIterable(T tombstone, Queues<T> queues) {
            this.queues = queues;
            this.tombstone = tombstone;
        }

        @Override
        public InputIterator<T> iterator() {
            System.out.println("iterator "+ type());
            final BlockingQueue<T> queue = nextQueue();
            return new InputIterator<T>() {
                long position;
                T element = nextElement();

                private T nextElement() {
                    try {
                        position++;
                        T next = queue.take();
                        return next.equals(tombstone) ? null : next;
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException(e);
                    }
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

        private BlockingQueue<T> nextQueue() {
            return queues.queue(index++);
        }
    }
}
