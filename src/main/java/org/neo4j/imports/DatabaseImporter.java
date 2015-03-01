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
import java.util.concurrent.ArrayBlockingQueue;
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

        ArrayBlockingQueue<InputNode> nodes = new ArrayBlockingQueue<InputNode>(1_000_000);
        ArrayBlockingQueue<InputRelationship> rels = new ArrayBlockingQueue<InputRelationship>(10_000_000);
        for (TableInfo table : tables) {
            ResultSet rs = DataReader.readTableData(conn, table);
            new Transformer().stream(table, rules,rs,nodes,rels);
        }
        importer.doImport(createInput(nodes,rels));
    }

    private Input createInput(final BlockingQueue<InputNode> nodes, final BlockingQueue<InputRelationship> rels) {
        return new Input() {
            @Override
            public InputIterable<InputNode> nodes() {
                return new QueueInputIterable(nodes);
            }

            @Override
            public InputIterable<InputRelationship> relationships() {
                return new InputIterable<InputRelationship>() {
                    @Override
                    public InputIterator<InputRelationship> iterator() {
                        return new InputIterator<InputRelationship>() {
                            long position = 0;
                            @Override
                            public long position() {
                                return position++;
                            }

                            @Override
                            public void close() {
//                                Thread.currentThread().interrupt();
                            }

                            @Override
                            public boolean hasNext() {
                                return !rels.isEmpty();
                            }

                            @Override
                            public InputRelationship next() {
                                try {
                                    return rels.take();
                                } catch (InterruptedException e) {
                                    Thread.currentThread().interrupt();
                                    throw new RuntimeException(e);
                                }
                            }
                        };
                    }
                };
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
        private final BlockingQueue<T> queue;

        public QueueInputIterable(BlockingQueue<T> queue) {
            this.queue = queue;
        }

        @Override
        public InputIterator<T> iterator() {
            return new InputIterator<T>() {
                long position;
                T element;
                boolean hasNext;

                private void nextElement() {
                    try {
                        element = queue.take();
                        position ++;
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
                    return hasNext;
                }

                @Override
                public T next() {
                    nextElement();
                    return element;
                }
            };
        }
    }
}
