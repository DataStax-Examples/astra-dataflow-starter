package org.apache.beam.sdk.io.astra.cql;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import org.apache.beam.sdk.io.astra.Mapper;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.function.BiFunction;

/**
 * Mutator allowing to do side effects into Apache Cassandra database.
 **/
public class CqlMutator<T> {

    /** Logger. */
    private static final Logger LOG = LoggerFactory.getLogger(CqlMutator.class);

    /**
     * The threshold of 100 concurrent async queries is a heuristic commonly used by the Apache
     * Cassandra community. There is no real gain to expect in tuning this value.
     */
    private static final int CONCURRENT_ASYNC_QUERIES = 100;

    private final Cluster cluster;

    private final Session session;

    private final SerializableFunction<Session, Mapper> mapperFactoryFn;

    private List<Future<Void>> mutateFutures;

    private final BiFunction<Mapper<T>, T, Future<Void>> mutator;

    private final String operationName;

    /**
     * Operation to mutate Astra.
     *
     * @param cqlWrite
     *      writer
     * @param mutator
     *      operation to perform (write, delete)
     * @param operationName
     *      name of operation
     */
    public CqlMutator(AstraCqlWrite<T> cqlWrite, BiFunction<Mapper<T>, T, Future<Void>> mutator, String operationName) {
        this.cluster = CqlConnectionManager.getCluster(cqlWrite);
        this.session = CqlConnectionManager.getSession(cqlWrite);
        this.mapperFactoryFn = cqlWrite.getMapperFactoryFn();
        this.mutateFutures = new ArrayList<>();
        this.mutator = mutator;
        this.operationName = operationName;
    }

    /**
     * Mutate the entity to the Cassandra instance, using {@link Mapper} obtained with the Mapper
     * factory, the DefaultObjectMapperFactory uses {@link
     * com.datastax.driver.mapping.MappingManager}. This method uses {@link
     * Mapper#saveAsync(Object)} method, which is asynchronous. Beam will wait for all futures to
     * complete, to guarantee all writes have succeeded.
     */
    void mutate(T entity) throws ExecutionException, InterruptedException {
        Mapper<T> mapper = mapperFactoryFn.apply(session);

        this.mutateFutures.add(mutator.apply(mapper, entity));

        if (this.mutateFutures.size() == CONCURRENT_ASYNC_QUERIES) {
            // We reached the max number of allowed in flight queries.
            // Write methods are synchronous in Beam,
            // so we wait for each async query to return before exiting.
            LOG.debug(
                    "Waiting for a batch of {} Cassandra {} to be executed...",
                    CONCURRENT_ASYNC_QUERIES,
                    operationName);
            waitForFuturesToFinish();
            this.mutateFutures = new ArrayList<>();
        }
    }

    void flush() throws ExecutionException, InterruptedException {
        if (this.mutateFutures.size() > 0) {
            // Waiting for the last in flight async queries to return before finishing the bundle.
            waitForFuturesToFinish();
        }
    }

    void close() {
        if (session != null) {
            session.close();
        }
        if (cluster != null) {
            cluster.close();
        }
    }

    private void waitForFuturesToFinish() throws ExecutionException, InterruptedException {
        for (Future<Void> future : mutateFutures) {
            future.get();
        }
    }
}
