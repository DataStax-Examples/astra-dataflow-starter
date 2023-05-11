package org.apache.beam.sdk.io.astra;

import com.datastax.driver.core.ConsistencyLevel;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

/**
 * Execute CQL Against Astra.
 *
 * @param <T>
 *      current bean
 */
public class AstraCqlQueryPTransform<T> extends PTransform<PCollection<T>, PCollection<T>> {

    /**
     * Logger
     */
    private static final Logger LOG = LoggerFactory.getLogger(AstraConnectionManager.class);

    /**
     * Execute a CQL query
     *
     * @param token
     * @param secureConnectBundle
     * @param keyspace
     */
    public AstraCqlQueryPTransform(String token, File secureConnectBundle, String keyspace, String cql) {
        LOG.info("Executing CQL: {}", cql);
        AstraConnectionManager
                .getInstance()
                .getSession(
                        ValueProvider.StaticValueProvider.of(token),
                        ValueProvider.StaticValueProvider.of(ConsistencyLevel.LOCAL_QUORUM.name()),
                        ValueProvider.StaticValueProvider.of(20000),
                        ValueProvider.StaticValueProvider.of(20000),
                        ValueProvider.StaticValueProvider.of(secureConnectBundle),
                        null,
                        keyspace)
                .execute(cql);
    }

    /**
     * Execute a CQL query
     *
     * @param token
     * @param secureConnectBundle
     * @param keyspace
     */
    public AstraCqlQueryPTransform(String token, byte[] secureConnectBundle, String keyspace, String cql) {
        LOG.info("Executing CQL: {}", cql);
        AstraConnectionManager
                .getInstance()
                .getSession(
                        ValueProvider.StaticValueProvider.of(token),
                        ValueProvider.StaticValueProvider.of(ConsistencyLevel.LOCAL_QUORUM.name()),
                        ValueProvider.StaticValueProvider.of(20000),
                        ValueProvider.StaticValueProvider.of(20000),
                        null,
                        ValueProvider.StaticValueProvider.of(secureConnectBundle), keyspace)
                .execute(cql);
    }

    @Override
    public PCollection<T> expand(PCollection<T> input) {
        return input;
    }

}
