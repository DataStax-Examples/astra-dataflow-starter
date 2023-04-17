package org.apache.beam.sdk.io.astra.cql;

import com.datastax.driver.core.Session;
import org.apache.beam.sdk.io.astra.Mapper;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.URL;

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkState;

/**
 * Write Data Into Astra with Cql.
 *
 * @param <T>
 */
public class AstraCqlWrite<T> extends PTransform<PCollection<T>, PDone> {

    /** Logger. */
    private static final Logger LOG = LoggerFactory.getLogger(AstraCqlWrite.class);

    // ----- Connectivity ------

    /** Token. */
    ValueProvider<String> token;

    /** Secure connect bundle file. */
    ValueProvider<File> secureConnectBundleFile;
    ValueProvider<URL> secureConnectBundleUrl;

    /** Connect Timeout. */
    ValueProvider<Integer> connectTimeout;

    /** Read Timeout. */
    ValueProvider<Integer> readTimeout;

    // ----- Operations ------

    /** Keyspace. */
    ValueProvider<String> keyspace;

    /** Entity to be saved. */
    Class<T> entity;

    /** Object Mapping. */
    SerializableFunction<Session, Mapper> mapperFactoryFn;

    /** Write or DELETE. */
    MutationType mutationType = MutationType.WRITE;

    /** Mutation. */
    public enum MutationType {  WRITE, DELETE }

    /**
     * Hide Default constructor
     */
    protected AstraCqlWrite() {}

    /**
     * Create builder.
     * @return
     */
    public static final AstraCqlWriteBuilder builder() {
        return new AstraCqlWriteBuilder();
    }


    /** {@inheritDoc}. */
    @Override
    public void validate(PipelineOptions pipelineOptions) {
        checkState(!StringUtils.isEmpty(token.get()),
                "AstraCqlWrite requires a token to be set via withToken(token)");
        checkState(secureConnectBundleFile != null || secureConnectBundleUrl != null,
                "AstraCqlWrite requires a secure connect bundle to be set via withSecureConnectBundle(token)");
        checkState(!StringUtils.isEmpty(keyspace.get()),
                "AstraCqlWrite requires a keyspace to be set via withKeyspace(keyspace)");
        checkState(entity != null,
                "AstraCqlWrite requires an entity to be set via withEntity(entity)");
    }

    /** {@inheritDoc}. */
    @Override
    public PDone expand(PCollection<T> input) {
        if (mutationType == MutationType.DELETE) {
            input.apply(ParDo.of(new CqlDeleteFn<>(this)));
        } else {
            input.apply(ParDo.of(new CqlWriteFn<>(this)));
        }
        return PDone.in(input.getPipeline());
    }

    /**
     * Gets token
     *
     * @return value of token
     */
    public ValueProvider<String> getToken() {
        return token;
    }

    /**
     * Gets secureConnectBundleFile
     *
     * @return value of secureConnectBundleFile
     */
    public ValueProvider<File> getSecureConnectBundleFile() {
        return secureConnectBundleFile;
    }

    /**
     * Gets secureConnectBundleUrl
     *
     * @return value of secureConnectBundleUrl
     */
    public ValueProvider<URL> getSecureConnectBundleUrl() {
        return secureConnectBundleUrl;
    }

    /**
     * Gets connectTimeout
     *
     * @return value of connectTimeout
     */
    public ValueProvider<Integer> getConnectTimeout() {
        return connectTimeout;
    }

    /**
     * Gets readTimeout
     *
     * @return value of readTimeout
     */
    public ValueProvider<Integer> getReadTimeout() {
        return readTimeout;
    }

    /**
     * Gets keyspace
     *
     * @return value of keyspace
     */
    public ValueProvider<String> getKeyspace() {
        return keyspace;
    }

    /**
     * Gets entity
     *
     * @return value of entity
     */
    public Class<T> getEntity() {
        return entity;
    }

    /**
     * Gets mapperFactoryFn
     *
     * @return value of mapperFactoryFn
     */
    public SerializableFunction<Session, Mapper> getMapperFactoryFn() {
        return mapperFactoryFn;
    }

    /**
     * Gets mutationType
     *
     * @return value of mutationType
     */
    public MutationType getMutationType() {
        return mutationType;
    }
}
