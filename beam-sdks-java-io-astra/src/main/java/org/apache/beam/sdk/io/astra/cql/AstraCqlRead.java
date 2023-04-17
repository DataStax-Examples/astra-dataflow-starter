package org.apache.beam.sdk.io.astra.cql;

import com.datastax.driver.core.Session;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.io.astra.AstraIO;
import org.apache.beam.sdk.io.astra.Mapper;
import org.apache.beam.sdk.io.astra.RingRange;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;

import java.io.File;
import java.net.URL;
import java.util.Set;

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;

/**
 * A {@link PTransform} to read data from Apache Cassandra. See {@link AstraCqlIO} for more
 *
 * information on usage and configuration.
 */
public class AstraCqlRead<T> extends PTransform<PBegin, PCollection<T>> {

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

    ValueProvider<String> keyspace;

    ValueProvider<String> table;

    ValueProvider<String> query;

    ValueProvider<String> consistencyLevel;

    Class<T> entity;

    Coder<T> coder;

    ValueProvider<Integer> minNumberOfSplits;

    SerializableFunction<Session, Mapper> mapperFactoryFn;

    ValueProvider<Set<RingRange>> ringRanges;

    /**
     * Create builder.
     * @return
     */
    public static final AstraCqlReadBuilder builder() {
        return new AstraCqlReadBuilder();
    }

    /** {@inheritDoc}. */
    @Override
    public PCollection<T> expand(PBegin input) {
        PCollection<AstraCqlRead<T>> splits = input
                        .apply(Create.of(this))
                        .apply("Create Splits", ParDo.of(new CqlSplitFn<T>()))
                        .setCoder(SerializableCoder.of(new TypeDescriptor<AstraCqlRead<T>>() {}));
        return splits.apply("ReadAll", new AstraCqlReadAll<T>(getCoder()));
    }

    /**
     * Update the ring ranges.
     *
     * @param ranges
     *      list of ranges
     * @return
     *      current reference
     */
    public AstraCqlRead<T> withRingRanges(Set<RingRange> ranges) {
        this.ringRanges = ValueProvider.StaticValueProvider.of(ranges);
        return this;
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
     * Gets table
     *
     * @return value of table
     */
    public ValueProvider<String> getTable() {
        return table;
    }

    /**
     * Gets query
     *
     * @return value of query
     */
    public ValueProvider<String> getQuery() {
        return query;
    }

    /**
     * Gets consistencyLevel
     *
     * @return value of consistencyLevel
     */
    public ValueProvider<String> getConsistencyLevel() {
        return consistencyLevel;
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
     * Gets coder
     *
     * @return value of coder
     */
    public Coder<T> getCoder() {
        return coder;
    }

    /**
     * Gets minNumberOfSplits
     *
     * @return value of minNumberOfSplits
     */
    public ValueProvider<Integer> getMinNumberOfSplits() {
        return minNumberOfSplits;
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
     * Gets ringRanges
     *
     * @return value of ringRanges
     */
    public ValueProvider<Set<RingRange>> getRingRanges() {
        return ringRanges;
    }
}
