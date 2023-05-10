/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.sdk.io.astra;

import com.datastax.driver.core.*;
import com.google.auto.value.AutoValue;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkState;

/**
 * An IO to read and write from/to Astra.
 */
@Experimental(Kind.SOURCE_SINK)
public class AstraIO {

  /**
   * Work with CQL and Astra.
   */
  private static final Logger LOG = LoggerFactory.getLogger(AstraIO.class);

  /**
   * Hidding default constructor.
   */
  private AstraIO() {}

  /** Provide a {@link Read} {@link PTransform} to read data from a Cassandra database. */
  public static <T> Read<T> read() {
    return new AutoValue_AstraIO_Read.Builder<T>().build();
  }

  /** Provide a {@link ReadAll} {@link PTransform} to read data from a Cassandra database. */
  public static <T> ReadAll<T> readAll() {
    return new AutoValue_AstraIO_ReadAll.Builder<T>().build();
  }

  /** Provide a {@link Write} {@link PTransform} to write data to a Cassandra database. */
  public static <T> Write<T> write() {
    return Write.<T>builder(MutationType.WRITE).build();
  }

  /** Provide a {@link Write} {@link PTransform} to delete data to a Cassandra database. */
  public static <T> Write<T> delete() {
    return Write.<T>builder(MutationType.DELETE).build();
  }

  /**
   * A {@link PTransform} to read data from Apache Cassandra. See {@link AstraIO} for more
   * information on usage and configuration.
   */
  @AutoValue
  @AutoValue.CopyAnnotations
  @SuppressWarnings({"rawtypes"})
  public abstract static class Read<T> extends PTransform<PBegin, PCollection<T>> {

    abstract @Nullable ValueProvider<String> keyspace();

    abstract @Nullable ValueProvider<String> table();

    abstract @Nullable ValueProvider<String> query();

    abstract @Nullable Class<T> entity();

    abstract @Nullable Coder<T> coder();

    abstract @Nullable ValueProvider<String> token();

    abstract @Nullable ValueProvider<String> consistencyLevel();

    abstract @Nullable ValueProvider<Integer> minNumberOfSplits();

    abstract @Nullable ValueProvider<Integer> connectTimeout();

    abstract @Nullable ValueProvider<Integer> readTimeout();

    abstract @Nullable ValueProvider<File> secureConnectBundle();

    abstract @Nullable ValueProvider<byte[]> secureConnectBundleData();

    abstract @Nullable SerializableFunction<Session, Mapper> mapperFactoryFn();

    @Nullable
    abstract ValueProvider<Set<RingRange>> ringRanges();

    abstract Builder<T> builder();

    /** Specify the Cassandra keyspace where to read data. */
    public Read<T> withKeyspace(String keyspace) {
      checkArgument(keyspace != null, "keyspace can not be null");
      return withKeyspace(ValueProvider.StaticValueProvider.of(keyspace));
    }

    /** Specify the Cassandra keyspace where to read data. */
    public Read<T> withKeyspace(ValueProvider<String> keyspace) {
      return builder().setKeyspace(keyspace).build();
    }

    /** Specify the Cassandra table where to read data. */
    public Read<T> withTable(String table) {
      checkArgument(table != null, "table can not be null");
      return withTable(ValueProvider.StaticValueProvider.of(table));
    }

    /** Specify the Cassandra table where to read data. */
    public Read<T> withTable(ValueProvider<String> table) {
      return builder().setTable(table).build();
    }

    /** Specify the query to read data. */
    public Read<T> withQuery(String query) {
      checkArgument(query != null && query.length() > 0, "query cannot be null");
      return withQuery(ValueProvider.StaticValueProvider.of(query));
    }

    /** Specify the query to read data. */
    public Read<T> withQuery(ValueProvider<String> query) {
      return builder().setQuery(query).build();
    }

    /**
     * Specify the entity class (annotated POJO). The {@link AstraIO} will read the data and
     * convert the data as entity instances. The {@link PCollection} resulting from the read will
     * contains entity elements.
     */
    public Read<T> withEntity(Class<T> entity) {
      checkArgument(entity != null, "entity can not be null");
      return builder().setEntity(entity).build();
    }

    /** Specify the {@link Coder} used to serialize the entity in the {@link PCollection}. */
    public Read<T> withCoder(Coder<T> coder) {
      checkArgument(coder != null, "coder can not be null");
      return builder().setCoder(coder).build();
    }

    /** Specify the token used for authentication. */
    public Read<T> withToken(String token) {
      checkArgument(token != null, "token can not be null");
      return withToken(ValueProvider.StaticValueProvider.of(token));
    }

    /** Specify the token used for authentication. */
    public Read<T> withToken(ValueProvider<String> token) {
      return builder().setToken(token).build();
    }

    /** Specify the consistency level for the request (e.g. ONE, LOCAL_ONE, LOCAL_QUORUM, etc). */
    public Read<T> withConsistencyLevel(String consistencyLevel) {
      checkArgument(consistencyLevel != null, "consistencyLevel can not be null");
      return withConsistencyLevel(ValueProvider.StaticValueProvider.of(consistencyLevel));
    }

    /** Specify the consistency level for the request (e.g. ONE, LOCAL_ONE, LOCAL_QUORUM, etc). */
    public Read<T> withConsistencyLevel(ValueProvider<String> consistencyLevel) {
      return builder().setConsistencyLevel(consistencyLevel).build();
    }

    /**
     * It's possible that system.size_estimates isn't populated or that the number of splits
     * computed by Beam is still to low for Cassandra to handle it. This setting allows to enforce a
     * minimum number of splits in case Beam cannot compute it correctly.
     */
    public Read<T> withMinNumberOfSplits(Integer minNumberOfSplits) {
      checkArgument(minNumberOfSplits != null, "minNumberOfSplits can not be null");
      checkArgument(minNumberOfSplits > 0, "minNumberOfSplits must be greater than 0");
      return withMinNumberOfSplits(ValueProvider.StaticValueProvider.of(minNumberOfSplits));
    }

    /**
     * It's possible that system.size_estimates isn't populated or that the number of splits
     * computed by Beam is still to low for Cassandra to handle it. This setting allows to enforce a
     * minimum number of splits in case Beam cannot compute it correctly.
     */
    public Read<T> withMinNumberOfSplits(ValueProvider<Integer> minNumberOfSplits) {
      return builder().setMinNumberOfSplits(minNumberOfSplits).build();
    }

    /**
     * Specify the Cassandra client connect timeout in ms. See
     * https://docs.datastax.com/en/drivers/java/3.8/com/datastax/driver/core/SocketOptions.html#setConnectTimeoutMillis-int-
     */
    public Read<T> withConnectTimeout(Integer timeout) {
      checkArgument(timeout != null, "Connect timeout can not be null");
      checkArgument(timeout > 0, "Connect timeout must be > 0, but was: %s", timeout);
      return withConnectTimeout(ValueProvider.StaticValueProvider.of(timeout));
    }

    /**
     * Specify the Cassandra client connect timeout in ms. See
     * https://docs.datastax.com/en/drivers/java/3.8/com/datastax/driver/core/SocketOptions.html#setConnectTimeoutMillis-int-
     */
    public Read<T> withConnectTimeout(ValueProvider<Integer> timeout) {
      return builder().setConnectTimeout(timeout).build();
    }

    /**
     * Specify the Cassandra client read timeout in ms. See
     * https://docs.datastax.com/en/drivers/java/3.8/com/datastax/driver/core/SocketOptions.html#setReadTimeoutMillis-int-
     */
    public Read<T> withReadTimeout(Integer timeout) {
      checkArgument(timeout != null, "Read timeout can not be null");
      checkArgument(timeout > 0, "Read timeout must be > 0, but was: %s", timeout);
      return withReadTimeout(ValueProvider.StaticValueProvider.of(timeout));
    }

    /**
     * Specify the Cassandra client read timeout in ms. See
     * https://docs.datastax.com/en/drivers/java/3.8/com/datastax/driver/core/SocketOptions.html#setReadTimeoutMillis-int-
     */
    public Read<T> withReadTimeout(ValueProvider<Integer> timeout) {
      return builder().setReadTimeout(timeout).build();
    }

    /**
     * A factory to create a specific {@link Mapper} for a given Cassandra Session. This is useful
     * to provide mappers that don't rely in Cassandra annotated objects.
     */
    public Read<T> withMapperFactoryFn(SerializableFunction<Session, Mapper> mapperFactory) {
      checkArgument(
          mapperFactory != null,
          "CassandraIO.withMapperFactory" + "(withMapperFactory) called with null value");
      return builder().setMapperFactoryFn(mapperFactory).build();
    }

    public Read<T> withRingRanges(Set<RingRange> ringRange) {
      return withRingRanges(ValueProvider.StaticValueProvider.of(ringRange));
    }

    public Read<T> withRingRanges(ValueProvider<Set<RingRange>> ringRange) {
      return builder().setRingRanges(ringRange).build();
    }

    /**
     * Populate SCB as a file.
     *
     * @param scbFile
     *    secure connect bundle file
     * @return
     *    reference to READ
     */
    public Read<T> withSecureConnectBundle(File scbFile) {
      checkArgument(scbFile != null, "keyspace can not be null");
      return withSecureConnectBundle(ValueProvider.StaticValueProvider.of(scbFile));
    }

    /** Specify the Cassandra keyspace where to read data. */
    public Read<T> withSecureConnectBundle(ValueProvider<File> cloudSecureConnectBundleFile) {
      checkArgument(cloudSecureConnectBundleFile != null, "keyspace can not be null");
      return builder().setSecureConnectBundle(cloudSecureConnectBundleFile).build();
    }

    /** Specify the Cassandra keyspace where to read data. */
    public Read<T> withSecureConnectBundleData(byte[] scbBytes) {
      checkArgument(scbBytes != null, "SCB url cannot be null");
      return withSecureConnectBundleData(ValueProvider.StaticValueProvider.of(scbBytes));
    }

    /** Specify the Cassandra keyspace where to read data. */
    public Read<T> withSecureConnectBundleData(ValueProvider<byte[]> scbBytes) {
      checkArgument(scbBytes != null, "SCB url cannot be null");
      return builder().setSecureConnectBundleData(scbBytes).build();
    }

    /**
     *
     * @param input
     * @return
     */
    @Override
    public PCollection<T> expand(PBegin input) {
      checkArgument(token() != null, "withToken() is required");
      checkArgument(keyspace() != null, "withKeyspace() is required");
      checkArgument(table() != null || query() != null, "table() or query() is required");
      checkArgument(entity() != null, "withEntity() is required");
      checkArgument(coder() != null, "withCoder() is required");
      checkArgument(secureConnectBundle() != null || secureConnectBundleData()!= null, "secure connect bundle is required");
      PCollection<Read<T>> splits =
          input
              .apply(Create.of(this))
              .apply("Create Splits", ParDo.of(new SplitFn<T>()))
              .setCoder(SerializableCoder.of(new TypeDescriptor<Read<T>>() {}));
      return splits.apply("ReadAll", org.apache.beam.sdk.io.astra.AstraIO.<T>readAll().withCoder(coder()));
    }

    private static class SplitFn<T> extends DoFn<Read<T>, Read<T>> {
      @ProcessElement
      public void process(
              @Element AstraIO.Read<T> read, OutputReceiver<Read<T>> outputReceiver) {
        Set<RingRange> ringRanges = getRingRanges(read);
        for (RingRange rr : ringRanges) {
          outputReceiver.output(read.withRingRanges(ImmutableSet.of(rr)));
        }
      }

      private static <T> Set<RingRange> getRingRanges(Read<T> read) {
        try (Cluster cluster =
            getCluster(
                read.token(),
                read.consistencyLevel(),
                read.connectTimeout(),
                read.readTimeout(),
                read.secureConnectBundle(),
                read.secureConnectBundleData())) {

            Integer splitCount;
            if (read.minNumberOfSplits() != null && read.minNumberOfSplits().get() != null) {
              splitCount = read.minNumberOfSplits().get();
            } else {
              splitCount = cluster.getMetadata().getAllHosts().size();
            }
            List<BigInteger> tokens =
                cluster.getMetadata().getTokenRanges().stream()
                    .map(tokenRange -> new BigInteger(tokenRange.getEnd().getValue().toString()))
                    .collect(Collectors.toList());
            SplitGenerator splitGenerator =
                new SplitGenerator(cluster.getMetadata().getPartitioner());

            return splitGenerator.generateSplits(splitCount, tokens).stream()
                .flatMap(List::stream)
                .collect(Collectors.toSet());
        }
      }
    }

    @AutoValue.Builder
    abstract static class Builder<T> {

      abstract Builder<T> setToken(ValueProvider<String> token);

      abstract Builder<T> setKeyspace(ValueProvider<String> keyspace);

      abstract Builder<T> setTable(ValueProvider<String> table);

      abstract Builder<T> setQuery(ValueProvider<String> query);

      abstract Builder<T> setEntity(Class<T> entity);

      abstract Optional<Class<T>> entity();

      abstract Builder<T> setCoder(Coder<T> coder);

      abstract Builder<T> setConsistencyLevel(ValueProvider<String> consistencyLevel);

      abstract Builder<T> setMinNumberOfSplits(ValueProvider<Integer> minNumberOfSplits);

      abstract Builder<T> setConnectTimeout(ValueProvider<Integer> timeout);

      abstract Builder<T> setReadTimeout(ValueProvider<Integer> timeout);

      abstract Builder<T> setMapperFactoryFn(SerializableFunction<Session, Mapper> mapperFactoryFn);

      abstract Optional<SerializableFunction<Session, Mapper>> mapperFactoryFn();

      abstract Builder<T> setRingRanges(ValueProvider<Set<RingRange>> ringRange);

      abstract Builder<T> setSecureConnectBundle(ValueProvider<File> scbFile);

      abstract Builder<T> setSecureConnectBundleData(ValueProvider<byte[]> scbStream);

      abstract Read<T> autoBuild();

      public Read<T> build() {
        if (!mapperFactoryFn().isPresent() && entity().isPresent()) {
          setMapperFactoryFn(new DefaultObjectMapperFactory(entity().get()));
        }
        return autoBuild();
      }
    }
  }

  /** Specify the mutation type: either write or delete. */
  public enum MutationType {
    WRITE,
    DELETE
  }

  /**
   * A {@link PTransform} to mutate into Apache Cassandra. See {@link AstraIO} for details on
   * usage and configuration.
   */
  @AutoValue
  @AutoValue.CopyAnnotations
  @SuppressWarnings({"rawtypes"})
  public abstract static class Write<T> extends PTransform<PCollection<T>, PDone> {

    abstract @Nullable ValueProvider<String> token();

    abstract @Nullable ValueProvider<File> secureConnectBundle();

    abstract @Nullable ValueProvider<byte[]> secureConnectBundleData();

    abstract @Nullable ValueProvider<String> keyspace();

    abstract @Nullable Class<T> entity();

    abstract MutationType mutationType();

    abstract @Nullable ValueProvider<Integer> connectTimeout();

    abstract @Nullable ValueProvider<Integer> readTimeout();

    abstract @Nullable SerializableFunction<Session, Mapper> mapperFactoryFn();

    abstract Builder<T> builder();

    static <T> Builder<T> builder(MutationType mutationType) {
      return new AutoValue_AstraIO_Write.Builder<T>().setMutationType(mutationType);
    }

    /** Specify the Cassandra keyspace where to write data. */
    public Write<T> withToken(String token) {
      checkArgument(
              token != null,
              "AstraIO."
                      + getMutationTypeName()
                      + "().withToken(token) called with "
                      + "null token");
      return withToken(ValueProvider.StaticValueProvider.of(token));
    }

    /** Specify the Cassandra keyspace where to read data. */
    public Write<T> withToken(ValueProvider<String> token) {
      return builder().setToken(token).build();
    }

    /** Specify the Cassandra keyspace where to write data. */
    public Write<T> withKeyspace(String keyspace) {
      checkArgument(
          keyspace != null,
          "CassandraIO."
              + getMutationTypeName()
              + "().withKeyspace(keyspace) called with "
              + "null keyspace");
      return withKeyspace(ValueProvider.StaticValueProvider.of(keyspace));
    }

    /** Specify the Cassandra keyspace where to read data. */
    public Write<T> withKeyspace(ValueProvider<String> keyspace) {
      return builder().setKeyspace(keyspace).build();
    }

    /**
     * Specify the entity class in the input {@link PCollection}. The {@link AstraIO} will map
     * this entity to the Cassandra table thanks to the annotations.
     */
    public Write<T> withEntity(Class<T> entity) {
      checkArgument(
          entity != null,
          "CassandraIO."
              + getMutationTypeName()
              + "().withEntity(entity) called with null "
              + "entity");
      return builder().setEntity(entity).build();
    }

    /** Cassandra client socket option for connect timeout in ms. */
    public Write<T> withConnectTimeout(Integer timeout) {
      checkArgument(
          (timeout != null && timeout > 0),
          "CassandraIO."
              + getMutationTypeName()
              + "().withConnectTimeout(timeout) called with invalid timeout "
              + "number (%s)",
          timeout);
      return withConnectTimeout(ValueProvider.StaticValueProvider.of(timeout));
    }

    /** Cassandra client socket option for connect timeout in ms. */
    public Write<T> withConnectTimeout(ValueProvider<Integer> timeout) {
      return builder().setConnectTimeout(timeout).build();
    }

    /** Cassandra client socket option to set the read timeout in ms. */
    public Write<T> withReadTimeout(Integer timeout) {
      checkArgument(
          (timeout != null && timeout > 0),
          "CassandraIO."
              + getMutationTypeName()
              + "().withReadTimeout(timeout) called with invalid timeout "
              + "number (%s)",
          timeout);
      return withReadTimeout(ValueProvider.StaticValueProvider.of(timeout));
    }

    /** Cassandra client socket option to set the read timeout in ms. */
    public Write<T> withReadTimeout(ValueProvider<Integer> timeout) {
      return builder().setReadTimeout(timeout).build();
    }

    // -------------------------------------------------
    // 3 ways to populate cloud secure bundle for WRITE
    // -------------------------------------------------

    /**
     * Populate SCB as a file.
     *
     * @param scbFile
     *    secure connect bundle file
     * @return
     *    reference to READ
     */
    public Write<T> withSecureConnectBundle(File scbFile) {
      checkArgument(scbFile != null, "keyspace can not be null");
      return withSecureConnectBundle(ValueProvider.StaticValueProvider.of(scbFile));
    }

    /** Specify the Cassandra keyspace where to read data. */
    public Write<T> withSecureConnectBundle(ValueProvider<File> cloudSecureConnectBundleFile) {
      checkArgument(cloudSecureConnectBundleFile != null, "keyspace can not be null");
      return builder().setSecureConnectBundle(cloudSecureConnectBundleFile).build();
    }

    /** Specify the Cassandra keyspace where to read data. */
    public Write<T> withSecureConnectBundleData(byte[] scbStream) {
      checkArgument(scbStream != null, "scbStream cannot be null");
      return withSecureConnectBundleData(ValueProvider.StaticValueProvider.of(scbStream));
    }

    /** Specify the Cassandra keyspace where to read data. */
    public Write<T> withSecureConnectBundleData(ValueProvider<byte[]> scbStream) {
      checkArgument(scbStream != null, "scbStream cannot be null");
      return builder().setSecureConnectBundleData(scbStream).build();
    }

    public Write<T> withMapperFactoryFn(SerializableFunction<Session, Mapper> mapperFactoryFn) {
      checkArgument(
          mapperFactoryFn != null,
          "AstraIO."
              + getMutationTypeName()
              + "().mapperFactoryFn"
              + "(mapperFactoryFn) called with null value");
      return builder().setMapperFactoryFn(mapperFactoryFn).build();
    }

    @Override
    public void validate(PipelineOptions pipelineOptions) {

      checkState(
              token() != null,
              "AstraIO."
                      + getMutationTypeName()
                      + "() requires a token to be set via "
                      + "withToken(token)");
      checkState(
          keyspace() != null,
          "AstraIO."
              + getMutationTypeName()
              + "() requires a keyspace to be set via "
              + "withKeyspace(keyspace)");
      checkState(
          entity() != null,
          "AstraIO."
              + getMutationTypeName()
              + "() requires an entity to be set via "
              + "withEntity(entity)");
    }

    @Override
    public PDone expand(PCollection<T> input) {
      if (mutationType() == MutationType.DELETE) {
        input.apply(ParDo.of(new DeleteFn<>(this)));
      } else {
        input.apply(ParDo.of(new WriteFn<>(this)));
      }
      return PDone.in(input.getPipeline());
    }

    private String getMutationTypeName() {
      return mutationType() == null
          ? MutationType.WRITE.name().toLowerCase()
          : mutationType().name().toLowerCase();
    }

    @AutoValue.Builder
    abstract static class Builder<T> {

      abstract Builder<T> setToken(ValueProvider<String> token);

      abstract Builder<T> setKeyspace(ValueProvider<String> keyspace);

      abstract Builder<T> setEntity(Class<T> entity);

      abstract Optional<Class<T>> entity();

      abstract Builder<T> setMutationType(MutationType mutationType);

      abstract Builder<T> setConnectTimeout(ValueProvider<Integer> timeout);

      abstract Builder<T> setReadTimeout(ValueProvider<Integer> timeout);

      abstract Builder<T> setMapperFactoryFn(SerializableFunction<Session, Mapper> mapperFactoryFn);

      /**
       * Setter for AutoValue to generate and populate the cloudSecureConnectBundlePath.
       */
      abstract Builder<T> setSecureConnectBundle(ValueProvider<File> scbFile);
      abstract Builder<T> setSecureConnectBundleData(ValueProvider<byte[]> scbStream);

      abstract Optional<SerializableFunction<Session, Mapper>> mapperFactoryFn();

      abstract Write<T> autoBuild(); // not public

      public Write<T> build() {

        if (!mapperFactoryFn().isPresent() && entity().isPresent()) {
          setMapperFactoryFn(new DefaultObjectMapperFactory(entity().get()));
        }
        return autoBuild();
      }
    }
  }

  private static class WriteFn<T> extends DoFn<T, Void> {
    private final Write<T> spec;
    private transient Mutator<T> writer;

    WriteFn(Write<T> spec) {
      this.spec = spec;
    }

    @Setup
    public void setup() {
      writer = new Mutator<>(spec, Mapper::saveAsync, "writes");
    }

    @ProcessElement
    public void processElement(ProcessContext c) throws ExecutionException, InterruptedException {
      writer.mutate(c.element());
    }

    @FinishBundle
    public void finishBundle() throws Exception {
      writer.flush();
    }

    @Teardown
    public void teardown() throws Exception {
      writer.close();
      writer = null;
    }
  }

  private static class DeleteFn<T> extends DoFn<T, Void> {
    private final Write<T> spec;
    private transient Mutator<T> deleter;

    DeleteFn(Write<T> spec) {
      this.spec = spec;
    }

    @Setup
    public void setup() {
      deleter = new Mutator<>(spec, Mapper::deleteAsync, "deletes");
    }

    @ProcessElement
    public void processElement(ProcessContext c) throws ExecutionException, InterruptedException {
      deleter.mutate(c.element());
    }

    @FinishBundle
    public void finishBundle() throws Exception {
      deleter.flush();
    }

    @Teardown
    public void teardown() throws Exception {
      deleter.close();
      deleter = null;
    }
  }

  /**
   * Get a Astra cluster using either hosts and port or cloudSecureBundle.
   *
   * @param token
   *    token (or clientSecret)
   * @param consistencyLevel
   *    consistency level
   * @param connectTimeout
   *    connection timeout
   * @param readTimeout
   *    read timeout
   * @param scbFile
   *    read scb as a file
   * @param scbStream
   *    read scb as stream
   * @return
   *    cassandra cluster
   */
  static Cluster getCluster(
          ValueProvider<String> token,
          ValueProvider<String> consistencyLevel,
          ValueProvider<Integer> connectTimeout,
          ValueProvider<Integer> readTimeout,
          ValueProvider<File> scbFile,
          ValueProvider<byte[]> scbStream) {

    Cluster.Builder builder = Cluster.builder();

    if (scbFile != null) {
      builder.withCloudSecureConnectBundle(scbFile.get());
    } else if (scbStream != null) {
      builder.withCloudSecureConnectBundle(new ByteArrayInputStream(scbStream.get()));
    } else {
      throw new IllegalArgumentException("Cloud Secure Bundle is Required");
    }
    builder.withAuthProvider(new PlainTextAuthProvider("token", token.get()));
    if (consistencyLevel != null) {
      builder.withQueryOptions(new QueryOptions().setConsistencyLevel(ConsistencyLevel.valueOf(consistencyLevel.get())));
    }

    SocketOptions socketOptions = new SocketOptions();
    builder.withSocketOptions(socketOptions);

    if (connectTimeout != null) {
      socketOptions.setConnectTimeoutMillis(connectTimeout.get());
    }

    if (readTimeout != null) {
      socketOptions.setReadTimeoutMillis(readTimeout.get());
    }
    Cluster cluster = builder.build();
    LOG.info("Connected to cluster: {}", cluster.getMetadata().getClusterName());
    return builder.build();
  }

  /** Mutator allowing to do side effects into Apache Cassandra database. */
  private static class Mutator<T> {
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

    Mutator(Write<T> spec, BiFunction<Mapper<T>, T, Future<Void>> mutator, String operationName) {
      this.cluster = getCluster(
                        spec.token(),
                        ValueProvider.StaticValueProvider.of(ConsistencyLevel.LOCAL_QUORUM.name()),
                        spec.connectTimeout(),
                        spec.readTimeout(),
                        spec.secureConnectBundle(),
                        spec.secureConnectBundleData());
      this.session = cluster.connect(spec.keyspace().get());
      this.mapperFactoryFn = spec.mapperFactoryFn();
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

  /**
   * A {@link PTransform} to read data from Apache Cassandra. See {@link AstraIO} for more
   * information on usage and configuration.
   */
  @AutoValue
  public abstract static class ReadAll<T> extends PTransform<PCollection<Read<T>>, PCollection<T>> {
    @AutoValue.Builder
    abstract static class Builder<T> {

      abstract Builder<T> setCoder(Coder<T> coder);

      abstract ReadAll<T> autoBuild();

      public ReadAll<T> build() {
        return autoBuild();
      }
    }

    @Nullable
    abstract Coder<T> coder();

    abstract Builder<T> builder();

    /** Specify the {@link Coder} used to serialize the entity in the {@link PCollection}. */
    public ReadAll<T> withCoder(Coder<T> coder) {
      checkArgument(coder != null, "coder can not be null");
      return builder().setCoder(coder).build();
    }

    @Override
    public PCollection<T> expand(PCollection<Read<T>> input) {
      checkArgument(coder() != null, "withCoder() is required");
      return input
          .apply("Reshuffle", Reshuffle.viaRandomKey())
          .apply("Read", ParDo.of(new ReadFn<>()))
          .setCoder(this.coder());
    }
  }

  /**
   * Check if the current partitioner is the Murmur3 (default in Cassandra version newer than 2).
   */
  @VisibleForTesting
  private static boolean isMurmur3Partitioner(Cluster cluster) {
    return MURMUR3PARTITIONER.equals(cluster.getMetadata().getPartitioner());
  }

  private static final String MURMUR3PARTITIONER = "org.apache.cassandra.dht.Murmur3Partitioner";
}
