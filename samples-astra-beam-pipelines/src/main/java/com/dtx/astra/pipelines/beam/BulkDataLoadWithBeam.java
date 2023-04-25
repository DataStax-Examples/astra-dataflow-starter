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
package com.dtx.astra.pipelines.beam;

import com.datastax.driver.core.*;
import com.datastax.driver.core.schemabuilder.SchemaBuilder;
import com.dtx.astra.pipelines.utils.AstraIOTestUtils;
import com.dtx.astra.pipelines.SimpleDataEntity;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.astra.AstraIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

/**
 * Load 100 records in an Astra Table.
 *
 * You must defined 3 Environment variables:
 *  - ASTRA_KEYSPACE, ASTRA_SCB_PATH and ASTRA_TOKEN
 *


 mvn -Pdirect-runner compile \
      exec:java \
      -Dexec.mainClass=com.dtx.astra.pipelines.beam.BulkDataLoadWithBeam \
      -Dexec.args="\
        --keyspace=${ASTRA_KEYSPACE} \
        --secureConnectBundle=${ASTRA_SCB_PATH} \
        --token${ASTRA_TOKEN}"
 */
public class BulkDataLoadWithBeam {

  /**
   * Logger for the class.
   */
  private static final Logger LOGGER = LoggerFactory.getLogger(BulkDataLoadWithBeam.class);

  /**
   * Interface definition of parameters needed for this pipeline
   */
  public interface LoadDataPipelineOptions extends PipelineOptions {

    @Description("The Zip file to secure the transport (secure connect bundle)")
    @Validation.Required
    String getSecureConnectBundle();
    void setSecureConnectBundle(String path);

    @Description("The token used as credentials (Astra Token)")
    @Validation.Required
    String getToken();
    void setToken(String token);

    @Description("Target Keyspace in the database")
    @Validation.Required
    String getKeyspace();
    void setKeyspace(String keyspace);
  }

  /**
   * Main.
   *
   * @param args
   */
  public static void main(String[] args) {
    LOGGER.info("Starting Pipeline");

    // Parse and Validate Parameters
    LoadDataPipelineOptions astraOptions = PipelineOptionsFactory
            .fromArgs(args)
            .withValidation()
            .as(LoadDataPipelineOptions.class);
    Pipeline pipelineWrite = Pipeline.create(astraOptions);
    FileSystems.setDefaultPipelineOptions(astraOptions);

    // Pipeline Definition
    pipelineWrite.apply(Create.of(AstraIOTestUtils.generateTestData(100)))
                 .apply(new CreateTableTransform<SimpleDataEntity>(astraOptions))
                 .apply(AstraIO.<SimpleDataEntity>write()
                         .withToken(astraOptions.getToken())
                         .withKeyspace(astraOptions.getKeyspace())
                         .withSecureConnectBundle(new File(astraOptions.getSecureConnectBundle()))
                         .withEntity(SimpleDataEntity.class));

    // Pipeline Execution
    pipelineWrite.run().waitUntilFinish();
  }

  /**
   * Transform in CSV lines.
   */
  private static class CreateTableTransform<T> extends PTransform<PCollection<T>, PCollection<T>> {

    /**
     * Astra options.
     *
     * @param astraOptions
     *    options
     */
    public CreateTableTransform(LoadDataPipelineOptions astraOptions) {
      // Create Target Table if needed
      try(Cluster cluster = Cluster.builder()
              .withAuthProvider(new PlainTextAuthProvider("token", astraOptions.getToken()))
              .withCloudSecureConnectBundle(new File(astraOptions.getSecureConnectBundle()))
              .withSocketOptions(new SocketOptions().setConnectTimeoutMillis(20000).setReadTimeoutMillis(20000))
              .build()) {
        try(Session session = cluster.connect(astraOptions.getKeyspace())) {
          LOGGER.info("+ Connected to Astra (pre-check)");
          session.execute(SchemaBuilder.createTable("simpledata")
                  .addPartitionKey("id", DataType.smallint())
                  .addColumn("data", DataType.text())
                  .ifNotExists());
          LOGGER.info("+ Table has been created (if needed)");
        }
      }
    }

    @Override
    public PCollection<T> expand(PCollection<T> input) {
      return input;
    }

  }
}
