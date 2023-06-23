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
package com.datastax.astra.dataflow;

import com.datastax.astra.dataflow.domains.LanguageCode;
import com.datastax.astra.dataflow.domains.LanguageCodeDaoMapperFactoryFn;
import com.datastax.astra.dataflow.utils.GoogleSecretManagerUtils;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.astra.db.AstraDbIO;
import org.apache.beam.sdk.io.astra.db.CqlSessionHolder;
import org.apache.beam.sdk.io.astra.db.options.AstraDbWriteOptions;
import org.apache.beam.sdk.io.astra.db.transforms.RunCqlQueryFn;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Load Data with Dataflow.
 *

 mvn compile exec:java \
 -Dexec.mainClass=com.datastax.astra.dataflow.Gcs_To_AstraDb \
 -Dexec.args="\
 --astraToken=projects/747469159044/secrets/astra-token/versions/2 \
 --astraSecureConnectBundle=projects/747469159044/secrets/secure-connect-bundle-demo/versions/2 \
 --astraKeyspace=samples_dataflow \
 --csvInput=gs://astra_dataflow_inputs/csv/language-codes.csv \
 --runner=DataflowRunner \
 --project=integrations-379317 \
 --region=us-central1"

 */
public class Gcs_To_AstraDb {

  /**
   * Logger for the class.
   */
  private static final Logger LOGGER = LoggerFactory.getLogger(Gcs_To_AstraDb.class);

  /**
   * Flow Interface
   */
  public interface GcsToAstraDbOptions extends AstraDbWriteOptions {

    // --- csvInput --

    @Validation.Required
    @Description("Path of file to read from a Bucket")
    String getCsvInput();

    @SuppressWarnings("unused")
    void setCsvInput(String csvFile);
  }

  /**
   * Main.
   */
  public static void main(String[] args) {
    LOGGER.info("Starting Pipeline");
    long top = System.currentTimeMillis();

    // Parsing Parameters
    GcsToAstraDbOptions options = PipelineOptionsFactory
            .fromArgs(args).withValidation()
            .as(GcsToAstraDbOptions.class);

    // The provided parameter is a resource ID.
    String astraToken = GoogleSecretManagerUtils.readTokenSecret(options.getAstraToken());
    byte[] astraSecureBundle = GoogleSecretManagerUtils.readSecureBundleSecret(options.getAstraSecureConnectBundle());
    LOGGER.info("+ Secrets Parsed after {} millis.", System.currentTimeMillis() - top);

    // Running Pipeline
    Pipeline pipelineWrite = Pipeline.create(options);

    try {
      pipelineWrite

              // Read a CSV
              .apply(TextIO.read().from(options.getCsvInput()))

              // Convert each CSV row to a LanguageCode bean
              .apply("Convert To LanguageCode", ParDo.of(new MapCsvLineAsRecord()))

              // Single Operation perform in the constructor of PTransform
              .apply("Create Destination Table", new RunCqlQueryFn<>(
                              astraToken, astraSecureBundle,
                              options.getAstraKeyspace(),
                              LanguageCode.cqlCreateTable()))

              // Insert Results Into Astra
              .apply("Write Into Astra", AstraDbIO.<LanguageCode>write()
                      .withToken(astraToken)                          // read from secret
                      .withSecureConnectBundle(astraSecureBundle) // read from secret
                      .withKeyspace(options.getAstraKeyspace())
                      .withMapperFactoryFn(new LanguageCodeDaoMapperFactoryFn())
                      .withEntity(LanguageCode.class));

      pipelineWrite.run().waitUntilFinish();
    } finally {
        CqlSessionHolder.cleanup();
    }
  }

  /**
   * Csv => Bean
   */
  private static class MapCsvLineAsRecord extends DoFn<String, LanguageCode> {
    @ProcessElement
    public void processElement(@Element String row, OutputReceiver<LanguageCode> receiver) {
      String[] fields = row.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
      System.out.println(row);
      receiver.output(new LanguageCode(fields[0], fields[1]));
    }
  }
}
