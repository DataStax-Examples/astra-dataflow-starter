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
package com.datastax.astra.beam;

import com.datastax.astra.beam.lang.LanguageCode;
import com.datastax.astra.beam.lang.LanguageCodeDaoMapperFactoryFn;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.astra.db.AstraDbIO;
import org.apache.beam.sdk.io.astra.db.options.AstraDbWriteOptions;
import org.apache.beam.sdk.io.astra.db.transforms.RunCqlQueryFn;
import org.apache.beam.sdk.io.astra.db.utils.AstraSecureConnectBundleUtils;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Load a CSV File into Astra Table.
 **/

/*
 [Get your scb]
 ----------------
 astra db download-scb beam_integration_test_scb -f /Users/cedricklunven/Downloads/beam_integration_test_scb.zip

 [Set up your env variables]
 -----------------------------
 export ASTRA_TOKEN=`astra token`
 export ASTRA_SCB_PATH=/Users/cedricklunven/Downloads/beam_integration_test_scb.zip
 export ASTRA_KEYSPACE=demo

 [Run the pipeline]
 -----------------------------
 mvn clean compile exec:java \
 -Dexec.mainClass=com.datastax.astra.beam.Csv_to_AstraDb \
 -Dexec.args="\
 --astraToken=${ASTRA_TOKEN} \
 --astraSecureConnectBundle=${ASTRA_SCB_PATH} \
 --astraKeyspace=${ASTRA_KEYSPACE} \
 --csvInput=`pwd`/src/test/resources/language-codes.csv"

 */
public class Csv_to_AstraDb {

  /** Logger. */
  private static final Logger LOG = LoggerFactory.getLogger(Csv_to_AstraDb.class);

  /**
   * Interface definition of parameters needed for this pipeline.
   */
  public interface CsvToAstraDbOptions extends AstraDbWriteOptions {

    @Validation.Required
    @Description("Path of file to read from")
    String getCsvInput();

    @SuppressWarnings("unused")
    void setCsvInput(String csvFile);

  }

  /**
   * Main execution
   */
  public static void main(String[] args) {
    // Parse and Validate Parameters
    CsvToAstraDbOptions options = PipelineOptionsFactory
            .fromArgs(args).withValidation()
            .as(CsvToAstraDbOptions.class);

    // Load Secure Bundle from Local File System
    byte[] scbZip = AstraSecureConnectBundleUtils
            .loadFromFilePath(options.getAstraSecureConnectBundle());

    long top = System.currentTimeMillis();
    try {
      LOG.info("Parameters validations is successful, launching pipeline");
      Pipeline pipelineWrite = Pipeline.create(options);
      pipelineWrite
              // Read a CSV
              .apply(TextIO.read().from(options.getCsvInput()))

              // Convert each CSV row to a LanguageCode bean
              .apply("Convert To LanguageCode", ParDo.of(new CsvToLanguageCodeFn()))

              // Single Operation perform in the constructor of PTransform
              .apply("Create Destination Table", new RunCqlQueryFn<>(options.getAstraToken(),
                      scbZip, options.getAstraKeyspace(), LanguageCode.cqlCreateTable()))

              // Insert Results Into Astra
              .apply("Write Into Astra", AstraDbIO.<LanguageCode>write()
                      .withToken(options.getAstraToken())
                      .withSecureConnectBundle(scbZip)
                      .withKeyspace(options.getAstraKeyspace())
                      .withMapperFactoryFn(new LanguageCodeDaoMapperFactoryFn())
                      .withEntity(LanguageCode.class));

      pipelineWrite.run().waitUntilFinish();

    } finally {
        LOG.info("Pipeline finished in {} millis", System.currentTimeMillis()-top);
        AstraDbIO.close();
    }
  }

  /**
   * Csv => Bean
   */
  private static class CsvToLanguageCodeFn extends DoFn<String, LanguageCode> {

    @ProcessElement
    public void processElement(@Element String row, OutputReceiver<LanguageCode> receiver) {
      receiver.output(LanguageCode.fromCsv(row));
    }
  }

}
