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

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.astra.db.AstraDbConnectionManager;
import org.apache.beam.sdk.io.astra.db.AstraDbIO;
import org.apache.beam.sdk.io.astra.db.options.AstraDbWriteOptions;
import org.apache.beam.sdk.io.astra.db.transforms.AstraCqlQueryPTransform;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;

import java.io.File;

/**
 * Load a CSV File into Astra Table.
 *

 mvn clean compile exec:java \
 -Dexec.mainClass=com.datastax.astra.beam.Csv_to_AstraDb \
 -Dexec.args="\
 --astraToken=${ASTRA_TOKEN} \
 --astraSecureConnectBundle=${ASTRA_SCB_PATH} \
 --keyspace=${ASTRA_KEYSPACE} \
 --csvInput=`pwd`/src/test/resources/language-codes.csv"

 */
public class Csv_to_AstraDb {

  /**
   * Interface definition of parameters needed for this pipeline.
   */
  public interface CsvToAstraDbOptions extends AstraDbWriteOptions {

    // --- csvInput --

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

    Pipeline pipelineWrite = Pipeline.create(options);

    try {
      pipelineWrite

              // Read a CSV
              .apply(TextIO.read().from(options.getCsvInput()))

              // Convert each CSV row to a LanguageCode bean
              .apply("Convert To LanguageCode", ParDo.of(new MapCsvLineAsRecord()))

              // Single Operation perform in the constructor of PTransform
              .apply("Create Destination Table",
                      new AstraCqlQueryPTransform<>(options,LanguageCode.cqlCreateTable()))

              // Insert Results Into Astra
              .apply("Write Into Astra", AstraDbIO.<LanguageCode>write()
                      .withToken(options.getAstraToken())
                      .withSecureConnectBundle(new File(options.getAstraSecureConnectBundle()))
                      .withKeyspace(options.getKeyspace())
                      .withEntity(LanguageCode.class));

      pipelineWrite.run().waitUntilFinish();
    } finally {
      AstraDbConnectionManager.cleanup();
    }
  }

  /**
   * Csv => Bean
   */
  private static class MapCsvLineAsRecord extends DoFn<String, LanguageCode> {
    @ProcessElement
    public void processElement(@Element String row, OutputReceiver<LanguageCode> receiver) {
      String[] fields = row.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
      receiver.output(new LanguageCode(fields[0], fields[1]));
    }
  }

}
