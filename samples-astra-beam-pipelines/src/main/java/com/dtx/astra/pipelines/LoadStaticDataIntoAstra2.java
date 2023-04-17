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
package com.dtx.astra.pipelines;

import com.dtx.astra.pipelines.domain.SimpleDataEntity;
import com.dtx.astra.pipelines.utils.AstraIOTestUtils;
import com.dtx.astra.pipelines.utils.AstraPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.astra.AstraIO;
import org.apache.beam.sdk.io.astra.cql.AstraCqlIO;
import org.apache.beam.sdk.io.astra.cql.AstraCqlWrite;
import org.apache.beam.sdk.io.astra.cql.AstraCqlWriteBuilder;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.URL;
import java.nio.channels.Channels;

/**
 * mvn -Pdirect-runner compile \
 *     exec:java \
 *     -Dexec.mainClass=com.dtx.astra.pipelines.LoadStaticDataIntoAstra \
 *     -Dexec.args="--keyspace=demo \
 *       --secureConnectBundle=/tmp/secure-connect-bundle-demo.zip \
 *       --token=AstraCS:token" \
 *     -Pdirect-runner
 */
public class LoadStaticDataIntoAstra2 {

  /**
   * Logger for the class.
   */
  private static final Logger LOGGER = LoggerFactory.getLogger(LoadStaticDataIntoAstra2.class);

  /**
   * Main.
   *
   * @param args
   */
  public static void main(String[] args) {
    LOGGER.info("Starting Pipeline");

    // Parse and Validate Parameters
    AstraPipelineOptions astraOptions = PipelineOptionsFactory
            .fromArgs(args)
            .withValidation()
            .as(AstraPipelineOptions.class);
    Pipeline pipelineWrite = Pipeline.create(astraOptions);
    FileSystems.setDefaultPipelineOptions(astraOptions);

    // Pipeline Definition
    pipelineWrite.apply(Create.of(AstraIOTestUtils.generateTestData(100)))
                 .apply(setupAstraWrite(astraOptions));

    // Pipeline Execution
    pipelineWrite.run().waitUntilFinish();

  }

  private static AstraCqlWrite<SimpleDataEntity> setupAstraWrite(AstraPipelineOptions astraOptions) {
    AstraCqlWriteBuilder<SimpleDataEntity> buider = AstraCqlIO.<SimpleDataEntity>write()
            .withKeyspace(astraOptions.getKeyspace())
            .withToken(astraOptions.getKeyspace());

    LOGGER.info("Looking for secure bundle");
    String scbParam = astraOptions.getSecureConnectBundle();
    try {
      if (scbParam.startsWith("gs")) {
        LOGGER.info("Secure bundle is in google cloud storage");
        //writeToAstra= buider.withSecureConnectBundleStream(Channels
        //        .newInputStream(FileSystems.open(FileSystems.matchNewResource(scbParam, false))));
      } else if (scbParam.startsWith("http")) {
        LOGGER.info("Secure bundle is accessible through URL");
        buider.withSecureConnectBundle(new URL(scbParam));
      } else {
        LOGGER.info("Secure bundle is on local file system");
        File scb = new File(scbParam);
        if (!scb.exists() || !scb.canRead()) {
          throw new IllegalArgumentException("Cannot find file " + scbParam + " on local system.");
        }
        buider.withSecureConnectBundle(new File(scbParam));
      }
    } catch(Exception e) {
      throw new IllegalStateException("Cannot load secure connect bundle", e);
    }
    return buider.build();
  }
}
