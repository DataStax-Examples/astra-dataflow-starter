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
import com.dtx.astra.pipelines.utils.AstraPipelineOptions;
import com.dtx.astra.pipelines.utils.AstraIOTestUtils;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.astra.AstraIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.URL;
import java.nio.channels.Channels;

/**
 * A starter example for writing Beam programs.
 *
 * <p>The example takes two strings, converts them to their upper-case
 * representation and logs them.
 *
 * <p>To run this starter example locally using DirectRunner, just
 * execute it without any additional parameters from your favorite development
 * environment.
 *
 * <p>To run this starter example using managed resource in Google Cloud
 * Platform, you should specify the following command-line options:
 *   --project=<YOUR_PROJECT_ID>
 *   --stagingLocation=<STAGING_LOCATION_IN_CLOUD_STORAGE>
 *   --runner=DataflowRunner
 */
public class LoadStaticDataIntoAstraDataflow {

  /**
   * Logger for the class.
   */
  private static final Logger LOGGER = LoggerFactory.getLogger(LoadStaticDataIntoAstraDataflow.class);

  /**
   * Main.
   *
   * @param args
   */
  public static void main(String[] args) {
    LOGGER.info("Starting Pipeline");
    AstraPipelineOptions astraOptions = PipelineOptionsFactory
            .fromArgs(args)
            .withValidation()
            .as(AstraPipelineOptions.class);
    Pipeline pipelineWrite = Pipeline.create(astraOptions);
    FileSystems.setDefaultPipelineOptions(astraOptions);

    AstraIO.Write<SimpleDataEntity> writeToAstra = AstraIO.<SimpleDataEntity>write()
            .withToken(astraOptions.getToken())
            .withKeyspace(astraOptions.getKeyspace())
            .withEntity(SimpleDataEntity.class);

    try {
      if (astraOptions.getSecureConnectBundle().startsWith("gs")) {
        writeToAstra= writeToAstra.withSecureConnectBundleStream(Channels
                .newInputStream(FileSystems.open(
                        FileSystems.matchNewResource(astraOptions.getSecureConnectBundle(), false))));
        System.out.println("STREAM");
      } else if (astraOptions.getSecureConnectBundle().startsWith("http")) {
        System.out.println("HTTP");
        writeToAstra= writeToAstra.withSecureConnectBundleURL(new URL(astraOptions.getSecureConnectBundle()));
      } else {
        System.out.println("DEFAULT");
        writeToAstra= writeToAstra.withSecureConnectBundleFile(new File(astraOptions.getSecureConnectBundle()));
      }
    } catch(Exception e) {
      throw new IllegalStateException("Cannot load secure connect bundle", e);
    }

    pipelineWrite.apply(Create.of(AstraIOTestUtils.generateTestData(100)))
                 .apply(writeToAstra);
    pipelineWrite.run().waitUntilFinish();

    /*
    p.apply(Create.of("Hello", "World"))
     .apply(MapElements.via(new SimpleFunction<String, String>() {
      @Override
      public String apply(String input) {
        return input.toUpperCase();
      }
    }))
     .apply(ParDo.of(new DoFn<String, Void>() {
      @ProcessElement
      public void processElement(ProcessContext c)  {
        LOG.info(c.element());
      }
    }));*/

  }
}
