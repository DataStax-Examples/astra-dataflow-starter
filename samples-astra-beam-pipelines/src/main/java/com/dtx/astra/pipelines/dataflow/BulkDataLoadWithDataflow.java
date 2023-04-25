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
package com.dtx.astra.pipelines.dataflow;

import com.dtx.astra.pipelines.SimpleDataEntity;
import com.dtx.astra.pipelines.utils.AstraIOTestUtils;
import com.google.cloud.secretmanager.v1.SecretManagerServiceClient;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.astra.AstraIO;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.Create;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Load Data with Dataflow.
 *

 mvn -Pdataflow-runner compile exec:java \
 -Dexec.mainClass=com.dtx.astra.pipelines.beam.dataflow.BulkDataLoadWithDataflow \
 -Dexec.args="\
 --astraToken=projects/747469159044/secrets/astra-token/versions/1 \
 --secureConnectBundle=projects/747469159044/secrets/cedrick-demo-scb/versions/1 \
 --keyspace=demo \
 --runner=DataflowRunner \
 --project=integrations-379317 \
 --region=us-central1"


 */
public class BulkDataLoadWithDataflow {

  /**
   * Logger for the class.
   */
  private static final Logger LOGGER = LoggerFactory.getLogger(BulkDataLoadWithDataflow.class);

  /**
   * Flow Interface
   */
  public interface LoadDataPipelineOptions extends PipelineOptions {

    @Description("Location of Astra Token secret")
    @Validation.Required
    String getAstraToken();
    void setAstraToken(String token);

    @Description("Location of secret for secure connect bundle")
    @Validation.Required
    String getSecureConnectBundle();
    void setSecureConnectBundle(String path);

    @Description("Source Keyspace")
    @Validation.Required
    String getKeyspace();
    void setKeyspace(String keyspace);
  }

  /**
   * Main.
   *
   * @param args
   */
  public static void main(String[] args) throws IOException {
    LOGGER.info("Starting Pipeline");
    // Parsing Parameters
    LoadDataPipelineOptions astraOptions = PipelineOptionsFactory
            .fromArgs(args)
            .withValidation()
            .as(LoadDataPipelineOptions.class);
    FileSystems.setDefaultPipelineOptions(astraOptions);

    // Extract Secrets from GCP
    SecretManagerServiceClient client = SecretManagerServiceClient.create();
    String astraToken = client
            .accessSecretVersion(astraOptions.getAstraToken())
            .getPayload().getData()
            .toStringUtf8();
    LOGGER.info("+ Token retrieved");
    byte[] astraSecureBundle = client
            .accessSecretVersion(astraOptions.getSecureConnectBundle())
            .getPayload().getData()
            .toByteArray();
    LOGGER.info("+ Secure connect bundle retrieved");

    // Running Pipeline
    Pipeline pipelineWrite = Pipeline.create(astraOptions);
    pipelineWrite.apply("Create 100 random items", Create.of(AstraIOTestUtils.generateTestData(100)))
                 .apply("Write into Astra", AstraIO.<SimpleDataEntity>write()
                         .withToken(astraToken)
                         .withKeyspace(astraOptions.getKeyspace())
                         .withSecureConnectBundleData(astraSecureBundle)
                         .withEntity(SimpleDataEntity.class));
    pipelineWrite.run().waitUntilFinish();
  }
}
