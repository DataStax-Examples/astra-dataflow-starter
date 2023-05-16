package com.datastax.astra.beam.dataflow;

import com.datastax.astra.beam.beam.BulkDataLoadWithBeam;
import com.datastax.astra.beam.utils.AstraIOTestUtils;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PlainTextAuthProvider;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SocketOptions;
import com.google.cloud.secretmanager.v1.SecretManagerServiceClient;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.Create;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;

/**
 * Reading secrets from GCP secret manager.
 *

 mvn -Pdataflow-runner compile exec:java \
 -Dexec.mainClass=com.dtx.astra.pipelines.ReadSecretFlow \
 -Dexec.args="\
 --astraToken=projects/747469159044/secrets/astra-token/versions/1 \
 --secureConnectBundle=projects/747469159044/secrets/cedrick-demo-scb/versions/1 \
 --runner=DataflowRunner \
 --project=integrations-379317 \
 --region=us-central1 \
 --gcpTempLocation=gs://dataflow-apache-quickstart_integrations-379317/temp/"

 *
 */
public class ReadSecretAndConnectDataFlow {

    /**
     * Logger for the class.
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(BulkDataLoadWithBeam.class);

    /**
     * Flow Interface
     */
    public interface MainPipelineOptions extends PipelineOptions {

        @Description("Location of Astra Token secret")
        @Validation.Required
        ValueProvider<String> getAstraToken();
        void setAstraToken(ValueProvider<String> token);

        @Description("Location of secret for secure connect bundle")
        @Validation.Required
        ValueProvider<String> getSecureConnectBundle();
        void setSecureConnectBundle(ValueProvider<String> path);
    }

    /**
     * Flow Code
     */
    public static void main(String[] args) throws IOException {
        LOGGER.info("Starting ReadSecretFlow");
        MainPipelineOptions options = PipelineOptionsFactory
              .fromArgs(args).withValidation()
              .as(MainPipelineOptions.class);

       // Read Secrets from Secret Manager
       SecretManagerServiceClient client = SecretManagerServiceClient.create();
       Cluster.Builder builder = Cluster.builder();
       String astraToken = client.accessSecretVersion(options.getAstraToken().get()).getPayload().getData().toStringUtf8();
       builder.withAuthProvider(new PlainTextAuthProvider("token", astraToken));
       if (astraToken != null) LOGGER.info("+ Token has been successfully retrieved");
       byte[] data = client.accessSecretVersion(options.getSecureConnectBundle().get()).getPayload().getData().toByteArray();
       if (data != null) LOGGER.info("+ Secure connect bundle retrieved");
       builder = builder.withCloudSecureConnectBundle(new ByteArrayInputStream(data));
       builder = builder.withSocketOptions(new SocketOptions().setConnectTimeoutMillis(20000).setReadTimeoutMillis(20000));

       // Test Credentials
       Cluster c = builder.build();
       try(Session session = c.connect("demo")) {
           LOGGER.info("+ Connected to Astra.");
           long rowCout = session.execute("SELECT COUNT(*) AS C FROM simpledata").one().getLong("C");
           LOGGER.info("+ Items int table simpledata:  {}", rowCout);
       }
       // Empty Pipeline
       Pipeline pipelineWrite = Pipeline.create(options);
       pipelineWrite.apply(Create.of(AstraIOTestUtils.generateTestData(100)));
       pipelineWrite.run().waitUntilFinish();
       LOGGER.info("SUCCESS");
    }
}
