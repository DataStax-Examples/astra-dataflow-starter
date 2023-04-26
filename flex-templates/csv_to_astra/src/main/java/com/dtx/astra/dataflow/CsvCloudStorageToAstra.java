package com.dtx.astra.dataflow;

import com.datastax.driver.core.*;
import com.datastax.driver.core.schemabuilder.SchemaBuilder;
import com.google.cloud.secretmanager.v1.SecretManagerServiceClient;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.astra.AstraIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;

/**
 * Sample Flow to Load a CSV file in Astra
 *

 # ---- LOCAL

 mvn -Pdirect-runner compile \
 exec:java \
 -Dexec.mainClass=com.dtx.astra.dataflow.CsvCloudStorageToAstra \
 -Dexec.args="\
 --astraToken=AstraCS:uZclXTYecCAqPPjiNmkezapR:e87d6edb702acd87516e4ef78e0c0e515c32ab2c3529f5a3242688034149a0e4 \
 --astraSecureConnectBundle=/Users/cedricklunven/Downloads/scb-demo.zip \
 --astraKeyspace=demo \
 --inputCsv=/Users/cedricklunven/Downloads/language-codes.csv"


 # ---- DATAFLOW 
 mvn -Pdataflow-runner compile exec:java \
 -Dexec.mainClass=com.dtx.astra.dataflow.CsvCloudStorageToAstra \
 -Dexec.args="\
 --astraToken=projects/747469159044/secrets/astra-token/versions/1 \
 --astraSecureConnectBundle=projects/747469159044/secrets/cedrick-demo-scb/versions/1 \
 --astraKeyspace=demo \
 --inputCsv=gs://astra_dataflow_inputs/csv/language-codes.csv \
 --runner=DataflowRunner \
 --project=integrations-379317 \
 --region=us-central1"

 */
public class CsvCloudStorageToAstra {

    /**
     * Logger.
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(CsvCloudStorageToAstra.class);

    /**
     * Definition of the interface reflecting
     * what we have in metadata.json
     */
    public interface LoadDataPipelineOptions extends PipelineOptions {

        @Description("Location of CSV File in GCS")
        @Validation.Required
        String getInputCsv();
        void setInputCsv(String token);

        @Description("Location of Astra Token secret")
        @Validation.Required
        String getAstraToken();
        void setAstraToken(String token);

        @Description("Location of secret for secure connect bundle")
        @Validation.Required
        String getAstraSecureConnectBundle();
        void setAstraSecureConnectBundle(String path);

        @Description("Target Keyspace")
        @Validation.Required
        String getAstraKeyspace();
        void setAstraKeyspace(String keyspace);
    }

    /**
     * Code for the Pipeline.
     */
    public static void main(String[] args) throws IOException {
        LOGGER.info("Starting Pipeline:");
        LoadDataPipelineOptions options = PipelineOptionsFactory
                .fromArgs(args)
                .withValidation()
                .as(LoadDataPipelineOptions.class);
        FileSystems.setDefaultPipelineOptions(options);
        LOGGER.info("Pipeline options successfully parsed.");

        Pipeline csvToAstra = Pipeline.create(options);
        LOGGER.info("Pipeline created");
        csvToAstra.apply("Read CSV File", TextIO.read().from(options.getInputCsv()))
                  .apply("Create Destination Table", new CreateTableLanguage<String>(options))
                  .apply("Marshalling rows", ParDo.of(new DoFn<String, LanguageCode>() {
                      @ProcessElement
                      public void processElement(ProcessContext c) {
                          String[] chunks = c.element().split(",");
                          c.output(new LanguageCode(chunks[0], chunks[1]));
                      }
                   }))
                 .setCoder(SerializableCoder.of(LanguageCode.class))
                 .apply(buildAstraWrite(options));
        csvToAstra.run().waitUntilFinish();
    }

    private static String readAstraTokenSecret(SecretManagerServiceClient client, LoadDataPipelineOptions options) {
        return client
                .accessSecretVersion(options.getAstraToken())
                .getPayload().getData()
                .toStringUtf8();
    }

    private static byte[] readAstraBundleSecret(SecretManagerServiceClient client, LoadDataPipelineOptions options) {
        return client
                .accessSecretVersion(options.getAstraSecureConnectBundle())
                .getPayload().getData()
                .toByteArray();
    }

    private static AstraIO.Write<LanguageCode> buildAstraWrite(LoadDataPipelineOptions options) throws IOException {
        AstraIO.Write<LanguageCode> write = AstraIO.<LanguageCode>write()
            .withKeyspace(options.getAstraKeyspace())
            .withEntity(LanguageCode.class);
        if (options.getRunner().getName().toLowerCase().startsWith("dataflow")) {
            SecretManagerServiceClient client = SecretManagerServiceClient.create();
            write = write.withToken(readAstraTokenSecret(client, options));
            write = write.withSecureConnectBundleData(readAstraBundleSecret(client, options));
        } else {
            write = write.withToken(options.getAstraToken());
            write = write.withSecureConnectBundle(new File(options.getAstraSecureConnectBundle()));
        }
        return write;
    }

    /**
     * When running in GCP extra steps needed to open the secrets.
     *
     * @param options
     *      option
     * @return
     * @throws IOException
     */
    private static Cluster buildClient(LoadDataPipelineOptions options) throws IOException {
        Cluster.Builder builder = Cluster.builder();
        builder.withSocketOptions(new SocketOptions().setConnectTimeoutMillis(20000).setReadTimeoutMillis(20000));
        if (options.getRunner().getName().toLowerCase().startsWith("dataflow")) {
            // When Executed in DataFlow AstraToken and secure Bundle are secrets we need to open
            SecretManagerServiceClient client = SecretManagerServiceClient.create();
            builder.withAuthProvider(new PlainTextAuthProvider("token", readAstraTokenSecret(client, options)));
            builder.withCloudSecureConnectBundle(new ByteArrayInputStream(readAstraBundleSecret(client, options)));
        } else {
            builder.withAuthProvider(new PlainTextAuthProvider("token", options.getAstraToken()));
            builder.withCloudSecureConnectBundle(new File(options.getAstraSecureConnectBundle()));
        }
        return builder.build();
    }

    /**
     * Transform in CSV lines.
     */
    private static class CreateTableLanguage<T> extends PTransform<PCollection<T>, PCollection<T>> {

        /**
         * Astra options.
         *
         * @param astraOptions
         *    options
         */
        public CreateTableLanguage(LoadDataPipelineOptions astraOptions) throws IOException {
            // Create Target Table if needed
            try(Cluster cluster = CsvCloudStorageToAstra.buildClient(astraOptions)) {
                try(Session session = cluster.connect(astraOptions.getAstraKeyspace())) {
                    LOGGER.info("+ Connected to Astra (pre-check)");
                    session.execute(SchemaBuilder.createTable("language")
                            .addPartitionKey("code", DataType.text())
                            .addColumn("country", DataType.text())
                            .ifNotExists());
                    LOGGER.info("+ Table has been created (if needed)");
                }
            }
        }

        @Override
        public PCollection<T> expand(PCollection<T> input) { return input; }
    }

}
