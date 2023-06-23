package com.datastax.astra.dataflow;

import com.datastax.astra.dataflow.domains.LanguageCode;
import com.datastax.astra.dataflow.domains.LanguageCodeDaoMapperFactoryFn;
import com.datastax.astra.dataflow.utils.GoogleSecretManagerUtils;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.astra.db.AstraDbIO;
import org.apache.beam.sdk.io.astra.db.CqlSessionHolder;
import org.apache.beam.sdk.io.astra.db.options.AstraDbReadOptions;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Sample Pipeline for processing Data in Bulk with Astra.
 *

 mvn compile exec:java \
 -Dexec.mainClass=com.datastax.astra.dataflow.AstraDb_To_Gcs \
 -Dexec.args="\
 --astraToken=${ASTRA_SECRET_TOKEN} \
 --astraSecureConnectBundle=${ASTRA_SECRET_SECURE_BUNDLE} \
 --keyspace=samples_dataflow \
 --table=languages \
 --outputFolder=${GCP_OUTPUT_CSV} \
 --runner=DataflowRunner \
 --project=${GCP_PROJECT_ID} \
 --region=us-central1"

 mvn compile exec:java \
 -Dexec.mainClass=com.datastax.astra.dataflow.AstraDb_To_Gcs \
 -Dexec.args="\
 --astraToken=projects/747469159044/secrets/astra-token/versions/2 \
 --astraSecureConnectBundle=projects/747469159044/secrets/secure-connect-bundle-demo/versions/2 \
 --astraKeyspace=samples_dataflow \
 --table=languages \
 --outputFolder=gs://astra_dataflow_output \
 --runner=DataflowRunner \
 --project=integrations-379317 \
 --region=us-central1"

 */
public class AstraDb_To_Gcs {

    /**
     * Logger for the class.
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(AstraDb_To_Gcs.class);

    /**
     * Flow Interface
     */
    public interface AstraDbToGcsOptions extends AstraDbReadOptions, PipelineOptions {

        // --- outputFolder --

        @Validation.Required
        @Description("Path of file to read from a Bucket")
        String getOutputFolder();

        @SuppressWarnings("unused")
        void setOutputFolder(String folder);
    }

    /**
     * Run Pipeline.
     *
     * @param args
     *      arguments
     */
    public static void main(String[] args)  {

        AstraDbToGcsOptions options = PipelineOptionsFactory
                .fromArgs(args).withValidation()
                .as(AstraDbToGcsOptions.class);// Parse Options

        String astraToken = GoogleSecretManagerUtils.
                readTokenSecret(options.getAstraToken());
        byte[] astraSecureBundle = GoogleSecretManagerUtils.
                readSecureBundleSecret(options.getAstraSecureConnectBundle());

        // Pipeline
        Pipeline astraDbToGcsPipeline = Pipeline.create(options);

        try {
            // Build Read
            AstraDbIO.Read<LanguageCode> astraSource = AstraDbIO.<LanguageCode>read()
                    .withToken(astraToken)
                    .withSecureConnectBundle(astraSecureBundle)
                    .withKeyspace(options.getAstraKeyspace())
                    .withTable(options.getTable())
                    .withCoder(SerializableCoder.of(LanguageCode.class))
                    .withMapperFactoryFn(new LanguageCodeDaoMapperFactoryFn())
                    .withEntity(LanguageCode.class);
            LOGGER.info("Astra Read has been initialized");

            astraDbToGcsPipeline
                    .apply("Read Table", astraSource)

                    .apply("MapCsv", ParDo.of(new MapRecordAsCsvLine()))

                    .apply("WriteCsvInCloudStorage", TextIO
                            .write()
                            .to(options.getOutputFolder())
                            .withSuffix(".csv"));
            //.withCompression(Compression.GZIP)
            astraDbToGcsPipeline.run().waitUntilFinish();
        } finally {
            CqlSessionHolder.cleanup();
        }
    }

    /**
     * Transform in CSV lines.
     */
    private static class MapRecordAsCsvLine extends DoFn<LanguageCode, String> {
        @DoFn.ProcessElement
        public void processElement(ProcessContext c) {
            c.output(c.element().toCsvRow());
        }
    }

}
