package com.datastax.astra.dataflow;

import com.datastax.astra.dataflow.utils.GoogleSecretManagerUtils;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.astra.db.CqlSessionHolder;
import org.apache.beam.sdk.io.astra.db.options.AstraDbWriteOptions;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.options.Validation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * When notification occur in GCS, push to AstraDb
 */
public class DataStream_To_AstraDb {

    /**
     * Logger for the class.
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(DataStream_To_AstraDb.class);

    /**
     * Streaming From DataStream to AstraDb
     */
    public interface DataStreamToAstraDbOptions extends PipelineOptions, StreamingOptions, AstraDbWriteOptions {

        // 1

        @Description("Path of the file pattern glob to read from, example: gs://bucket-name/path/to/files/*")
        String getInputFilePattern();

        @SuppressWarnings("unused")
        void setInputFilePattern(String value);

        // 2

        @Description("Format for notifications sent by datastream")
        @Default.String("json")
        String getInputFileFormat();

        @SuppressWarnings("unused")
        void setInputFileFormat(String value);

        // 3

        @Description("Pub/Sub subscription to read the input from, in the format " +
                "of 'projects/your-project-id/subscriptions/your-subscription-name'")
        String getInputSubscription();

        @SuppressWarnings("unused")
        void setInputSubscription(String value);

        // 4

        @Description("The DataStream Stream to Reference.")
        String getStreamName();

        @SuppressWarnings("unused")
        void setStreamName(String value);

        // 5

        @Description("The starting DateTime used to fetch from Cloud Storage")
        @Default.String("1970-01-01T00:00:00.00Z")
        String getRfcStartDateTime();

        @SuppressWarnings("unused")
        void setRfcStartDateTime(String value);

        // 6

        @Description("The number of concurrent DataStream files to read.")
        @Default.Integer(10)
        Integer getFileReadConcurrency();


        @SuppressWarnings("unused")
        void setFileReadConcurrency(Integer value);

        // 7

        @Description("Adding Astra Destination table")
        @Validation.Required
        String getAstraTable();

        @SuppressWarnings("unused")
        void setAstraTable(String table);
    }

    private static final String AVRO_SUFFIX = "avro";

    public static final Set<String> MAPPER_IGNORE_FIELDS =
            new HashSet<String>(
                    Arrays.asList(
                            "_metadata_stream",
                            "_metadata_schema",
                            "_metadata_table",
                            "_metadata_source",
                            "_metadata_ssn",
                            "_metadata_rs_id",
                            "_metadata_tx_id",
                            "_metadata_dlq_reconsumed",
                            "_metadata_error",
                            "_metadata_retry_count",
                            "_metadata_timestamp",
                            "_metadata_read_timestamp",
                            "_metadata_read_method",
                            "_metadata_source_type",
                            "_metadata_deleted",
                            "_metadata_change_type",
                            "_metadata_primary_keys",
                            "_metadata_log_file",
                            "_metadata_log_position"));

    /**
     * Main.
     */
    public static void main(String[] args) {

        DataStreamToAstraDbOptions options = PipelineOptionsFactory
                .fromArgs(args).withValidation()
                .as(DataStreamToAstraDbOptions.class);
        // Important !
        options.setStreaming(true);

        try {

            // Read Input From Google Secrets
            String astraToken = GoogleSecretManagerUtils.
                    readTokenSecret(options.getAstraToken());
            byte[] astraSecureBundle = GoogleSecretManagerUtils.
                    readSecureBundleSecret(options.getAstraSecureConnectBundle());

            Pipeline dataStreamToAstraDbPipeline = Pipeline.create(options);
            LOGGER.info("+ Pipeline Created");

            /*
             * Stage 1: Ingest and Normalize Data to FailsafeElement with JSON Strings
             *   a) Read DataStream data from GCS into JSON String FailsafeElements (datastreamJsonRecords)

            PCollection<FailsafeElement<String, String>> datastreamJsonRecords =
                    pipeline.apply(
                            new DataStreamIO(
                                    options.getStreamName(),
                                    options.getInputFilePattern(),
                                    options.getInputFileFormat(),
                                    options.getInputSubscription(),
                                    options.getRfcStartDateTime())
                                    .withFileReadConcurrency(options.getFileReadConcurrency()));

            dataStreamToAstraDbPipeline

                    // 2. Marshall as expected entities
                    .apply(MapElements
                            .into(TypeDescriptor.of(LanguageCode.class))
                            .via(LanguageCode::fromBigQueryTableRow))

                    // 3. Create Table if needed with a CQL Statement
                    .apply("Create Destination Table",
                            new AstraCqlQueryPTransform<>(astraToken, astraSecureBundle,
                                    options.getKeyspace(), LanguageCode.cqlCreateTable()))

                    // 4. Write into Astra
                    .apply("Write Into Astra", AstraDbIO.<LanguageCode>write()
                            .withToken(astraToken)                          // read from secret
                            .withSecureConnectBundleData(astraSecureBundle) // read from secret
                            .withKeyspace(options.getKeyspace())
                            .withEntity(LanguageCode.class));
*/
            dataStreamToAstraDbPipeline.run().waitUntilFinish();
        } finally {
            CqlSessionHolder.cleanup();
        }
    }

}