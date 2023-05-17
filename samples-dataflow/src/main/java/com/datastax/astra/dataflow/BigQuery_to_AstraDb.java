package com.datastax.astra.dataflow;

import com.datastax.astra.dataflow.utils.GoogleSecretManagerUtils;
import com.google.api.services.bigquery.model.TableReference;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.io.astra.db.AstraDbConnectionManager;
import org.apache.beam.sdk.io.astra.db.AstraDbIO;
import org.apache.beam.sdk.io.astra.db.options.AstraDbWriteOptions;
import org.apache.beam.sdk.io.astra.db.transforms.AstraCqlQueryPTransform;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Big Query to Astra
 *

 mvn compile exec:java \
 -Dexec.mainClass=com.datastax.astra.dataflow.BigQuery_to_AstraDb \
 -Dexec.args="\
 --astraToken=projects/747469159044/secrets/astra-token/versions/2 \
 --astraSecureConnectBundle=projects/747469159044/secrets/secure-connect-bundle-demo/versions/1 \
 --keyspace=samples_dataflow \
 --bigQueryDataset=dataflow_input_us \
 --bigQueryTable=destination \
 --runner=DataflowRunner \
 --project=integrations-379317 \
 --region=us-central1"

 */
public class BigQuery_to_AstraDb {

    /**
     * Logger for the class.
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(Gcs_To_AstraDb.class);

    /**
     * BigQuery to Astra
     */
    public interface BigQueryToAstraDbOptions extends GcpOptions, AstraDbWriteOptions {

        @Description("BigQuery dataset name")
        @Default.String("dataflow_input_tiny")
        String getBigQueryDataset();

        @SuppressWarnings("unused")
        void setBigQueryDataset(String dataset);

        @Description("BigQuery table name")
        @Default.String("table_language_codes")
        String getBigQueryTable();

        @SuppressWarnings("unused")
        void setBigQueryTable(String table);

    }

    /**
     * Main.
     */
    public static void main(String[] args) {

        BigQueryToAstraDbOptions options = PipelineOptionsFactory
                .fromArgs(args).withValidation()
                .as(BigQueryToAstraDbOptions.class);

        try {

            // Read Input From Google Secrets
            String astraToken = GoogleSecretManagerUtils.
                    readTokenSecret(options.getAstraToken());
            byte[] astraSecureBundle = GoogleSecretManagerUtils.
                    readSecureBundleSecret(options.getAstraSecureConnectBundle());

            Pipeline bigQueryToAstraDbPipeline = Pipeline.create(options);
            LOGGER.info("+ Pipeline Created");

            bigQueryToAstraDbPipeline
                    // Source From BigQuery
                    .apply("Read from BigQuery query", BigQueryIO
                            .readTableRows()
                            .from(new TableReference()
                                    .setProjectId(options.getProject())
                                    .setDatasetId(options.getBigQueryDataset())
                                    .setTableId(options.getBigQueryTable())))

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
            bigQueryToAstraDbPipeline.run().waitUntilFinish();
        } finally {
            AstraDbConnectionManager.cleanup();
        }
    }

}
