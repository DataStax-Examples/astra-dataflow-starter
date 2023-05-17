package com.datastax.astra.dataflow;

import com.datastax.astra.dataflow.utils.GoogleBigQueryUtils;
import com.datastax.astra.dataflow.utils.GoogleSecretManagerUtils;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableSchema;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.io.astra.db.AstraDbConnectionManager;
import org.apache.beam.sdk.io.astra.db.AstraDbIO;
import org.apache.beam.sdk.io.astra.db.options.AstraDbReadOptions;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Copy a Cassandra Table in BiQuery.

 mvn compile exec:java \
 -Dexec.mainClass=com.datastax.astra.dataflow.AstraDb_To_BigQuery \
 -Dexec.args="\
 --astraToken=projects/747469159044/secrets/astra-token/versions/2 \
 --astraSecureConnectBundle=projects/747469159044/secrets/secure-connect-bundle-demo/versions/1 \
 --keyspace=samples_dataflow \
 --table=languages \
 --bigQueryDataset=dataflow_input_us \
 --bigQueryTable=destination \
 --runner=DataflowRunner \
 --project=integrations-379317 \
 --region=us-central1"

 */
public class AstraDb_To_BigQuery {

    /**
     * Flow Interface
     * RefTableSet = <projectId>:<dataset_name>.<table_name>
     */
    public interface AstraDbToBigQueryOptions extends AstraDbReadOptions, GcpOptions {

        @Description("BigQuery dataset name")
        @Validation.Required
        String getBigQueryDataset();
        void setBigQueryDataset(String dataset);

        @Description("BigQuery table name")
        @Validation.Required
        String getBigQueryTable();
        void setBigQueryTable(String table);
    }

    /**
     * Main.
    */
    public static void main(String[] args) {
        AstraDbToBigQueryOptions options = PipelineOptionsFactory
            .fromArgs(args).withValidation()
            .as(AstraDbToBigQueryOptions.class);

        try {

            // Read Input From Google Secrets
            String astraToken = GoogleSecretManagerUtils.
                    readTokenSecret(options.getAstraToken());
            byte[] astraSecureBundle = GoogleSecretManagerUtils.
                    readSecureBundleSecret(options.getAstraSecureConnectBundle());

            // Build source
            AstraDbIO.Read<LanguageCode> astraSource = AstraDbIO.<LanguageCode>read()
                    .withToken(astraToken)
                    .withSecureConnectBundleData(astraSecureBundle)
                    .withKeyspace(options.getKeyspace())
                    .withTable(options.getTable())
                    .withCoder(SerializableCoder.of(LanguageCode.class))
                    .withEntity(LanguageCode.class);

            // Read BigQuery Schema
            TableSchema tableSchema = GoogleBigQueryUtils
                    .getTableSchema("schema_language_codes.json");

            // Build sink
            BigQueryIO.Write<LanguageCode> bigQuerySink =  BigQueryIO.<LanguageCode>write()
                    .to(new TableReference()
                            .setProjectId(options.getProject())
                            .setDatasetId(options.getBigQueryDataset())
                            .setTableId(options.getBigQueryTable()))
                    .withSchema(tableSchema)
                    .withFormatFunction(LanguageCode::toBigQueryTableRow)
                    .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                    .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND);

            // Run Pipeline
            Pipeline astraDbToBigQueryPipeline = Pipeline.create(options);
            astraDbToBigQueryPipeline
                    .apply("Read From Astra", astraSource)
                    .apply("Write To BigQuery", bigQuerySink);

            astraDbToBigQueryPipeline.run().waitUntilFinish();
        } finally {
            AstraDbConnectionManager.cleanup();
        }

    }



}
