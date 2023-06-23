package com.datastax.astra.dataflow;

import com.datastax.astra.dataflow.domains.LanguageCode;
import com.datastax.astra.dataflow.domains.LanguageCodeDaoMapperFactoryFn;
import com.datastax.astra.dataflow.utils.GoogleBigQueryUtils;
import com.datastax.astra.dataflow.utils.GoogleSecretManagerUtils;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableSchema;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.io.astra.db.AstraDbIO;
import org.apache.beam.sdk.io.astra.db.CqlSessionHolder;
import org.apache.beam.sdk.io.astra.db.options.AstraDbReadOptions;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;

/**
 * Copy a Cassandra Table in BiQuery.

 export ASTRA_SECRET_TOKEN=projects/747469159044/secrets/astra-token/versions/2
 export ASTRA_SECRET_SECURE_BUNDLE=projects/747469159044/secrets/secure-connect-bundle-demo/versions/2
 export ASTRA_KEYSPACE=samples_dataflow
 export ASTRA_TABLE=languages

 export GCP_PROJECT_ID=integrations-379317
 export GCP_BIGQUERY_DATASET=dataflow_input_us
 export GCP_BIGQUERY_TABLE=destination

 mvn compile exec:java \
 -Dexec.mainClass=com.datastax.astra.dataflow.AstraDb_To_BigQuery \
 -Dexec.args="\
 --astraToken=${ASTRA_SECRET_TOKEN} \
 --astraSecureConnectBundle=${ASTRA_SECRET_SECURE_BUNDLE} \
 --astraKeyspace=samples_dataflow \
 --table=${ASTRA_TABLE} \
 --bigQueryDataset=${GCP_BIGQUERY_DATASET} \
 --bigQueryTable=${GCP_BIGQUERY_TABLE} \
 --runner=DataflowRunner \
 --project=${GCP_PROJECT_ID} \
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

        @SuppressWarnings("unused")
        void setBigQueryDataset(String dataset);

        @Description("BigQuery table name")
        @Validation.Required
        String getBigQueryTable();

        @SuppressWarnings("unused")
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
                    .withSecureConnectBundle(astraSecureBundle)
                    .withKeyspace(options.getAstraKeyspace())
                    .withTable(options.getTable())
                    .withCoder(SerializableCoder.of(LanguageCode.class))
                    .withMapperFactoryFn(new LanguageCodeDaoMapperFactoryFn())
                    .withEntity(LanguageCode.class);

            // Read BigQuery Schema
            TableSchema tableSchema = GoogleBigQueryUtils
                    .readTableSchemaFromJsonFile("schema_language_codes.json");

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
            CqlSessionHolder.cleanup();
        }

    }



}
