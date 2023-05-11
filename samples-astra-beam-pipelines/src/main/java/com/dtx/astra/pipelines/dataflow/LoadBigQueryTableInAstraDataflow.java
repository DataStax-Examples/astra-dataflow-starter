package com.dtx.astra.pipelines.dataflow;

import com.dtx.astra.pipelines.LanguageCodeEntity;
import com.google.api.client.auth.oauth2.Credential;
import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableSchema;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.io.astra.AstraIO;
import org.apache.beam.sdk.io.astra.AstraCqlQueryPTransform;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static com.dtx.astra.pipelines.utils.AstraIOGcpUtils.*;

import java.io.IOException;

/**
 * Big Query to Astra
 *

 mvn -Pdataflow-runner compile exec:java \
 -Dexec.mainClass=com.dtx.astra.pipelines.dataflow.LoadBigQueryTableInAstraDataflow \
 -Dexec.args="\
 --project=integrations-379317 \
 --bigQueryDataset=dataflow_input_us \
 --bigQueryTable=languages \
 --region=us-central1 \
 --astraToken=projects/747469159044/secrets/astra-token/versions/2 \
 --secureConnectBundle=projects/747469159044/secrets/cedrick-demo-scb/versions/2 \
 --keyspace=gcp_integrations \
 --runner=DataflowRunner"

 */
public class LoadBigQueryTableInAstraDataflow {

    /**
     * Logger for the class.
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(BulkDataLoadWithDataflow.class);

    /**
     * BigQuery to Astra
     */
    public interface BigQueryTableToAstraPipelineOptions extends GcpOptions {

        // ==== FROM BIG QUERY ====

        @Description("BigQuery dataset name")
        @Default.String("dataflow_input_tiny")
        String getBigQueryDataset();
        void setBigQueryDataset(String dataset);

        @Description("BigQuery table name")
        @Default.String("table_language_codes")
        String getBigQueryTable();
        void setBigQueryTable(String table);

        // ==== TO ASTRA ====

        @Description("Astra Token resource id in Google Secret Manager")
        @Validation.Required
        String getAstraToken();
        void setAstraToken(String resourceId);

        @Description("Secure Connect Bundle resource id in Google Secret Manager")
        @Validation.Required
        String getSecureConnectBundle();
        void setSecureConnectBundle(String resourceId);

        @Description("Source Keyspace")
        @Validation.Required
        String getKeyspace();
        void setKeyspace(String keyspace);
    }

    /**
     * Big Query Client
     */
    private Bigquery bigQueryClient = null;

    /**
     * Main.
     *
     * @param args
     */
    public static void main(String[] args) throws IOException {

        BigQueryTableToAstraPipelineOptions bq2AstraOptions = PipelineOptionsFactory
                .fromArgs(args).withValidation()
                .as(BigQueryTableToAstraPipelineOptions.class);

        // --> Extract Secrets from Google Secret Manager
        String astraToken       = readTokenSecret(bq2AstraOptions.getAstraToken());
        byte[] astraSecureBundle = readSecureBundleSecret(bq2AstraOptions.getSecureConnectBundle());
        LOGGER.info("+ Secrets Parsed");

        Pipeline bq2AstraPipeline = Pipeline.create(bq2AstraOptions);
        LOGGER.info("+ Pipeline Created");

        bq2AstraPipeline
                        // 1. READ From BigQuery
                        .apply("Read from BigQuery query", BigQueryIO
                           .readTableRows()
                           .from( new TableReference()
                                   .setProjectId(bq2AstraOptions.getProject())
                                   .setDatasetId(bq2AstraOptions.getBigQueryDataset())
                                   .setTableId(bq2AstraOptions.getBigQueryTable())))

                        // 2. Marshal the beans
                        .apply(MapElements
                                .into(TypeDescriptor.of(LanguageCodeEntity.class))
                                .via(LanguageCodeEntity::fromTableRow))

                        // 3. Create Table if needed with a CQL Statement
                        .apply("Create Table " + LanguageCodeEntity.TABLE_NAME, new AstraCqlQueryPTransform<LanguageCodeEntity>(
                                astraToken, astraSecureBundle, bq2AstraOptions.getKeyspace(),
                                LanguageCodeEntity.createTableStatement().toString()))

                        // 4. Write into Astra
                        .apply("Write into Astra", AstraIO.<LanguageCodeEntity>write()
                           .withToken(astraToken)
                           .withKeyspace(bq2AstraOptions.getKeyspace())
                           .withSecureConnectBundleData(astraSecureBundle)
                           .withEntity(LanguageCodeEntity.class));

        bq2AstraPipeline.run().waitUntilFinish();
    }


}
