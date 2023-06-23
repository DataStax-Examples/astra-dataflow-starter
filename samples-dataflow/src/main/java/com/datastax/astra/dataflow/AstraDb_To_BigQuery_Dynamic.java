package com.datastax.astra.dataflow;

import com.datastax.astra.dataflow.transforms.CassandraToBigQuerySchemaMapperFn;
import com.datastax.astra.dataflow.utils.GoogleSecretManagerUtils;
import com.datastax.oss.driver.api.core.CqlSession;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.DatasetId;
import com.google.cloud.bigquery.DatasetInfo;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.io.astra.db.AstraDbIO;
import org.apache.beam.sdk.io.astra.db.CqlSessionHolder;
import org.apache.beam.sdk.io.astra.db.mapping.AstraDbMapper;
import org.apache.beam.sdk.io.astra.db.mapping.BeamRowDbMapperFactoryFn;
import org.apache.beam.sdk.io.astra.db.options.AstraDbReadOptions;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryUtils;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;

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
 -Dexec.mainClass=com.datastax.astra.dataflow.AstraDb_To_BigQuery_Dynamic \
 -Dexec.args="\
 --astraToken=${ASTRA_SECRET_TOKEN} \
 --astraSecureConnectBundle=${ASTRA_SECRET_SECURE_BUNDLE} \
 --astraKeyspace=samples_dataflow \
 --table=${ASTRA_TABLE} \
 --runner=DataflowRunner \
 --project=${GCP_PROJECT_ID} \
 --region=us-central1"

 */
public class AstraDb_To_BigQuery_Dynamic {

    /**
     * Logger for the class.
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(AstraDb_To_BigQuery_Dynamic.class);


    /**
     * Flow Interface
     * RefTableSet = <projectId>:<dataset_name>.<table_name>
     */
    public interface AstraDbToBigQueryOptions extends AstraDbReadOptions, GcpOptions {

        @Description("BigQuery dataset name, if not provided will be set to Keyspace")
        String getBigQueryDataset();

        @SuppressWarnings("unused")
        void setBigQueryDataset(String dataset);

        @Description("BigQuery table name, if not provided will be set to Cassandra Table name")
        String getBigQueryTable();

        @SuppressWarnings("unused")
        void setBigQueryTable(String table);
    }

    /**
     * Main.
    */
    public static void main(String[] args) {

        try {

            /*
             * Pipeline Parsing.
             * BigQuery Dataset and Table have been made optional and
             * will be set to Keyspace and Table if not provided.
             */
            AstraDbToBigQueryOptions options = PipelineOptionsFactory
                    .fromArgs(args).withValidation()
                    .as(AstraDbToBigQueryOptions.class);
            LOGGER.info("Pipeline Validated");
            String bigQueryDataset = options.getBigQueryDataset();
            if (bigQueryDataset == null || "".equals(bigQueryDataset)) {
                bigQueryDataset = options.getAstraKeyspace();
            }
            LOGGER.info("Big Query dataset set to {}", bigQueryDataset);
            String bigQueryTable = options.getBigQueryTable();
            if (bigQueryTable == null || "".equals(bigQueryTable)) {
                bigQueryTable = options.getTable();
            }
            LOGGER.info("Big Query table set to {}", bigQueryTable);

            /*
             * Astra Credentials as stored in Google Secrets.
             * - astraToken could be provided as a String in the template (simpler ?)
             * - astraSecureBundle is a file (binary), could it be provided as template input ?
             */
            String astraToken = GoogleSecretManagerUtils.
                    readTokenSecret(options.getAstraToken());
            byte[] astraSecureBundle = GoogleSecretManagerUtils.
                    readSecureBundleSecret(options.getAstraSecureConnectBundle());
            LOGGER.info("Astra Credentials retrieved from Google Secrets");

            /*
             * If DataSet does not exist, creating DataSet
             * in same region as the worker.
             */
            BigQuery bigquery = BigQueryOptions.newBuilder()
                    .setProjectId(options.getProject()).build().getService();
            if (bigquery.getDataset(DatasetId.of(options.getProject(), bigQueryDataset)) == null) {
                LOGGER.info("Dataset was not found: creating DataSet {} in region {}",
                        bigQueryDataset, options.getWorkerRegion());
                bigquery.create(DatasetInfo.newBuilder(bigQueryDataset)
                        .setLocation(options.getWorkerRegion())
                        .build());
                LOGGER.info("Dataset Creation [OK]");
            }

            /*
             * Generic Serializer Cassandra Record => Beam Row.
             * - Most types are supported except UDT and Tuple.
             * - The mapper is created dynamically based on the table schema.
             * - List, Set are converted to ARRAY
             * - Columns part of the primary are not REQUIRED.
             */
            SerializableFunction<CqlSession, AstraDbMapper<Row>> beamRowMapperFactory =
                    new BeamRowDbMapperFactoryFn(options.getAstraKeyspace(), options.getTable());

            // Mapper Cassandra Table => BigQuery Schema
            SerializableFunction<AstraDbIO.Read<?>, TableSchema> bigQuerySchemaFactory =
                    new CassandraToBigQuerySchemaMapperFn(options.getAstraKeyspace(), options.getTable());
            LOGGER.info("Serializer initializations [OK]");

            // Source: AstraDb
            AstraDbIO.Read<Row> astraSource = AstraDbIO.<Row>read()
                    .withToken(astraToken)
                    .withSecureConnectBundle(astraSecureBundle)
                    .withKeyspace(options.getAstraKeyspace())
                    .withTable(options.getTable())
                    .withMinNumberOfSplits(5)
                    .withMapperFactoryFn(beamRowMapperFactory)
                    .withCoder(SerializableCoder.of(Row.class))
                    .withEntity(Row.class);
            LOGGER.info("AstraDb Source initialization [OK]");

            // Sink: BigQuery
            BigQueryIO.Write<Row> bigQuerySink =  BigQueryIO.<Row>write()
                    .to(new TableReference()
                            .setProjectId(options.getProject())
                            .setDatasetId(bigQueryDataset)
                            .setTableId(bigQueryTable))
                    // Specialized function reading cassandra source table and mapping to BigQuery Schema
                    .withSchema(bigQuerySchemaFactory.apply(astraSource))
                    // Provided by google, convert a Beam Row to a BigQuery TableRow
                    .withFormatFunction(BigQueryUtils::toTableRow)
                    // Table Will be created if not exist
                    .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                    .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND);
            LOGGER.info("BigQuery Sink initialization [OK]");

            // Run Pipeline
            Pipeline astraDbToBigQueryPipeline = Pipeline.create(options);
            astraDbToBigQueryPipeline
                    .apply("Read From Astra", astraSource)
                    .apply("Show", ParDo.of(new Slf4jBeamRowLoggerFn(LOGGER))).setCoder(SerializableCoder.of(Row.class)) // DEBUG
                    .apply("Write To BigQuery", bigQuerySink);
            astraDbToBigQueryPipeline.run().waitUntilFinish();

        } finally {
            CqlSessionHolder.cleanup();
        }
    }

    /**
     * Was relevant to debug the pipeline and mapping
     */
    public static class Slf4jBeamRowLoggerFn extends DoFn<Row, Row> implements Serializable {

        /** Logger of current Pipeline. */
        private Logger logger;

        /**
         * Constructor.
         *
         * @param log
         *      current logger
         */
        public Slf4jBeamRowLoggerFn(Logger log) {
            this.logger = log;
        }

        @ProcessElement
        public void processElement(ProcessContext c) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Row: {}", c.element().toString());
            }
            c.output(c.element());
        }
    }


}
