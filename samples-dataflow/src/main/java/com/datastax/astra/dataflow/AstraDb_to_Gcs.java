package com.datastax.astra.dataflow;

import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.options.ValueProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Sample Pipeline for processing Data in Bulk with Astra.



 mvn -Pdataflow-runner compile exec:java \
 -Dexec.mainClass=com.dtx.astra.pipelines.dataflow.BulkDataExportWithDataflow \
 -Dexec.args="\
 --astraToken=projects/747469159044/secrets/astra-token/versions/1 \
 --secureConnectBundle=projects/747469159044/secrets/cedrick-demo-scb/versions/1 \
 --keyspace=demo \
 --table=simpledata \
 --targetFolder=gs://dataflow-apache-quickstart_integrations-379317/temp/
 --runner=DataflowRunner \
 --project=integrations-379317 \
 --region=us-central1 \
 --gcpTempLocation=gs://dataflow-apache-quickstart_integrations-379317/temp/"

 */
public class AstraDb_to_Gcs {

    /**
     * Logger for the class.
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(AstraDb_to_Gcs.class);

    /**
     * Flow Interface
     */
    public interface ExportTablePipelineOptions extends PipelineOptions {

        @Description("Location of Astra Token secret")
        @Validation.Required
        ValueProvider<String> getAstraToken();
        void setAstraToken(ValueProvider<String> token);

        @Description("Location of secret for secure connect bundle")
        @Validation.Required
        ValueProvider<String> getSecureConnectBundle();
        void setSecureConnectBundle(ValueProvider<String> path);

        @Description("Source Keyspace")
        @Validation.Required
        String getKeyspace();
        void setKeyspace(String keyspace);

        @Description("Source Table")
        @Validation.Required
        String getTable();
        void setTable(String table);

        @Description("Target folder")
        @Validation.Required
        String getTargetFolder();
        void setTargetFolder(String folder);
    }

    /**
     * Run Pipeline.
     *
     * @param args
     *      arguments
     * @throws Exception
     *      error occured in processing

    public static void main(String[] args) throws Exception {
        // Parse Options
        LOGGER.info("Starting 'ExportTableDataFlow' ....");
        ExportTablePipelineOptions options = PipelineOptionsFactory
                .fromArgs(args)
                .withValidation()
                .as(ExportTablePipelineOptions.class);

        // Build Read
        SecretManagerServiceClient client = SecretManagerServiceClient.create();
        AstraIO.Read<SimpleDataEntity> readFromAstra = AstraIO.<SimpleDataEntity>read()
                .withToken(client
                        .accessSecretVersion(options.getAstraToken().get())
                        .getPayload().getData()
                        .toStringUtf8())
                .withSecureConnectBundleData(client
                        .accessSecretVersion(options.getSecureConnectBundle().get())
                        .getPayload().getData()
                        .toByteArray())
                .withKeyspace(options.getKeyspace())
                .withTable(options.getTable())
                .withCoder(SerializableCoder.of(SimpleDataEntity.class))
                .withEntity(SimpleDataEntity.class);
        LOGGER.info("Astra Read has been initialized");

        // Run Pipeline to Read from Astra
        FileSystems.setDefaultPipelineOptions(options);
        Pipeline exportCsvPipeline = Pipeline.create(options);
        exportCsvPipeline.apply("Read Table", readFromAstra)
                         .apply("MapCsv", ParDo.of(new MapRecordAsCsvLine()))
                         .apply("WriteCsvInCloudStorage", TextIO
                                 .write()
                                 .to(options.getTargetFolder())
                                 .withSuffix(".txt")
                                 .withCompression(Compression.GZIP));
        exportCsvPipeline.run().waitUntilFinish();
    }

    /**
     * Transform in CSV lines.

    private static class MapRecordAsCsvLine extends DoFn<SimpleDataEntity, String> {

        @ProcessElement
        public void processElement(ProcessContext c) {
            String csvLine = c.element().getId() + ";" + c.element().getData();
            LOGGER.info("CSV Line: {}", csvLine);
            c.output(csvLine);
        }
    }*/

}
