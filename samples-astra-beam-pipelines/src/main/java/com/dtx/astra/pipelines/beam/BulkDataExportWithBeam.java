package com.dtx.astra.pipelines.beam;

import com.dtx.astra.pipelines.SimpleDataEntity;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.astra.AstraIO;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

/**
 * Sample Pipeline for processing Data in Bulk with Astra.

 mvn -Pdirect-runner compile exec:java \
 -Dexec.mainClass=com.dtx.astra.pipelines.beam.BulkDataExportWithBeam \
 -Dexec.args="\
 --astraToken=${ASTRA_TOKEN} \
 --secureConnectBundle=${ASTRA_SCB_PATH} \
 --keyspace=${ASTRA_KEYSPACE} \
 --table=${ASTRA_TABLE} \
 --targetFolder=${DESTINATION}"

 */
public class BulkDataExportWithBeam {

    /**
     * Logger for the class.
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(BulkDataExportWithBeam.class);

    /**
     * Flow Interface
     */
    public interface ExportTablePipelineOptions extends PipelineOptions {

        @Description("AstraToken Value")
        @Validation.Required
        ValueProvider<String> getAstraToken();
        void setAstraToken(ValueProvider<String> token);

        @Description("Location of fie on disk")
        @Validation.Required
        ValueProvider<String> getSecureConnectBundle();
        void setSecureConnectBundle(ValueProvider<String> path);

        @Description("Source Keyspace")
        @Validation.Required
        String getKeyspace();
        void setKeyspace(String keyspace);

        @Description("Source Table")
        String getTable();
        void setTable(String table);

        @Description("Destination folder")
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
     */
    public static void main(String[] args) throws Exception {
        // Parse Options
        LOGGER.info("Starting 'ExportTableLocally' (local) ....");
        ExportTablePipelineOptions options = PipelineOptionsFactory
                .fromArgs(args)
                .withValidation()
                .as(ExportTablePipelineOptions.class);
        FileSystems.setDefaultPipelineOptions(options);

        // Build Read
        Pipeline exportCsvPipeline = Pipeline.create(options);
        exportCsvPipeline
                .apply("Read Table", AstraIO
                        .<SimpleDataEntity>read()
                        .withToken(options.getAstraToken().get())
                        .withSecureConnectBundle(new File(options.getSecureConnectBundle().get()))
                        .withKeyspace(options.getKeyspace())
                        .withTable(options.getTable())
                        .withCoder(SerializableCoder.of(SimpleDataEntity.class))
                        .withEntity(SimpleDataEntity.class))
                .apply("MapCsv", ParDo.of(new MapRecordAsCsvLine()))
                .apply("WriteCsvInCloudStorage", TextIO.write().to(options.getTargetFolder()));
        exportCsvPipeline
                .run()
                .waitUntilFinish(Duration.standardSeconds(30));
    }

    /**
     * Transform in CSV lines.
     */
    private static class MapRecordAsCsvLine extends DoFn<SimpleDataEntity, String> {

        @ProcessElement
        public void processElement(ProcessContext c) {
            String csvLine = c.element().getId() + ";" + c.element().getData();
            LOGGER.info("CSV Line: {}", csvLine);
            c.output(csvLine);
        }
    }

}
