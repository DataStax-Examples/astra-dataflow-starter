package com.datastax.astra.beam;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.astra.db.AstraDbConnectionManager;
import org.apache.beam.sdk.io.astra.db.AstraDbIO;
import org.apache.beam.sdk.io.astra.db.options.AstraDbReadOptions;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;

import java.io.File;

/**
 * Export an Astra Table as a CSV.
 *
 mvn clean compile exec:java \
 -Dexec.mainClass=com.datastax.astra.beam.AstraDb_To_Csv \
 -Dexec.args="\
 --astraToken=${ASTRA_TOKEN} \
 --astraSecureConnectBundle=${ASTRA_SCB_PATH} \
 --keyspace=${ASTRA_KEYSPACE} \
 --table=languages \
 --csvOutput=`pwd`/src/test/resources/out/language"
 */
public class AstraDb_To_Csv {

    /**
     * Flow Interface
     */
    public interface AstraDbToCsvOptions extends AstraDbReadOptions {

        // --- csvOutput --

        @Validation.Required
        @Description("Path of file to read from")
        String getCsvOutput();

        @SuppressWarnings("unused")
        void setCsvOutput(String csvOutput);
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
        AstraDbToCsvOptions options = PipelineOptionsFactory
                .fromArgs(args).withValidation()
                .as(AstraDbToCsvOptions.class);

        // Build Read
        Pipeline exportCsvPipeline = Pipeline.create(options);
        try {
            exportCsvPipeline

                    .apply("Read Table", AstraDbIO.<LanguageCode>read()
                            .withToken(options.getAstraToken())
                            .withSecureConnectBundle(new File(options.getAstraSecureConnectBundle()))
                            .withKeyspace(options.getKeyspace())
                            .withTable(options.getTable())
                            .withCoder(SerializableCoder.of(LanguageCode.class))
                            .withEntity(LanguageCode.class))

                    .apply("MapCsv", ParDo.of(new MapRecordAsCsvLine()))

                    .apply("Write CSV file", TextIO.write().to(options.getCsvOutput()));

            exportCsvPipeline.run();
        } finally {
            AstraDbConnectionManager.cleanup();
        }
    }

    /**
     * Transform in CSV lines.
     */
    private static class MapRecordAsCsvLine extends DoFn<LanguageCode, String> {

        @ProcessElement
        public void processElement(ProcessContext c) {
            c.output(c.element().toCsvRow());
        }
    }

}
