package com.datastax.astra.beam;

import com.datastax.astra.beam.lang.LanguageCode;
import com.datastax.astra.beam.lang.LanguageCodeDaoMapperFactoryFn;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.astra.db.AstraDbIO;
import org.apache.beam.sdk.io.astra.db.options.AstraDbReadOptions;
import org.apache.beam.sdk.io.astra.db.utils.AstraSecureConnectBundleUtils;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;

/**
 * Export an Astra Table as a CSV.
 **/

/*
 [Get your scb]
 ----------------
 astra db download-scb beam_integration_test_scb -f /Users/cedricklunven/Downloads/beam_integration_test_scb.zip

 [Set up your env variables]
 -----------------------------
 export ASTRA_TOKEN=`astra token`
 export ASTRA_SCB_PATH=/Users/cedricklunven/Downloads/beam_integration_test_scb.zip
 export ASTRA_KEYSPACE=beam

 [Run the pipeline]
 -----------------------------
 mvn clean compile exec:java \
 -Dexec.mainClass=com.datastax.astra.beam.AstraDb_To_Csv \
 -Dexec.args="\
 --astraToken=${ASTRA_TOKEN} \
 --astraSecureConnectBundle=${ASTRA_SCB_PATH} \
 --astraKeyspace=${ASTRA_KEYSPACE} \
 --table=languages \
 --csvOutput=`pwd`/src/test/resources/out/language"
 */
public class AstraDb_To_Csv {

    /**
     * Flow Interface
     */
    public interface AstraDbToCsvOptions extends AstraDbReadOptions {

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

        // Load Secure Bundle from Local File System
        byte[] scbZip = AstraSecureConnectBundleUtils
                .loadFromFilePath(options.getAstraSecureConnectBundle());

        try {
            Pipeline astra2Csv = Pipeline.create(options);
            astra2Csv.apply("Read Table", AstraDbIO.<LanguageCode>read()
                            .withToken(options.getAstraToken())
                            .withKeyspace(options.getAstraKeyspace())
                            .withSecureConnectBundle(scbZip)
                            .withTable(options.getTable())
                            .withCoder(SerializableCoder.of(LanguageCode.class))
                            .withMapperFactoryFn(new LanguageCodeDaoMapperFactoryFn())
                            .withEntity(LanguageCode.class))
                    .apply("MapCsv", MapElements.via(new MapRecordAsCsvLine()))
                    .apply("Write CSV file", TextIO.write().to(options.getCsvOutput()));

            astra2Csv.run().waitUntilFinish();
        } finally {
            AstraDbIO.close();
        }
    }

    /**
     * Transform in CSV lines.
     */
    private static class MapRecordAsCsvLine extends SimpleFunction<LanguageCode, String> {
        @Override
        public String apply(LanguageCode bean) {
            return bean.toCsvRow();
        }
    }

}
