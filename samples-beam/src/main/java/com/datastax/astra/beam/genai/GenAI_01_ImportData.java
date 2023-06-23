package com.datastax.astra.beam.genai;

import com.datastax.astra.beam.fable.Fable;
import com.datastax.astra.beam.fable.FableDaoMapperFactoryFn;
import com.datastax.astra.beam.fable.FableDto;
import com.datastax.astra.beam.fable.SimpleFableDbMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.astra.db.AstraDbIO;
import org.apache.beam.sdk.io.astra.db.options.AstraDbReadOptions;
import org.apache.beam.sdk.io.astra.db.transforms.RunCqlQueryFn;
import org.apache.beam.sdk.io.astra.db.utils.AstraSecureConnectBundleUtils;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;

/**
 * Create Embeddings
 **/
@Slf4j
public class GenAI_01_ImportData {

    /**
     * Flow Interface
     */
    public interface CsvImportOption extends AstraDbReadOptions {

        @Validation.Required
        @Description("Path of file to read from")
        String getCsvInput();

        @SuppressWarnings("unused")
        void setCsvInput(String csvFile);
    }

    /**
     * Main execution
     */
    public static void main(String[] args) {
        // Parse and Validate Parameters
        CsvImportOption options = PipelineOptionsFactory
                .fromArgs(args).withValidation()
                .as(CsvImportOption.class);

        // Load Secure Bundle from Local File System
        byte[] scbZip = AstraSecureConnectBundleUtils
                .loadFromFilePath(options.getAstraSecureConnectBundle());

        long top = System.currentTimeMillis();
        try {
            log.info("Parameters validations is successful, launching pipeline");
            Pipeline pipelineWrite = Pipeline.create(options);
            pipelineWrite
                    // Read a CSV
                    .apply("Read Data From Disk",
                            TextIO.read().from(options.getCsvInput()))

                    // Convert each CSV row to a LanguageCode bean
                    .apply("Convert To Cassandra Bean", MapElements.via(new MapCsvRowToFable()))

                    // Single Operation perform in the constructor of PTransform
                    .apply("Create Destination Table", new RunCqlQueryFn<>(options.getAstraToken(),
                            scbZip, options.getAstraKeyspace(),
                            Fable.cqlCreateTable(options.getAstraKeyspace())))

                    // Insert Results Into Astra
                    .apply("Write Into Astra", AstraDbIO.<FableDto>write()
                            .withToken(options.getAstraToken())
                            .withSecureConnectBundle(scbZip)
                            .withKeyspace(options.getAstraKeyspace())
                            .withMapperFactoryFn(new SimpleFableDbMapper())
                            .withEntity(FableDto.class));

            pipelineWrite.run().waitUntilFinish();

        } finally {
            log.info("Pipeline finished in {} millis", System.currentTimeMillis()-top);
            AstraDbIO.close();
        }
    }

    private static class MapCsvRowToFable extends SimpleFunction<String, FableDto> {
        @Override
        public FableDto apply(String input) {
            return new FableDto(Fable.fromCsvRow(input));
        }
    }

}
