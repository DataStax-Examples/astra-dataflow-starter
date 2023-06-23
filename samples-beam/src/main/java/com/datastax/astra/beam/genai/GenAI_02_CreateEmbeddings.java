package com.datastax.astra.beam.genai;

import com.datastax.astra.beam.fable.Fable;
import com.datastax.astra.beam.fable.FableDaoMapperFactoryFn;
import com.datastax.astra.beam.fable.FableDto;
import com.datastax.astra.beam.fable.SimpleFableDbMapper;
import com.theokanning.openai.embedding.EmbeddingRequest;
import com.theokanning.openai.service.OpenAiService;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.io.astra.db.AstraDbIO;
import org.apache.beam.sdk.io.astra.db.options.AstraDbReadOptions;
import org.apache.beam.sdk.io.astra.db.transforms.RunCqlQueryFn;
import org.apache.beam.sdk.io.astra.db.utils.AstraSecureConnectBundleUtils;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;

import java.util.Arrays;
import java.util.stream.Collectors;

/**
 * Create Embeddings
 */
@Slf4j
public class GenAI_02_CreateEmbeddings {

    /**
     * Flow Interface
     */
    public interface CreateEmbeddingsOptions extends AstraDbReadOptions {

        @Validation.Required
        @Description("Path of file to read from")
        String getOpenAiKey();

        @SuppressWarnings("unused")
        void setOpenAiKey(String key);
    }

    /**
     * Main execution
     */
    public static void main(String[] args) {
        // Parse and Validate Parameters
        CreateEmbeddingsOptions options = PipelineOptionsFactory
                .fromArgs(args).withValidation()
                .as(CreateEmbeddingsOptions.class);

        // Load Secure Bundle from Local File System
        byte[] scbZip = AstraSecureConnectBundleUtils
                .loadFromFilePath(options.getAstraSecureConnectBundle());

        long top = System.currentTimeMillis();
        try {
            log.info("Parameters validations is successful, launching pipeline");
            Pipeline genaiPipeline = Pipeline.create(options);

            // Read the table AS-IS
            genaiPipeline.apply("Read Table", AstraDbIO.<FableDto>read()
                            .withToken(options.getAstraToken())
                            .withKeyspace(options.getAstraKeyspace())
                            .withSecureConnectBundle(scbZip)
                            .withTable(options.getTable())
                            .withCoder(SerializableCoder.of(FableDto.class))
                            .withMapperFactoryFn(new SimpleFableDbMapper())
                            .withEntity(FableDto.class))

            // Alter table to add Vector
            .apply("Alter Table to add the vector capability",
                    new RunCqlQueryFn<>(options.getAstraToken(),
                            scbZip, options.getAstraKeyspace(),
                            Fable.cqlAlterTableForVectorSearch1(options.getAstraKeyspace(),
                                    "fable", 1536)))

            .apply("Alter Table to add the vector capability",
                    new RunCqlQueryFn<>(options.getAstraToken(),
                            scbZip, options.getAstraKeyspace(),
                            Fable.cqlAlterTableForVectorSearch2(options.getAstraKeyspace(),
                                    "fable")))

            // Create Index on the table
            .apply("Alter Table to add the vector capability",
                    new RunCqlQueryFn<>(options.getAstraToken(),
                            scbZip, options.getAstraKeyspace(),
                            Fable.cqlCreateIndexForVectorSearch(options.getAstraKeyspace(), "fable")))

             // Open AI Enrichment
             .apply("Embeddings transform embeddings",
                     ParDo.of(new TransformEmbeddingFn(options.getOpenAiKey())))

             // Insert Results Into Astra
             .apply("Write Into Astra", AstraDbIO.<FableDto>write()
                            .withToken(options.getAstraToken())
                            .withSecureConnectBundle(scbZip)
                            .withKeyspace(options.getAstraKeyspace())
                            .withMapperFactoryFn(new FableDaoMapperFactoryFn())
                            .withEntity(FableDto.class));

            genaiPipeline.run().waitUntilFinish();

        } finally {
            log.info("Pipeline finished in {} millis", System.currentTimeMillis()-top);
            AstraDbIO.close();
        }
    }

    private static class TransformEmbeddingFn extends DoFn<FableDto, FableDto> {

        private String openApiKey;

        public TransformEmbeddingFn(String openApiKey) {
            this.openApiKey = openApiKey;
        }
        @ProcessElement
        public void processElement(@Element FableDto row, OutputReceiver<FableDto> receiver) {
            FableDto f = new FableDto();
            f.setDocument(row.getDocument());
            f.setDocumentId(row.getDocumentId());
            f.setTitle(row.getTitle());
            // Metadata
            f.setMetadata("{ \"processing\": \"openai\" }");
            // Random List
            /*
            List<Float> floatList = IntStream.range(0, 1536)
                    .mapToObj(i -> new Random().nextFloat())
                    .collect(Collectors.toList());

             */
            EmbeddingRequest request = EmbeddingRequest.builder()
                    .model("text-embedding-ada-002")
                    .input(Arrays.asList(row.getDocument()))
                    .build();
            // only one request sent
            f.setVector(new OpenAiService(openApiKey).createEmbeddings(request)
                    .getData().get(0)
                    .getEmbedding().stream()
                    .map(e -> e.floatValue())
                    .collect(Collectors.toList()));
            log.info("Vector {}", f.getVector());
            receiver.output(f);
        }
    }

}
