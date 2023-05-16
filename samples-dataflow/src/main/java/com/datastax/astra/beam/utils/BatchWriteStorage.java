package com.datastax.astra.beam.utils;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;

import java.util.Arrays;
import java.util.List;

/**
 * Write

 mvn -Pdataflow-runner compile exec:java \
 -Dexec.mainClass=com.dtx.astra.pipelines.utils.BatchWriteStorage \
 -Dexec.args="\
 --runner=DataflowRunner \
 --project=integrations-379317 \
 --region=us-central1 \
 --gcpTempLocation=gs://dataflow-apache-quickstart_integrations-379317/temp \
 --bucketName=gs://astra_dataflow_output"

 */
public class BatchWriteStorage {

    public interface Options extends PipelineOptions {
        @Description("The Cloud Storage bucket to write to")
        String getBucketName();
        void setBucketName(String value);
    }

    public static void main(String[] args) {
        final List<String> wordsList = Arrays.asList("1", "2", "3", "4");
        var options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
        var pipeline = Pipeline.create(options);
        pipeline.apply(Create.of(wordsList))
                .apply(TextIO.write()
                        .to(options.getBucketName())
                        .withSuffix(".txt")
                        .withCompression(Compression.GZIP));
        pipeline.run().waitUntilFinish();
    }

}
