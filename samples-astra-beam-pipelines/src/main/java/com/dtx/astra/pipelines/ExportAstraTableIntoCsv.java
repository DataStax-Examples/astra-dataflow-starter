package com.dtx.astra.pipelines;

import com.dtx.astra.pipelines.utils.AstraPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Sample Pipeline for processing Data in Bulk with Astra.
 *
 * - Connect to Astra through CQL interface
 * - Extract data as a CSV and save into GCS
 *
 * Options are provided in the command line:
 * -Dexec.args="\
 *   --project=${projectCode} \
 *   --keyspace=${keyspaceName} \
 *   --secureConnectBundle=${scbUrl in GCS or Http} \
 *   --token=${astra token} \
 *   --gcpTempLocation=gs://dataflow-apache-quickstart_integrations-379317/temp/ \
 *   --runner=DataflowRunner \
 *   --region=us-central1" \
 */
public class ExportAstraTableIntoCsv {

    /**
     * Logger for the class.
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(ExportAstraTableIntoCsv.class);

    /**
     * Run Pipeline.
     *
     * @param args
     *      arguments
     */
    public static void main(String[] args) {
        AstraPipelineOptions astraOptions = PipelineOptionsFactory
                .fromArgs(args)
                .withValidation()
                .as(AstraPipelineOptions.class);
        Pipeline pipelineWrite = Pipeline.create(astraOptions);
        FileSystems.setDefaultPipelineOptions(astraOptions);

        // TODO
    }

}
