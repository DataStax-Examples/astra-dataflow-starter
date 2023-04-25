package org.apache.beam.sdk.io.astra;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.PipelineOptionsValidator;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.beam.sdk.io.astra.AstraIOTestUtils.*;

/**
 * Test as a Pipeline
 *
 * To run the test:
 * mvn test \
 *   -Dtest=org.apache.beam.sdk.io.astra.AstraIOIT \
 * -DbeamTestPipelineOptions='["--secureConnectBundle=/scb-demo.zip",
 * "--token=AstraCS:uZclXTYecCAqP....","--keyspace=demo"]'  \
 *   -DintegrationTestRunner=direct
 */
public class AstraIOIT {

    /** Logger for the Class. */
    private static final Logger LOG = LoggerFactory.getLogger(AstraIOIT.class);

    /**
     * Specific Options for the Astra Pipeline
     */
    private static AstraIOTestOptions astraOptions;

    @Rule
    public transient TestPipeline pipelineWrite = TestPipeline.create();

    @Rule
    public transient TestPipeline pipelineRead = TestPipeline.create();

    @BeforeClass
    public static void setup() {
        PipelineOptionsFactory.register(AstraIOTestOptions.class);
        PipelineOptions options = TestPipeline.testingPipelineOptions().as(AstraIOTestOptions.class);
        astraOptions = PipelineOptionsValidator.validate(AstraIOTestOptions.class, options);
        Cluster cluster = createCluster(astraOptions.getSecureConnectBundle(), astraOptions.getToken());
        Session session = cluster.connect(astraOptions.getKeyspace());
        createTable(session);
        truncateTable(session);
    }

    @Test
    public void testWriteThenRead() {
        // Write Data in Astra
        pipelineWrite.apply(Create.of(AstraIOTestUtils.generateTestData(100)))
                .apply(org.apache.beam.sdk.io.astra.AstraIO.<SimpleDataEntity>write()
                        .withToken(astraOptions.getToken())
                        .withSecureConnectBundle(astraOptions.getSecureConnectBundle())
                        .withKeyspace(astraOptions.getKeyspace())
                        .withEntity(SimpleDataEntity.class));
        pipelineWrite.run().waitUntilFinish();
    }


}
