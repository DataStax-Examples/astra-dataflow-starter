package org.apache.beam.sdk.io.astra.cql;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import org.apache.beam.sdk.io.astra.AstraIO;
import org.apache.beam.sdk.io.astra.AstraIOTestUtils;
import org.apache.beam.sdk.io.astra.SimpleDataEntity;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.Serializable;

public class AstraCqlIOTest implements Serializable {

        // --> Static Configuration
        public static final String TOKEN            = "AstraCS:uZclXTYecCAqPPjiNmkezapR:e87d6edb702acd87516e4ef78e0c0e515c32ab2c3529f5a3242688034149a0e4";
        public static final String ASTRA_ZIP_FILE   = "/Users/cedricklunven/Downloads/scb-demo.zip";
        public static final String ASTRA_KEYSPACE   = "demo";
        // <--

    public static final String TABLE = "simpledata";


    /** Logger for the Class. */
    private static final Logger LOG = LoggerFactory.getLogger(AstraIO.class);

    /**
     * Pipeline reference
     */
    @Rule
    public transient TestPipeline pipelineWrite = TestPipeline.create();

    /**
     * Pipeline reference
     */
    @Rule
    public transient TestPipeline pipelineRead = TestPipeline.create();

    /**
     * Cassandra control connection
     */
    private static Cluster cluster;
    private static Session session;

    @BeforeClass
    public static void beforeClass() {
        cluster = AstraIOTestUtils.createCluster(new File(ASTRA_ZIP_FILE), TOKEN);
        session = cluster.connect(ASTRA_KEYSPACE);
        AstraIOTestUtils.createTable(session);
        AstraIOTestUtils.truncateTable(session);
    }

    @Test
    public void shouldWriteDataToAstra() {
        LOG.info("WRITE DATA TO ASTRA");

        // Create a writer
        AstraCqlWrite<SimpleDataEntity> writer = AstraCqlWrite.<SimpleDataEntity>builder()
                .withToken(TOKEN)
                .withSecureConnectBundle(new File(ASTRA_ZIP_FILE))
                .withKeyspace(ASTRA_KEYSPACE)
                .build();

        pipelineWrite = TestPipeline.create();
        pipelineWrite.apply(Create.of(AstraIOTestUtils.generateTestData(100)))
                     .apply(writer);
        pipelineWrite.run().waitUntilFinish();
    }


}
