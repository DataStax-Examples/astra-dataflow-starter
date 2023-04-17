package org.apache.beam.sdk.io.astra;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.Serializable;
import java.util.List;

/**
 * Testing for AstraIO using CQL.
 */
@RunWith(JUnit4.class)
public class AstraIOTest implements Serializable {

    // --> Static Configuration
    public static final String TOKEN            = "AstraCS:uZclXTYecCAqPPjiNmkezapR:e87d6edb702acd87516e4ef78e0c0e515c32ab2c3529f5a3242688034149a0e4";
    public static final String ASTRA_ZIP_FILE   = "/Users/cedricklunven/Downloads/scb-demo.zip";
    public static final String ASTRA_KEYSPACE   = "demo";
    // <--

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
    public void shouldWriteIntoAstra() {
        LOG.info("WRITE DATA INTO ASTRA");
        pipelineWrite.apply(Create.of(AstraIOTestUtils.generateTestData(100)))
                .apply(AstraIO.<SimpleDataEntity>write()
                        .withToken(TOKEN)
                        .withSecureConnectBundleFile(new File(ASTRA_ZIP_FILE))
                        .withKeyspace(ASTRA_KEYSPACE)
                        .withEntity(SimpleDataEntity.class));
        pipelineWrite.run().waitUntilFinish();

        List<Row> results = session.execute("SELECT * FROM simpledata").all();
        for (Row r : results) {
            LOG.info("id={} and data={}", r.getInt("id"), r.getString("data"));
        }
    }

    @Test
    public void shouldReadFromAstra() {
        LOG.info("READ DATA FROM ASTRA");
        pipelineRead = TestPipeline.create();
        LOG.info("+ Pipeline created");
        PCollection<SimpleDataEntity> simpleDataPCollection =
                pipelineRead.apply(org.apache.beam.sdk.io.astra.AstraIO.<SimpleDataEntity>read()
                                .withToken(TOKEN)
                                .withSecureConnectBundleFile(new File(ASTRA_ZIP_FILE))
                                .withKeyspace(ASTRA_KEYSPACE)
                                .withTable("simpledata")
                                .withMinNumberOfSplits(50)
                                .withCoder(SerializableCoder.of(SimpleDataEntity.class))
                                .withEntity(SimpleDataEntity.class));
        // Results ?
        PAssert.thatSingleton(simpleDataPCollection.apply("Count", Count.globally())).isEqualTo(2L);

        // Count number of items
        //PAssert.thatMap()
        //simpleDataPCollection.
        //LOG.info("+ OK");
    }

    @Test
    public void shouldReadWithQueryFromAstra() {

    }

    @AfterClass
    public static void afterClass() {
        if (session!= null) session.close();
        if (cluster!= null) cluster.close();
    }

}
