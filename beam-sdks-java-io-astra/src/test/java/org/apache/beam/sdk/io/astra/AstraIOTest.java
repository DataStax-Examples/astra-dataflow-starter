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
import org.junit.*;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.junit.runners.MethodSorters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.Serializable;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;

/**
 * Testing for AstraIO using CQL.
 */
@RunWith(JUnit4.class)
public class AstraIOTest extends AbstractAstraTest implements Serializable {

    /** Logger for the Class. */
    private static final Logger LOG = LoggerFactory.getLogger(AstraIO.class);

    /**
     * Pipeline reference
     */
    @Rule
    public transient TestPipeline pipeline = TestPipeline.create();

    /**
     * Cassandra control connection
     */
    private static Cluster cluster;
    private static Session session;

    @BeforeClass
    public static void beforeClass() throws Exception {
        LOG.info("INITIALIZATION, Please Wait...");
        cluster = Cluster.builder()
                .withCloudSecureConnectBundle(new File(ASTRA_ZIP_FILE))
                .withCredentials("token", TOKEN)
                .build();
        LOG.info("+ Cluster Created");
        session = cluster.connect(ASTRA_KEYSPACE);
        LOG.info("+ Connected to Keyspace {}" ,session.getLoggedKeyspace());

        session.execute("CREATE TABLE IF NOT EXISTS simpledata (id int PRIMARY KEY, data text);");
        LOG.info("+ Table created (if needed).");

        session.execute("TRUNCATE TABLE simpledata;");
        LOG.info("+ Table truncated");
    }

    @Test
    public void shouldWriteIntoAstra() {
        LOG.info("WRITE DATA INTO ASTRA");
        int numRow = 10;
        List<SimpleDataEntity> data = IntStream
                 .range(1, numRow)
                 .boxed()
                 .map(i -> new SimpleDataEntity(i, "something funny with " + i))
                .collect(Collectors.toList());

        pipeline.apply(Create.of(data))
                .apply(AstraIO.<SimpleDataEntity>write()
                        .withToken(TOKEN)
                        .withCloudSecureConnectBundle(ASTRA_ZIP_FILE)
                        .withKeyspace(ASTRA_KEYSPACE)
                        .withEntity(SimpleDataEntity.class));
        pipeline.run();

        List<Row> results = session.execute("SELECT * FROM simpledata").all();
        for (Row r : results) {
            LOG.info("id={} and data={}", r.getInt("id"), r.getString("data"));
        }
    }

    @Test
    public void shouldReadFromAstra() {
        System.out.println("Test Read");
        TestPipeline pipeline = TestPipeline.create();
        System.out.println("+ Pipeline created");
        PCollection<SimpleDataEntity> simpleDataPCollection =
                pipeline.apply(AstraIO.<SimpleDataEntity>read()
                                .withToken(TOKEN)
                                .withCloudSecureConnectBundle(ASTRA_ZIP_FILE)
                                .withKeyspace(ASTRA_KEYSPACE)
                                .withTable("simpledata")
                                .withMinNumberOfSplits(50)
                                .withCoder(SerializableCoder.of(SimpleDataEntity.class))
                                .withEntity(SimpleDataEntity.class));
        PAssert.thatSingleton(simpleDataPCollection.apply("Count", Count.globally())).isEqualTo(2L);
        System.out.println(simpleDataPCollection.expand());
    }

    @AfterClass
    public static void afterClass() throws Exception {
        if (session!= null) session.close();
        if (cluster!= null) cluster.close();
    }

}
