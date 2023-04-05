package org.apache.beam.sdk.io.cassandra;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.File;
import java.io.Serializable;
import java.util.Collections;

@RunWith(JUnit4.class)
public class AstraConnectionTest implements Serializable {

    public static final String TOKEN            = "AstraCS:IjjSfFRRunnQZBxoAWzCvumX:0898b151bd33eb7f5bafe4ab97c0cea15318c04cf03530beb788c698b557193b";
    public static final String ASTRA_ZIP_FILE   = "/Users/cedricklunven/Downloads/secure-connect-db1.zip";
    public static final String ASTRA_KEYSPACE   = "beam_ks";

    @Test
    public void testAstraConnectivity() {
        try(Cluster cluster = Cluster.builder()
                .withCloudSecureConnectBundle(new File(ASTRA_ZIP_FILE))
                .withCredentials("token", TOKEN)
                .build() ) {
            Session session = cluster.connect(ASTRA_KEYSPACE);
            System.out.println(String.format("Connected to Keyspace %s",session.getLoggedKeyspace()));
            session.execute("CREATE TABLE IF NOT EXISTS simpledata (id int PRIMARY KEY, data text);");
            System.out.println("+ Table created.");
        }
    }

    @Test
    public void testReadAgainstAstra() {
        System.out.println("Test Read");
        TestPipeline pipeline = TestPipeline.create();
        System.out.println("+ Pipeline created");
        PCollection<CassandraIOTest.SimpleData> simpleDataPCollection =
                pipeline.apply(CassandraIO.<CassandraIOTest.SimpleData>read()
                                .withCloudSecureConnectBundle(ASTRA_ZIP_FILE)
                                .withUsername("token")
                                .withPassword(TOKEN)
                                .withKeyspace(ASTRA_KEYSPACE)
                                .withTable("simpledata")
                                .withMinNumberOfSplits(50)
                                .withCoder(SerializableCoder.of(CassandraIOTest.SimpleData.class))
                                .withEntity(CassandraIOTest.SimpleData.class));
        PAssert.thatSingleton(simpleDataPCollection.apply("Count", Count.globally())).isEqualTo(2L);

    }

}
