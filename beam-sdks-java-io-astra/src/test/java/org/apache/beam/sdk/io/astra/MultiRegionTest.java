package org.apache.beam.sdk.io.astra;


import com.datastax.oss.driver.api.core.CqlSession;
import org.junit.jupiter.api.Test;

import java.nio.file.Paths;

public class MultiRegionTest {

    public static final String BUNDLE_SINGAPORE = "/Users/cedricklunven/Downloads/ap-south-east1.zip";
    public static final String BUNDLE_MUMBAI    = "/Users/cedricklunven/Downloads/ap-south-1.zip";
    public static final String ASTRA_PASSWORD   = "AstraCS:IjjSfFRRunnQZBxoAWzCvumX:0898b151bd33eb7f5bafe4ab97c0cea15318c04cf03530beb788c698b557193b";
    public static final String ASTRA_KEYSPACE   = "ks";

    @Test
    public void testWriteInSingapore() {
        try (CqlSession cqlSession = CqlSession.builder()
                .withCloudSecureConnectBundle(Paths.get(BUNDLE_SINGAPORE))
                .withAuthCredentials("token", ASTRA_PASSWORD)
                .withKeyspace(ASTRA_KEYSPACE)
                .build()) {
            cqlSession.execute("CREATE TABLE IF NOT EXISTS foo(bar1 text primary key, bar2 text);");
            cqlSession.execute("INSERT INTO foo(bar1, bar2) VALUES ('1', 'test');");
            cqlSession.execute("INSERT INTO foo(bar1, bar2) VALUES ('2', 'test2');");
            cqlSession.execute("INSERT INTO foo(bar1, bar2) VALUES ('3', 'test3');");
        }
    }

    @Test
    public void testReadInMumbai() {
        try (CqlSession cqlSession = CqlSession.builder()
                .withCloudSecureConnectBundle(Paths.get(BUNDLE_MUMBAI))
                .withAuthCredentials("token", ASTRA_PASSWORD)
                .withKeyspace(ASTRA_KEYSPACE)
                .build()) {
            cqlSession.execute("SELECT * FROM foo").all().stream().forEach(row -> {
                System.out.println(String.format("Row bar1=%s and bar2=%s", row.getString("bar1"),row.getString("bar2") ));
            });
        }
    }
}
