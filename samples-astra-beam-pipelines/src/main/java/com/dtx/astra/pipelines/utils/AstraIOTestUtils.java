package com.dtx.astra.pipelines.utils;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.dtx.astra.pipelines.SimpleDataEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Shared Code
 */
public class AstraIOTestUtils {

    /** Logger for the Class. */
    private static final Logger LOG = LoggerFactory.getLogger(AstraIOTestUtils.class);

    public static final Cluster createCluster(String secureConnect, String token) {
        LOG.info("INITIALIZATION, Please Wait...");
        Cluster cluster = Cluster.builder()
                .withCloudSecureConnectBundle(new File(secureConnect))
                .withCredentials("token", token)
                .build();
        LOG.info("+ Cluster Created");
        return cluster;
    }
    public static final void createTable(Session session) {
        session.execute("CREATE TABLE IF NOT EXISTS simpledata (id int PRIMARY KEY, data text);");
        LOG.info("+ Table created (if needed).");
    }

    public static final void truncateTable(Session session) {
        session.execute("TRUNCATE TABLE simpledata;");
        LOG.info("+ Table truncated");
    }

    public static final List<SimpleDataEntity> generateTestData(int numRows) {
        return IntStream
                .range(1, numRows)
                .boxed()
                .map(i -> new SimpleDataEntity(i, "something funny with " + i))
                .collect(Collectors.toList());
    }

}
