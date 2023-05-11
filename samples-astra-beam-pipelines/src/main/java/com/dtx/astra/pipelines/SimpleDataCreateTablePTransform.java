package com.dtx.astra.pipelines;

import com.datastax.driver.core.*;
import com.datastax.driver.core.schemabuilder.SchemaBuilder;
import com.dtx.astra.pipelines.beam.BulkDataLoadWithBeam;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

/**
 * Create a table if needed before Insertion.
 */
public class SimpleDataCreateTablePTransform<T> extends PTransform<PCollection<T>, PCollection<T>> {

    /**
     * Logger for the class.
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(BulkDataLoadWithBeam.class);

    /**
     * Astra options.
     *
     * @param token
     *      authentication token
     * @param scb
     *      secure connect bundle
     * @param keyspace
     *      target keyspace
     */
    public SimpleDataCreateTablePTransform(String token, String scb, String keyspace) {
        try(Cluster cluster = Cluster.builder()
                .withAuthProvider(new PlainTextAuthProvider("token", token))
                .withCloudSecureConnectBundle(new File(scb))
                .withSocketOptions(new SocketOptions().setConnectTimeoutMillis(20000).setReadTimeoutMillis(20000))
                .build()) {
            try(Session session = cluster.connect(keyspace)) {
                LOGGER.info("+ Connected to Astra (pre-check)");
                createTableSimpleData(session);
            }
        }
        LOGGER.info("+ Table has been created (if needed)");
    }

    private static void createTableSimpleData(Session session) {
        session.execute(SchemaBuilder.createTable("simpledata")
                .addPartitionKey("id", DataType.cint())
                .addColumn("data", DataType.text())
                .ifNotExists());
    }

    @Override
    public PCollection<T> expand(PCollection<T> input) {
        return input;
    }

}
