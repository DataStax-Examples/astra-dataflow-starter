package com.datastax.astra.beam;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.google.common.io.Resources;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.io.astra.db.AstraDbConnectionManager;
import org.apache.beam.sdk.io.astra.db.AstraDbIO;
import org.apache.beam.sdk.io.astra.db.options.AstraDbWriteOptions;
import org.apache.beam.sdk.io.cassandra.CassandraIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Scanner;

/**
 * Copy a table from Cassandra to AstraDB.
 *

 mvn clean compile exec:java \
 -Dexec.mainClass=com.datastax.astra.beam.Cassandra_To_AstraDb \
 -Dexec.args="\
 --astraToken=${ASTRA_TOKEN} \
 --astraSecureConnectBundle=${ASTRA_SCB_PATH} \
 --keyspace=${ASTRA_KEYSPACE} \
 --cassandraHost=localhost \
 --cassandraPort=9042 \
 --tableName=languages"

 */
public class Cassandra_To_AstraDb {

    /**
     * Interface definition of parameters needed for this pipeline.
     */
    public interface CassandraToAstraDbOptions extends AstraDbWriteOptions {

        @Validation.Required
        String getCassandraHost();

        @SuppressWarnings("unused")
        void setCassandraHost(String host);

        @Validation.Required
        int getCassandraPort();

        @SuppressWarnings("unused")
        void setCassandraPort(int host);

        @Validation.Required
        String getTableName();

        @SuppressWarnings("unused")
        void setTableName(String tableName);
    }

    public static void main(String[] args) throws IOException {

        // Parse and Validate Parameters*
        CassandraToAstraDbOptions options = PipelineOptionsFactory
                .fromArgs(args).withValidation()
                .as(CassandraToAstraDbOptions.class);

        // Create Keyspace, Table and insert a few rows
        initializeCassandraDataSet(
                options.getCassandraHost(),
                options.getCassandraPort(),
                options.getKeyspace());

        try {
            // Source Cassandra
            CassandraIO.Read<LanguageCode> cassandraSource = CassandraIO.<LanguageCode>read()
                .withHosts(Collections.singletonList(options.getCassandraHost()))
                .withPort(options.getCassandraPort())
                .withKeyspace(options.getKeyspace())
                .withTable(options.getTableName())
                .withCoder(SerializableCoder.of(LanguageCode.class))
                .withEntity(LanguageCode.class);

            // Sink Astra
            AstraDbIO.Write<LanguageCode> astraSink = AstraDbIO.<LanguageCode>write()
                    .withToken(options.getAstraToken())
                    .withSecureConnectBundle(new File(options.getAstraSecureConnectBundle()))
                    .withKeyspace(options.getKeyspace())
                    .withEntity(LanguageCode.class);

            Pipeline cassandraToAstraDbPipeline = Pipeline.create(options);
            cassandraToAstraDbPipeline
                    .apply("Read From Cassandra", cassandraSource)
                    .apply("Write to Astra", astraSink);
            cassandraToAstraDbPipeline.run().waitUntilFinish();
        } finally {
            AstraDbConnectionManager.cleanup();
        }
    }

    private static final void initializeCassandraDataSet(String host, int port, String keyspace) throws IOException {
        try(Cluster c = Cluster.builder()
                .withPort(port)
                .addContactPoint(host).build()) {
            try(Session adminSession = c.connect()) {
                adminSession.execute("CREATE KEYSPACE IF NOT EXISTS " + keyspace +
                        " WITH REPLICATION = { " +
                        "'class' : 'SimpleStrategy', " +
                        "'replication_factor' : 1 " +
                        "} ");
            }
            try(Session session = c.connect(keyspace)) {
                session.execute(LanguageCode.cqlCreateTable());
                PreparedStatement ps = session.prepare("INSERT INTO languages (code, language) VALUES (?, ?)");
                String text = Resources.toString(Resources.getResource("language-codes.csv"), StandardCharsets.UTF_8);
                try (Scanner scanner = new Scanner(text)) {
                    while (scanner.hasNextLine()) {
                        String[] chunks = scanner.nextLine().split(",");
                        session.execute(ps.bind(chunks[0], chunks[1]));
                    }
                }
            }
        }
    }


}
