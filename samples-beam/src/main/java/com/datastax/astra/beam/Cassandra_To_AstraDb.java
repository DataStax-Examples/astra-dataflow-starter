package com.datastax.astra.beam;

import com.datastax.astra.beam.cassandra.LanguageCodeCassandra;
import com.datastax.astra.beam.lang.LanguageCode;
import com.datastax.astra.beam.lang.LanguageCodeDaoMapperFactoryFn;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.google.common.io.Resources;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.io.astra.db.AstraDbIO;
import org.apache.beam.sdk.io.astra.db.options.AstraDbWriteOptions;
import org.apache.beam.sdk.io.astra.db.utils.AstraSecureConnectBundleUtils;
import org.apache.beam.sdk.io.cassandra.CassandraIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Scanner;

/**
 * Copy a table from Cassandra to AstraDB.
 *

 export ASTRA_TOKEN=`astra token`
 export ASTRA_SCB_PATH=/Users/cedricklunven/Downloads/beam_integration_test_scb.zip
 export ASTRA_KEYSPACE=beam

 mvn clean compile exec:java \
 -Dexec.mainClass=com.datastax.astra.beam.Cassandra_To_AstraDb \
 -Dexec.args="\
 --astraToken=${ASTRA_TOKEN} \
 --astraSecureConnectBundle=${ASTRA_SCB_PATH} \
 --astraKeyspace=${ASTRA_KEYSPACE} \
 --cassandraHost=localhost \
 --cassandraKeyspace=demo \
 --cassandraTableName=languages \
 --cassandraPort=9042 \
 --tableName=languages"

 */
public class Cassandra_To_AstraDb {

    /**
     * Work with CQL and Astra.
     */
    private static final Logger LOG = LoggerFactory.getLogger(Cassandra_To_AstraDb.class);

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

        @Validation.Required
        String getCassandraTableName();

        @SuppressWarnings("unused")
        void setCassandraTableName(String tableName);

        @Validation.Required
        String getCassandraKeyspace();

        @SuppressWarnings("unused")
        void setCassandraKeyspace(String keyspace);
    }


    public static void main(String[] args) throws IOException {

        // Parse and Validate Parameters*
        CassandraToAstraDbOptions options = PipelineOptionsFactory
                .fromArgs(args).withValidation()
                .as(CassandraToAstraDbOptions.class);
        LOG.info("Options have been parsed {}", options.toString());

        byte[] scbZip = AstraSecureConnectBundleUtils
                .loadFromFilePath(options.getAstraSecureConnectBundle());

        // Create Keyspace, Table and insert a few rows
        initializeCassandraDataSet(
                options.getCassandraHost(),
                options.getCassandraPort(),
                options.getCassandraKeyspace());
        LOG.info("Cassandra DataSet loaded");

        try {
            // Source Cassandra
            CassandraIO.Read<LanguageCodeCassandra> cassandraSource = CassandraIO.<LanguageCodeCassandra>read()
                .withHosts(Collections.singletonList(options.getCassandraHost()))
                .withPort(options.getCassandraPort())
                .withKeyspace(options.getCassandraKeyspace())
                .withTable(options.getCassandraTableName())
                .withCoder(SerializableCoder.of(LanguageCodeCassandra.class))
                .withEntity(LanguageCodeCassandra.class);
            LOG.info("Cassandra Read [OK]");

            // Astra Destination
            AstraDbIO.Write<LanguageCode> astraSink = AstraDbIO.<LanguageCode>write()
                    .withToken(options.getAstraToken())
                    .withSecureConnectBundle(scbZip)
                    .withKeyspace(options.getAstraKeyspace())
                    .withMapperFactoryFn(new LanguageCodeDaoMapperFactoryFn())
                    .withEntity(LanguageCode.class);
            LOG.info("Astra Sink [OK]");

            Pipeline cassandraToAstraDbPipeline = Pipeline.create(options);
            cassandraToAstraDbPipeline
                    .apply("Read From Cassandra", cassandraSource)
                    .apply("Pojo Mapping", MapElements.via(new MapFromCassandraToAstraBean()))
                    .apply("Write to Astra", astraSink);

            PipelineResult result = cassandraToAstraDbPipeline.run();
            result.waitUntilFinish(Duration.standardSeconds(10));
            System.exit(0);
        } finally {
            AstraDbIO.close();
        }
    }

    static class MapFromCassandraToAstraBean extends SimpleFunction<LanguageCodeCassandra, LanguageCode> {

        @Override
        public LanguageCode apply(LanguageCodeCassandra cassieBean) {
            LanguageCode astraBean = new LanguageCode();
            astraBean.setCode(cassieBean.getCode());
            astraBean.setLanguage(cassieBean.getLanguage());
            LOG.info("Mapping {}", cassieBean.getCode());
            return astraBean;
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
