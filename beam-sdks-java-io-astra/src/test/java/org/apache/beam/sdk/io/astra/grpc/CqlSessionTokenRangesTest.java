package org.apache.beam.sdk.io.astra.grpc;

import com.datastax.astra.sdk.AstraClient;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.metadata.token.TokenRange;
import org.junit.Test;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;

public class CqlSessionTokenRangesTest extends AbstractAstraTest {

    /** Minimum Token. */
    public static final BigInteger MIN = new BigInteger("2").pow(63).negate();

    /** Maximum Token. */
    public static final BigInteger MAX = new BigInteger("2").pow(63).subtract(BigInteger.ONE);

    @Test
    public void testCqlWithSDK() {
        try(AstraClient astraClient = AstraClient.builder()
                .withDatabaseRegion(DB_REGION)
                .withDatabaseId(DB_ID)
                .withToken(TOKEN)
                .enableCql().enableDownloadSecureConnectBundle()
                .build()) {

            CqlSession cqlSession = astraClient.cqlSession();

            List<Row> rows = new ArrayList<Row>();
            int i=0;
            boolean first = true;
            for (TokenRange subRange : cqlSession.getMetadata().getTokenMap().get().getTokenRanges()) {

                if (first) {
                    ResultSet rs = cqlSession.execute(
                            "SELECT * FROM demo.cities_by_country " +
                                    "WHERE token(country_name) >= ? and token(country_name) <= ?",
                            MIN, subRange.getStart());
                    System.out.println(MIN + "-" + subRange.getStart());
                    first = false;
                    rows.addAll(rs.all());
                }

                ResultSet rs = cqlSession.execute(
                        "SELECT * FROM demo.cities_by_country" +
                               " WHERE token(country_name) >= ? and token(country_name) <= ?",
                       subRange.getStart(),
                        subRange.getStart().compareTo(subRange.getEnd()) > 0 ? MAX : subRange.getEnd());
                rows.addAll(rs.all());
            }

            for(Row row: rows) {
                System.out.println(row.getString("country_name"));
            }
        }
    }
}
