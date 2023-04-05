package org.apache.beam.sdk.io.astra;

import com.datastax.astra.sdk.AstraClient;
import io.stargate.sdk.grpc.domain.QueryGrpc;
import io.stargate.sdk.grpc.domain.ResultSetGrpc;
import io.stargate.sdk.grpc.domain.RowGrpc;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class SampleJavaSdk extends AbstractAstraTest {

    @Test
    public void testGrpcWithSDK() {
        try(AstraClient astraClient = AstraClient.builder()
                .withDatabaseRegion(DB_REGION)
                .withDatabaseId(DB_ID)
                .withToken(TOKEN)
                .enableGrpc()
                .build()) {
            ResultSetGrpc resGrpc = astraClient
                    .apiStargateGrpc()
                    .execute("SELECT * from ks1.user");
            for(RowGrpc row : resGrpc.getResults()) {
                System.out.println(row.getString("email"));
            }
        }
    }

}
