package org.apache.beam.sdk.io.astra;

import com.datastax.driver.core.Cluster;
import org.apache.beam.sdk.io.astra.cql.AstraCqlRead;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableSet;

import java.math.BigInteger;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class SplitFn<T> extends DoFn<AstraIO.Read<T>, AstraIO.Read<T>> {

    @ProcessElement
    public void process(@Element AstraIO.Read<T> read, OutputReceiver<AstraIO.Read<T>> outputReceiver) {
        Set<RingRange> ringRanges = getRingRanges(read);
        for (RingRange rr : ringRanges) {
            outputReceiver.output(read.withRingRanges(ImmutableSet.of(rr)));
        }
    }

    private static <T> Set<RingRange> getRingRanges(AstraIO.Read<T> read) {
        try (Cluster cluster = ConnectionManager.getCluster(
                             read.token(),
                             read.consistencyLevel(),
                             read.connectTimeout(),
                             read.readTimeout(),
                             read.secureConnectBundleFile(),
                             read.secureConnectBundleUrl(),
                             read.secureConnectBundleStream())) {

            Integer splitCount;
            if (read.minNumberOfSplits() != null && read.minNumberOfSplits().get() != null) {
                splitCount = read.minNumberOfSplits().get();
            } else {
                splitCount = cluster.getMetadata().getAllHosts().size();
            }
            List<BigInteger> tokens =
                    cluster.getMetadata().getTokenRanges().stream()
                            .map(tokenRange -> new BigInteger(tokenRange.getEnd().getValue().toString()))
                            .collect(Collectors.toList());
            SplitGenerator splitGenerator =
                    new SplitGenerator(cluster.getMetadata().getPartitioner());

            return splitGenerator.generateSplits(splitCount, tokens).stream()
                    .flatMap(List::stream)
                    .collect(Collectors.toSet());
        }
    }
}