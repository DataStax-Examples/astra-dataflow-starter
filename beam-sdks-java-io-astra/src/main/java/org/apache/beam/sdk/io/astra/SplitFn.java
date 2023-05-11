package org.apache.beam.sdk.io.astra;

import com.datastax.driver.core.Cluster;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableSet;

import java.math.BigInteger;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Split read per Token Range.
 *
 * @param <T>
 *     working entity
 */
public class SplitFn<T> extends DoFn<AstraIO.Read<T>, AstraIO.Read<T>> {

    /**
     * For each token ranges, update output receiver.
     * @param read
     * @param outputReceiver
     */
    @ProcessElement
    public void process(@Element AstraIO.Read<T> read, OutputReceiver<AstraIO.Read<T>> outputReceiver) {
        ((Set<RingRange>) getRingRanges(read)).stream()
                .map(ImmutableSet::of)
                .map(read::withRingRanges)
                .forEach(outputReceiver::output);
    }

    private static <T> Set<RingRange> getRingRanges(AstraIO.Read<T> read) {
        try (Cluster cluster = AstraConnectionManager.getInstance().getCluster(
                             read.token(),
                             read.consistencyLevel(),
                             read.connectTimeout(),
                             read.readTimeout(),
                             read.secureConnectBundle(),
                             read.secureConnectBundleData())) {

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