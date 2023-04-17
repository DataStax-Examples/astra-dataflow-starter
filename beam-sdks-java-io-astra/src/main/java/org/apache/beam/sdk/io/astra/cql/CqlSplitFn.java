package org.apache.beam.sdk.io.astra.cql;

import com.datastax.driver.core.Cluster;
import org.apache.beam.sdk.io.astra.ConnectionManager;
import org.apache.beam.sdk.io.astra.RingRange;
import org.apache.beam.sdk.io.astra.SplitGenerator;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableSet;

import java.math.BigInteger;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class CqlSplitFn<T> extends DoFn<AstraCqlRead<T>, AstraCqlRead<T>> {

    @ProcessElement
    public void process(@Element AstraCqlRead<T> read, OutputReceiver<AstraCqlRead<T>> outputReceiver) {
        Set<RingRange> ringRanges = getRingRanges(read);
        for (RingRange rr : ringRanges) {
            outputReceiver.output(read.withRingRanges(ImmutableSet.of(rr)));
        }
    }

    private static <T> Set<RingRange> getRingRanges(AstraCqlRead<T> read) {
        Cluster cluster = CqlConnectionManager.getCluster(read);
        Integer splitCount;
        if (read.getMinNumberOfSplits() != null && read.getMinNumberOfSplits().get() != null) {
            splitCount = read.getMinNumberOfSplits().get();
        } else {
            splitCount = cluster.getMetadata().getAllHosts().size();
        }
        List<BigInteger> tokens = cluster
                .getMetadata().getTokenRanges().stream()
                .map(tokenRange -> new BigInteger(tokenRange.getEnd().getValue().toString()))
                .collect(Collectors.toList());
        return new SplitGenerator(cluster.getMetadata().getPartitioner()).generateSplits(splitCount, tokens).stream()
                    .flatMap(List::stream)
                    .collect(Collectors.toSet());
    }
}