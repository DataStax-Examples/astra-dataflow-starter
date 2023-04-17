/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.sdk.io.astra.cql;

import com.datastax.driver.core.*;
import org.apache.beam.sdk.io.astra.AstraIO.Read;
import org.apache.beam.sdk.io.astra.ConnectionManager;
import org.apache.beam.sdk.io.astra.Mapper;
import org.apache.beam.sdk.io.astra.RingRange;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Joiner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigInteger;
import java.util.Collections;
import java.util.Iterator;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Read function from Astra using Cql interface.
 *
 * @param <T>
 *     current bean
 */
class CqlReadFn<T> extends DoFn<AstraCqlRead<T>, T> {

  /** Logger for the class. */
  private static final Logger LOG = LoggerFactory.getLogger(CqlReadFn.class);

  /** {@inheritDoc}. */
  @ProcessElement
  public void processElement(@Element AstraCqlRead<T> read, OutputReceiver<T> receiver) {
    try {
      // Load existing or opening Session
      Session session = CqlConnectionManager.getSession(read);
      Mapper<T> mapper = read.getMapperFactoryFn().apply(session);
      String partitionKey = session.getCluster().getMetadata().getKeyspace(read.getKeyspace().get())
              .getTable(read.getTable().get()).getPartitionKey().stream()
              .map(ColumnMetadata::getName)
              .collect(Collectors.joining(","));

      String query = generateRangeQuery(read, partitionKey, read.getRingRanges() != null);
      PreparedStatement preparedStatement = session.prepare(query);
      Set<RingRange> ringRanges = read.getRingRanges() == null ? Collections.emptySet() : read.getRingRanges().get();

      for (RingRange rr : ringRanges) {
        Token startToken = session.getCluster().getMetadata().newToken(rr.getStart().toString());
        Token endToken = session.getCluster().getMetadata().newToken(rr.getEnd().toString());
        if (rr.isWrapping()) {
          // A wrapping range is one that overlaps from the end of the partitioner range and its
          // start (ie : when the start token of the split is greater than the end token)
          // We need to generate two queries here : one that goes from the start token to the end
          // of
          // the partitioner range, and the other from the start of the partitioner range to the
          // end token of the split.
          outputResults(
              session.execute(getLowestSplitQuery(read, partitionKey, rr.getEnd())),
              receiver,
              mapper);
          outputResults(
              session.execute(getHighestSplitQuery(read, partitionKey, rr.getStart())),
              receiver,
              mapper);
        } else {
          ResultSet rs =
              session.execute(
                  preparedStatement.bind().setToken(0, startToken).setToken(1, endToken));
          outputResults(rs, receiver, mapper);
        }
      }

      if (read.getRingRanges() == null) {
        ResultSet rs = session.execute(preparedStatement.bind());
        outputResults(rs, receiver, mapper);
      }
    } catch (Exception ex) {
      LOG.error("error", ex);
    }
  }

  private static <T> void outputResults(
      ResultSet rs, OutputReceiver<T> outputReceiver, Mapper<T> mapper) {
    Iterator<T> iter = mapper.map(rs);
    while (iter.hasNext()) {
      T n = iter.next();
      outputReceiver.output(n);
    }
  }

  private static String getHighestSplitQuery(
          AstraCqlRead<?> spec, String partitionKey, BigInteger highest) {
    String highestClause = String.format("(token(%s) >= %d)", partitionKey, highest);
    String finalHighQuery =
        (spec.getQuery() == null)
            ? buildInitialQuery(spec, true) + highestClause
            : spec.getQuery() + " AND " + highestClause;
    LOG.debug("CassandraIO generated a wrapAround query : {}", finalHighQuery);
    return finalHighQuery;
  }

  private static String getLowestSplitQuery(AstraCqlRead<?> spec, String partitionKey, BigInteger lowest) {
    String lowestClause = String.format("(token(%s) < %d)", partitionKey, lowest);
    String finalLowQuery =
        (spec.getQuery() == null)
            ? buildInitialQuery(spec, true) + lowestClause
            : spec.getQuery() + " AND " + lowestClause;
    LOG.debug("CassandraIO generated a wrapAround query : {}", finalLowQuery);
    return finalLowQuery;
  }

  private static String generateRangeQuery(AstraCqlRead<?> spec, String partitionKey, Boolean hasRingRange) {
    final String rangeFilter = hasRingRange
            ? Joiner.on(" AND ")
                .skipNulls()
                .join(
                    String.format("(token(%s) >= ?)", partitionKey),
                    String.format("(token(%s) < ?)", partitionKey))
            : "";
    final String combinedQuery = buildInitialQuery(spec, hasRingRange) + rangeFilter;
    LOG.debug("CassandraIO generated query : {}", combinedQuery);
    return combinedQuery;
  }

  /**
   * Generate CQL query when not provided (READ ALL)
   *
   * @param spec
   *    current read
   * @param hasRingRange
   *    ring range is enabled
   * @return
   *    first CQL
   */
  private static String buildInitialQuery(AstraCqlRead<?> spec, Boolean hasRingRange) {
    String query = String.format("SELECT * FROM %s.%s", spec.getKeyspace().get(), spec.getTable().get());
    if (spec.getQuery() != null) {
      query = spec.getQuery().get();
    }
    if (hasRingRange) {
      query+= query.contains("WHERE") ? " AND " : " WHERE ";
    }
    return query;
  }

}
