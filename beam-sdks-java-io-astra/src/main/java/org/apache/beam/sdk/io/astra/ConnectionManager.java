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
package org.apache.beam.sdk.io.astra;

import com.datastax.driver.core.*;

import java.io.File;
import java.io.InputStream;
import java.net.URL;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.beam.sdk.io.astra.AstraIO.Read;
import org.apache.beam.sdk.options.ValueProvider;

@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class ConnectionManager {

  private static final ConcurrentHashMap<String, Cluster> clusterMap = new ConcurrentHashMap<String, Cluster>();
  private static final ConcurrentHashMap<String, Session> sessionMap = new ConcurrentHashMap<String, Session>();

  static {
    Runtime.getRuntime()
        .addShutdownHook(
            new Thread(
                () -> {
                  for (Session session : sessionMap.values()) {
                    if (!session.isClosed()) {
                      session.close();
                    }
                  }
                }));
  }

  private static String readToClusterHash(Read<?> read) {
    return Objects.requireNonNull(read.token()).get()
            + safeVPGet(read.consistencyLevel());
  }

  private static String readToSessionHash(Read<?> read) {
    return readToClusterHash(read) + read.keyspace().get();
  }

  static Session getSession(Read<?> read) {
    Cluster cluster =
        clusterMap.computeIfAbsent(
            readToClusterHash(read),
            k ->
                org.apache.beam.sdk.io.astra.AstraIO.getCluster(
                    read.token(),
                    read.consistencyLevel(),
                    read.connectTimeout(),
                    read.readTimeout(),
                    read.secureConnectBundleFile(),
                    read.secureConnectBundleUrl(),
                    read.secureConnectBundleStream()));
    return sessionMap.computeIfAbsent(
        readToSessionHash(read),
        k -> cluster.connect(Objects.requireNonNull(read.keyspace()).get()));
  }

  private static String safeVPGet(ValueProvider<String> s) {
    return s != null ? s.get() : "";
  }

  /**
   * Get a Astra cluster using either hosts and port or cloudSecureBundle.
   *
   * @param token
   *    token (or clientSecret)
   * @param consistencyLevel
   *    consistency level
   * @param connectTimeout
   *    connection timeout
   * @param readTimeout
   *    read timeout
   * @param scbFile
   *    read scb as a file
   * @param scbUrl
   *    read scb as an url
   * @param scbStream
   *    read scb as stream
   * @return
   *    cassandra cluster
   */
  public static Cluster getCluster(
          ValueProvider<String> token,
          ValueProvider<String> consistencyLevel,
          ValueProvider<Integer> connectTimeout,
          ValueProvider<Integer> readTimeout,
          ValueProvider<File> scbFile,
          ValueProvider<URL> scbUrl,
          ValueProvider<InputStream> scbStream) {

    Cluster.Builder builder = Cluster.builder();

    if (scbFile != null) {
      builder.withCloudSecureConnectBundle(scbFile.get());
    } else if (scbUrl != null) {
      builder.withCloudSecureConnectBundle(scbUrl.get());
    } else if (scbStream != null) {
      builder.withCloudSecureConnectBundle(scbStream.get());
    } else {
      throw new IllegalArgumentException("Cloud Secure Bundle is Required");
    }
    builder.withAuthProvider(new PlainTextAuthProvider("token", token.get()));
    if (consistencyLevel != null) {
      builder.withQueryOptions(new QueryOptions().setConsistencyLevel(ConsistencyLevel.valueOf(consistencyLevel.get())));
    }

    SocketOptions socketOptions = new SocketOptions();
    builder.withSocketOptions(socketOptions);

    if (connectTimeout != null) {
      socketOptions.setConnectTimeoutMillis(connectTimeout.get());
    }

    if (readTimeout != null) {
      socketOptions.setReadTimeoutMillis(readTimeout.get());
    }

    return builder.build();
  }
}
