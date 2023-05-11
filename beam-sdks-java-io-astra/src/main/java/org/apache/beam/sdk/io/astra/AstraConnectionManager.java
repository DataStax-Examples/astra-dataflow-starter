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
import org.apache.beam.sdk.io.astra.AstraIO.Read;
import org.apache.beam.sdk.options.ValueProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Formatter;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Session and Cluster should be singletons for a destination (token, keyspace).
 */
public class AstraConnectionManager {

  /**
   * Logger
   */
  private static final Logger LOG = LoggerFactory.getLogger(AstraConnectionManager.class);

  /**
   * Singleton.
   */
  private static AstraConnectionManager _instance = null;

  /**
   * Cache for clusters (token / cloud secure bundle).
   */
  private final ConcurrentHashMap<String, Cluster> cacheClusters = new ConcurrentHashMap<>();

  /**
   * Cache for sessions (cluster / keyspaces).
   */
  private final ConcurrentHashMap<String, Session> cacheSessions = new ConcurrentHashMap<>();

  /**
   * Message Digest.
   */
  private MessageDigest md;

  /**
   * Singleton Pattern
   */
  public static synchronized AstraConnectionManager getInstance() {
    if (_instance == null) {
      _instance = new AstraConnectionManager();
      try {
        _instance.md = MessageDigest.getInstance("SHA-1");
      } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException("SHA-1 is not supported");
      }
      Runtime.getRuntime().addShutdownHook(new Thread(() -> {
        for (Session session : _instance.cacheSessions.values()) {
         if (!session.isClosed()) {
           LOG.info("Closing Cassandra Session.");
           session.close();
         }
        }
      }));
    }
    return _instance;
  }

  /**
   * Retrieve an existing session.
   *
   * @param write
   *    read session
   * @return
   *    current session
   */
  public synchronized Session getSession(AstraIO.Write<?> write) {
    return getSession(write.token(),
            ValueProvider.StaticValueProvider.of(ConsistencyLevel.LOCAL_QUORUM.name()),
            write.connectTimeout(),
            write.readTimeout(),
            write.secureConnectBundle(),
            write.secureConnectBundleData(),
            write.keyspace().get());
  }

  /**
   * Retrieve an existing session.
   *
   * @param read
   *    read session
   * @return
   *    current session
   */
  public synchronized Session getSession(Read<?> read) {
    return getSession(read.token(),
            read.consistencyLevel(),
            read.connectTimeout(),
            read.readTimeout(),
            read.secureConnectBundle(),
            read.secureConnectBundleData(),
            read.keyspace().get());
  }

  public synchronized Session getSession(
          ValueProvider<String> token,
          ValueProvider<String> consistencyLevel,
          ValueProvider<Integer> connectTimeout,
          ValueProvider<Integer> readTimeout,
          ValueProvider<File> scbFile,
          ValueProvider<byte[]> scbStream,
          String keyspace) {
    Cluster cluster = getCluster(token, consistencyLevel, connectTimeout, readTimeout, scbFile, scbStream);
    String sessionSha1 = computeClusterSHA1(token.get(),scbFile,scbStream) + keyspace;
    if (!cacheSessions.containsKey(sessionSha1)) {
      LOG.info("Initializing Session.");
      cacheSessions.put(sessionSha1, cluster.connect(keyspace));
    }
    return cacheSessions.get(sessionSha1);
  }

  public synchronized Cluster getCluster(AstraIO.Write<?> write) {
    return getCluster(write.token(),
            ValueProvider.StaticValueProvider.of(ConsistencyLevel.LOCAL_QUORUM.name()),
            write.connectTimeout(),
            write.readTimeout(),
            write.secureConnectBundle(),
            write.secureConnectBundleData());
  }

  public synchronized Cluster getCluster(AstraIO.Read<?> read) {
    return getCluster(read.token(),
            read.consistencyLevel(),
            read.connectTimeout(),
            read.readTimeout(),
            read.secureConnectBundle(),
            read.secureConnectBundleData());
  }

  /**
   * Get Astra cluster using either hosts and port or cloudSecureBundle.
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
   * @param scbStream
   *    read scb as stream
   * @return
   *    cassandra cluster
   */
  public synchronized Cluster getCluster(
          ValueProvider<String> token,
          ValueProvider<String> consistencyLevel,
          ValueProvider<Integer> connectTimeout,
          ValueProvider<Integer> readTimeout,
          ValueProvider<File> scbFile,
          ValueProvider<byte[]> scbStream) {

    String clusterSha1 = computeClusterSHA1(token.get(), scbFile, scbStream);
    if (!cacheClusters.containsKey(clusterSha1)) {
      long top = System.currentTimeMillis();
      LOG.info("Initializing Cluster...");
      Cluster.Builder builder = Cluster.builder();
      if (scbFile != null) {
        builder.withCloudSecureConnectBundle(scbFile.get());
      } else if (scbStream != null) {
        builder.withCloudSecureConnectBundle(new ByteArrayInputStream(scbStream.get()));
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
      cacheClusters.put(clusterSha1, builder.build());
      LOG.info("Connection established in {} millis.", System.currentTimeMillis() - top);
    }
    return cacheClusters.get(clusterSha1);
  }

  /**
   * Generate a fast hash to identify a cluster with a key.
   *
   * @param token
   *    current token
   * @param scbFile
   *    cloud secure file
   * @param scbStream
   *    cloud secure stream
   * @return
   *    sha1 cluster
   */
  private String computeClusterSHA1(String token, ValueProvider<File> scbFile, ValueProvider<byte[]> scbStream) {
    String result = token + "_";
    byte[] data;
    if (scbFile != null) {
      data = scbFile.get().getAbsolutePath().getBytes(StandardCharsets.UTF_8);
    } else if (scbStream != null) {
      data = scbStream.get();
    } else {
      throw new IllegalArgumentException("Cloud Secure Bundle is Required");
    }
    Formatter formatter = new Formatter();
    for (byte b : md.digest(data)) {
        formatter.format("%02x", b);
    }
    return result + formatter;
  }

}
