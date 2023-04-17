package org.apache.beam.sdk.io.astra.cql;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PlainTextAuthProvider;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SocketOptions;

import java.util.concurrent.ConcurrentHashMap;

/**
 * Manage Open Connections to Astra
 */
public class CqlConnectionManager {

    /** Reference to Clusters. */
    private static final ConcurrentHashMap<String, Cluster> clusterMap = new ConcurrentHashMap<>();

    /** Reference to Session. */
    private static final ConcurrentHashMap<String, Session> sessionMap = new ConcurrentHashMap<>();

    /**
     * Close Session and Cluster at shutdown
     */
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

    /**
     * Get current Cluster.
     *
     * @param read
     *      current writer
     * @return
     *      hash
     */
    public static Cluster getCluster(AstraCqlRead<?> read) {
        String clusterHash = computeClusterHash(read);
        // This cluster has not been created before
        if (clusterMap.containsKey(clusterHash)) {
            Cluster.Builder builder = Cluster.builder();
            builder.withAuthProvider(new PlainTextAuthProvider("token", read.getToken().get()));
            if (read.getSecureConnectBundleFile() != null) {
                builder.withCloudSecureConnectBundle(read.getSecureConnectBundleFile().get());
            } else if (read.getSecureConnectBundleUrl() != null) {
                builder.withCloudSecureConnectBundle(read.getSecureConnectBundleUrl().get());
            } else {
                throw new IllegalArgumentException("Cloud Secure Bundle is Required");
            }
            // Connectivity
            builder.withSocketOptions( new SocketOptions()
                    .setConnectTimeoutMillis(read.getConnectTimeout().get())
                    .setReadTimeoutMillis(read.getReadTimeout().get()));
            clusterMap.put(clusterHash, builder.build());
        }
        return clusterMap.get(clusterHash);
    }

    /**
     * Get current Cluster.
     *
     * @param write
     *      current writer
     * @return
     *      hash
     */
    public static Cluster getCluster(AstraCqlWrite<?> write) {
        String clusterHash = computeClusterHash(write);
        // This cluster has not been created before
        if (clusterMap.containsKey(clusterHash)) {
            Cluster.Builder builder = Cluster.builder();
            builder.withAuthProvider(new PlainTextAuthProvider("token", write.getToken().get()));
            if (write.getSecureConnectBundleFile() != null) {
                builder.withCloudSecureConnectBundle(write.getSecureConnectBundleFile().get());
            } else if (write.getSecureConnectBundleUrl() != null) {
                builder.withCloudSecureConnectBundle(write.getSecureConnectBundleUrl().get());
            } else {
                throw new IllegalArgumentException("Cloud Secure Bundle is Required");
            }
            // Connectivity
            builder.withSocketOptions( new SocketOptions()
                    .setConnectTimeoutMillis(write.getConnectTimeout().get())
                    .setReadTimeoutMillis(write.getReadTimeout().get()));
            clusterMap.put(clusterHash, builder.build());
        }
        return clusterMap.get(clusterHash);
    }

    /**
     * Get current Session.
     *
     * @param write
     *      current writer
     * @return
     *      hash
     */
    public static Session getSession(AstraCqlWrite<?> write) {
        String sessionHash = computeSessionHash(write);
        if (sessionMap.containsKey(sessionHash)) {
            sessionMap.put(sessionHash,
                    getCluster(write).connect(write.getKeyspace().get()));
        }
        return sessionMap.get(sessionHash);
    }

    /**
     * Get current Session.
     *
     * @param read
     *      current reader
     * @return
     *      hash
     */
    public static Session getSession(AstraCqlRead<?> read) {
        String sessionHash = computeSessionHash(read);
        if (sessionMap.containsKey(sessionHash)) {
            sessionMap.put(sessionHash,
                    getCluster(read).connect(read.getKeyspace().get()));
        }
        return sessionMap.get(sessionHash);
    }

    /**
     * To identify the proper cluster we need token and SCB.
     *
     * @param write
     *      current writer
     * @return
     *      hash for a cluster
     */
    private static String computeClusterHash(AstraCqlWrite<?> write) {
        String writeHash = write.getToken().get();
        if (null != write.getSecureConnectBundleFile()) {
            writeHash += write.getSecureConnectBundleFile().get().getAbsolutePath();
        }
        if (null != write.getSecureConnectBundleUrl()) {
            writeHash += write.getSecureConnectBundleFile().get().getAbsolutePath();
        }
        return writeHash;
    }

    /**
     * To identify the proper cluster we need token and SCB.
     *
     * @param read
     *      current reader
     * @return
     *      hash for a cluster
     */
    private static String computeClusterHash(AstraCqlRead<?> read) {
        String readHash = read.getToken().get();
        if (null != read.getSecureConnectBundleFile()) {
            readHash += read.getSecureConnectBundleFile().get().getAbsolutePath();
        }
        if (null != read.getSecureConnectBundleUrl()) {
            readHash += read.getSecureConnectBundleFile().get().getAbsolutePath();
        }
        return readHash;
    }

    /**
     * To identify the proper cluster we need token and SCB.
     *
     * @param write
     *      current writer
     * @return
     *      hash for a cluster
     */
    private static String computeSessionHash(AstraCqlWrite<?> write) {
        return computeClusterHash(write) + write.getKeyspace().get();
    }

    /**
     * To identify the proper cluster we need token and SCB.
     *
     * @param read
     *      current reader
     * @return
     *      hash for a cluster
     */
    private static String computeSessionHash(AstraCqlRead<?> read) {
        return computeClusterHash(read) + read.getKeyspace().get();
    }

}
