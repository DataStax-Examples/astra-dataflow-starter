package org.apache.beam.sdk.io.astra.cql;

import com.datastax.driver.core.Session;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.astra.DefaultObjectMapperFactory;
import org.apache.beam.sdk.io.astra.Mapper;
import org.apache.beam.sdk.io.astra.RingRange;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.io.InputStream;
import java.net.URL;
import java.util.Set;

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;

/**
 * Build for READ.
 *
 * @param <T>
 *     type of entity used
 */
public class AstraCqlReadBuilder<T> {

    /** Token. */
    protected String token;

    /** Secure connect bundle file. */
    private File secureConnectBundleFile;
    private URL secureConnectBundleUrl;

    /** Connect Timeout. */
    private int connectTimeout = 20;

    /** Read Timeout. */
    private int readTimeout = 20;

    // ----- Operations ------

    /** Keyspace. */
    private String keyspace;

    private String table;

    private String query;

    private String consistencyLevel;



    /**
     * Hide Constructor
     */
    AstraCqlReadBuilder() {}

    /**
     * Build the Reader.
     * @return
     *      current Reader.
     */
    public AstraCqlRead<T> build() {
        AstraCqlRead<T> read = new AstraCqlRead<>();
        read.connectTimeout = ValueProvider.StaticValueProvider.of(connectTimeout);
        read.readTimeout    = ValueProvider.StaticValueProvider.of(readTimeout);

        checkArgument(!StringUtils.isEmpty(token), "Token should not be null nor empty");
        read.token          = ValueProvider.StaticValueProvider.of(token);

        if (secureConnectBundleUrl != null) {
            read.secureConnectBundleUrl = ValueProvider.StaticValueProvider.of(secureConnectBundleUrl);
        }
        if (secureConnectBundleFile != null) {
            read.secureConnectBundleFile = ValueProvider.StaticValueProvider.of(secureConnectBundleFile);
        }

        checkArgument(!StringUtils.isEmpty(keyspace), "Keyspace should not be null nor empty");
        read.keyspace = ValueProvider.StaticValueProvider.of(keyspace);

        read.entity = (Class<T>) getClass().getTypeParameters()[0].getClass();
        read.mapperFactoryFn = new DefaultObjectMapperFactory(read.entity);
        return read;
    }

    /**
     * Specify the Cassandra keyspace where to read data.
     *
     * @param keyspace
     *      keyspace name
     * @return
     *      reference to builder
     */
    public AstraCqlReadBuilder<T> withKeyspace(String keyspace) {
        this.keyspace = keyspace;
        return this;
    }

    /**
     * Specify the Astra token (credentials)
     *
     * @param token
     *      credential token
     * @return
     *      reference to builder
     */
    public AstraCqlReadBuilder<T> withToken(String token) {
        this.token = token;
        return this;
    }

    /**
     * Specify the connection timeout in seconds
     *
     * @param connectTimeout
     *      connection timeout
     * @return
     *      reference to builder
     */
    public AstraCqlReadBuilder<T> withConnectTimeout(int connectTimeout) {
        this.connectTimeout = connectTimeout;
        return this;
    }

    /**
     * Specify the read timeout in seconds
     *
     * @param readTimeout
     *      readTimeout timeout
     * @return
     *      reference to builder
     */
    public AstraCqlReadBuilder<T> withReadTimeout(int readTimeout) {
        this.readTimeout = readTimeout;
        return this;
    }

    /**
     * Specify the secure connect bundle
     *
     * @param scb
     *      secure connect bundle url
     * @return
     *      reference to builder
     */
    public AstraCqlReadBuilder<T> withSecureConnectBundle(URL scb) {
        this.secureConnectBundleUrl = scb;
        return this;
    }

    /**
     * Specify the secure connect bundle
     *
     * @param scb
     *      secure connect bundle url
     * @return
     *      reference to builder
     */
    public AstraCqlReadBuilder<T> withSecureConnectBundle(File scb) {
        this.secureConnectBundleFile = scb;
        return this;
    }

}
