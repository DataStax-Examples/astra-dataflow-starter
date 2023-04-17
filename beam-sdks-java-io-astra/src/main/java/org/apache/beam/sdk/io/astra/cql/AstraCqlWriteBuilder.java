package org.apache.beam.sdk.io.astra.cql;

import org.apache.beam.sdk.io.astra.DefaultObjectMapperFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.net.URL;

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;

/**
 * Builder for {@link AstraCqlWrite}
 */
public class AstraCqlWriteBuilder<T> {

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

    /** Write or DELETE. */
    AstraCqlWrite.MutationType mutationType = AstraCqlWrite.MutationType.WRITE;

    /**
     * Hide Constructor
     */
    AstraCqlWriteBuilder() {}

    /**
     * Build the Reader.
     * @return
     *      current Reader.
     */
    public AstraCqlWrite<T> build() {
        AstraCqlWrite<T> write = new AstraCqlWrite<>();
        write.connectTimeout = ValueProvider.StaticValueProvider.of(connectTimeout);
        write.readTimeout    = ValueProvider.StaticValueProvider.of(readTimeout);

        checkArgument(!StringUtils.isEmpty(token), "Token should not be null nor empty");
        write.token          = ValueProvider.StaticValueProvider.of(token);

        if (secureConnectBundleUrl != null) {
            write.secureConnectBundleUrl = ValueProvider.StaticValueProvider.of(secureConnectBundleUrl);
        }
        if (secureConnectBundleFile != null) {
            write.secureConnectBundleFile = ValueProvider.StaticValueProvider.of(secureConnectBundleFile);
        }

        checkArgument(!StringUtils.isEmpty(keyspace), "Keyspace should not be null nor empty");
        write.keyspace = ValueProvider.StaticValueProvider.of(keyspace);

        write.entity = (Class<T>) getClass().getTypeParameters()[0].getClass();
        write.mapperFactoryFn = new DefaultObjectMapperFactory(write.entity);
        write.mutationType = mutationType;
        return write;
    }

    /**
     * Specify the Cassandra keyspace where to read data.
     *
     * @param keyspace
     *      keyspace name
     * @return
     *      reference to builder
     */
    public AstraCqlWriteBuilder<T> withKeyspace(String keyspace) {
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
    public AstraCqlWriteBuilder<T> withToken(String token) {
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
    public AstraCqlWriteBuilder<T> withConnectTimeout(int connectTimeout) {
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
    public AstraCqlWriteBuilder<T> withReadTimeout(int readTimeout) {
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
    public AstraCqlWriteBuilder<T> withSecureConnectBundle(URL scb) {
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
    public AstraCqlWriteBuilder<T> withSecureConnectBundle(File scb) {
        this.secureConnectBundleFile = scb;
        return this;
    }
}
