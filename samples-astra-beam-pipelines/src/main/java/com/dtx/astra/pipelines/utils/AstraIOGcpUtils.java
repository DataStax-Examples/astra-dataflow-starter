package com.dtx.astra.pipelines.utils;

import com.google.cloud.secretmanager.v1.SecretManagerServiceClient;

import java.io.IOException;

/**
 * Utilities to work with GCP and Astra
 */
public class AstraIOGcpUtils {

    public static SecretManagerServiceClient client;

    /**
     * Access the Token.
     *
     * @param secretResourceId
     *      token resource Id
     * @return
     *      token Value
     */
    public static final String readTokenSecret(String secretResourceId) {
        try {
            if (client == null) client = SecretManagerServiceClient.create();
            return client
                .accessSecretVersion(secretResourceId)
                .getPayload().getData()
                .toStringUtf8();
        } catch (IOException e) {
            throw new IllegalStateException("Cannot read google secrets", e);
        }
    }

    /**
     * Access Secure Bundle.
     *
     * @param secretResourceId
     *    Secure Bundle resource Id
     * @return
     *    Secure Bundle Value
     */
    public static final byte[] readSecureBundleSecret(String secretResourceId) {
        try {
            if (client == null) client = SecretManagerServiceClient.create();
            return client
                .accessSecretVersion(secretResourceId)
                .getPayload().getData()
                .toByteArray();
        } catch (IOException e) {
            throw new IllegalStateException("Cannot read google secrets", e);
        }
    }

}
