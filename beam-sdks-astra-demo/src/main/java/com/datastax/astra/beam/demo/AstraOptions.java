package com.datastax.astra.beam.demo;

import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation;

/**
 * Input needed for a pipeine with Astra.
 */
public interface AstraOptions extends PipelineOptions {

    @Description("Secure Connect Bundle")
    @Validation.Required
    String getSecureConnectBundle();
    void setSecureConnectBundle(String path);

    @Description("Astra Token")
    @Validation.Required
    String getToken();
    void setToken(String token);

    @Description("Target Keyspace")
    @Validation.Required
    String getKeyspace();
    void setKeyspace(String keyspace);

}
