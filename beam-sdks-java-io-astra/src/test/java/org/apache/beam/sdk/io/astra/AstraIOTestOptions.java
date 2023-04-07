package org.apache.beam.sdk.io.astra;

import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.testing.TestPipelineOptions;

/**
 * Working Against Astra
 */
public interface AstraIOTestOptions extends TestPipelineOptions {

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