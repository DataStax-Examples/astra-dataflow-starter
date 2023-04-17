package com.dtx.astra.pipelines.utils;

import com.dtx.astra.pipelines.domain.SimpleDataEntity;
import org.apache.beam.sdk.io.astra.AstraIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation;

/**
 * Simple Pipeline Options for Bulk Data Processing (CQL).
 */
public interface AstraPipelineOptions extends PipelineOptions {

    @Description("SecureConnectBundle")
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

    @Description("Folder to look for inputs")
    String getInputFolder();
    void setInputFolder(String folder);

    @Description("Folder to look for inputs")
    String getOutputFolder();
    void setOutputFolder(String folder);

}
