package com.datastax.astra.dataflow.utils;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.gson.Gson;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.lang.reflect.Type;
import com.google.gson.reflect.TypeToken;

/**
 * Utility class to work with Google BigQuery.
 */
public class GoogleBigQueryUtils {

    /**
     * Utilities for BigQuery
     */
    private GoogleBigQueryUtils() {}

    /**
     * Get Table Schema from JSON.
     *
     * @param jsonFileName
     *      json file
     * @return
     *      table Schema
     */
    public static TableSchema getTableSchema(String jsonFileName) {
        Type listType = new TypeToken<ArrayList<TableFieldSchema>>(){}.getType();
        List<TableFieldSchema> yourClassList = new Gson().fromJson(readJsonFile(jsonFileName), listType);
        TableSchema tableSchema = new TableSchema();
        tableSchema.setFields(new Gson().fromJson(readJsonFile(jsonFileName), listType));
        return tableSchema;
    }

    /**
     * Read a JSON FILE.
     * @param filePath
     *      current json
     * @return
     *      content of the JSON
     */
    private static String readJsonFile(String filePath) {
        // Get the InputStream for the file from the classpath
        InputStream inputStream = GoogleBigQueryUtils.class.getClassLoader().getResourceAsStream(filePath);
        if (inputStream != null) {
            try (Scanner scanner = new Scanner(inputStream, StandardCharsets.UTF_8.name())) {
                return scanner.useDelimiter("\\A").next();
            }
        } else {
            throw new IllegalArgumentException("Cannot read Json Schema File");
        }
    }

    public static void main(String[] args) {
        System.out.println(getTableSchema("schema_language_codes.json"));
    }
}
