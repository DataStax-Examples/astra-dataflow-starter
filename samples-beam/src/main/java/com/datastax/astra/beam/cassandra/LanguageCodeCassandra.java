package com.datastax.astra.beam.cassandra;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.schemabuilder.SchemaBuilder;
import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;

import java.io.Serializable;

/**
 * Mapping as Driver 3x.
 */
@Table(name = "languages")
public class LanguageCodeCassandra implements Serializable {

    @PartitionKey
    @Column(name = "code")
    private String code;

    @Column(name = "language")
    private String language;

    public LanguageCodeCassandra() {
    }

    public LanguageCodeCassandra(String code, String language) {
        this.code = code;
        this.language = language;
    }

    /**
     * Help generating the Target Table if it does not exist.
     *
     * @return
     *      create statement
     */
    public static LanguageCodeCassandra fromCsvRow(String csvRow) {
        String[] chunks = csvRow.split(",");
        return new LanguageCodeCassandra(chunks[0], chunks[1]);
    }

    /**
     * Convert to CSV Row.
     *
     * @return
     *      csv Row.
     */
    public String toCsvRow() {
        return code + "," + language;
    }

    /**
     * Map Csv Row to LanguageCode.
     * @param csvRow
     * @return
     */
    public static LanguageCodeCassandra fromCsv(String csvRow) {
        String[] chunks = csvRow.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
        return new LanguageCodeCassandra(chunks[0], chunks[1]);
    }

    /**
     * Help generating the Target Table if it does not exist.
     *
     * @return
     *      create statement
     */
    public static String cqlCreateTable() {
        return SchemaBuilder.createTable("language")
                .addPartitionKey("code", DataType.text())
                .addColumn("language", DataType.text())
                .ifNotExists().toString();
    }

    /**
     * Gets code
     *
     * @return value of code
     */
    public String getCode() {
        return code;
    }

    /**
     * Set value for code
     *
     * @param code
     *         new value for code
     */
    public void setCode(String code) {
        this.code = code;
    }

    /**
     * Gets language
     *
     * @return value of language
     */
    public String getLanguage() {
        return language;
    }

    /**
     * Set value for language
     *
     * @param language
     *         new value for language
     */
    public void setLanguage(String language) {
        this.language = language;
    }


}
