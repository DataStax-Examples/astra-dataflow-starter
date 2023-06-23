package com.datastax.astra.beam.lang;

import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.mapper.annotations.CqlName;
import com.datastax.oss.driver.api.mapper.annotations.Entity;
import com.datastax.oss.driver.api.mapper.annotations.PartitionKey;
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder;

import java.io.Serializable;

/**
 * DTO for Language Code.
 */
@Entity
@CqlName(LanguageCode.TABLE_NAME)
public class LanguageCode implements Serializable {

    /** Constants for mapping. */
    public static final String TABLE_NAME      = "languages";

    @PartitionKey
    @CqlName("code")
    private String code;

    @CqlName("language")
    private String language;

    /**
     * Constructor
     */
    public LanguageCode() {
    }

    /**
     * Full Fledge constructor
     */
    public LanguageCode(String code, String language) {
        this.code = code;
        this.language = language;
    }

    /**
     * Help generating the Target Table if it does not exist.
     *
     * @return
     *      create statement
     */
    public static LanguageCode fromCsvRow(String csvRow) {
        String[] chunks = csvRow.split(",");
        return new LanguageCode(chunks[0], chunks[1]);
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
    public static LanguageCode fromCsv(String csvRow) {
        String[] chunks = csvRow.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
        return new LanguageCode(chunks[0], chunks[1]);
    }

    /**
     * Help generating the Target Table if it does not exist.
     *
     * @return
     *      create statement
     */
    public static String cqlCreateTable() {
        return SchemaBuilder.createTable(TABLE_NAME)
                .ifNotExists()
                .withPartitionKey("code", DataTypes.TEXT)
                .withColumn("language", DataTypes.TEXT)
                .toString();
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