package com.datastax.astra.beam;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.schemabuilder.SchemaBuilder;
import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;

import java.io.Serializable;

/**
 * DTO for Language Code.
 */
@Table(name = LanguageCode.TABLE_NAME)
public class LanguageCode implements Serializable {

    /** Constants for mapping. */
    public static final String TABLE_NAME      = "languages";
    public static final String COLUMN_CODE     = "code";
    public static final String COLUMN_LANGUAGE = "language";

    @PartitionKey
    @Column(name = COLUMN_CODE)
    private String code;

    @Column(name = COLUMN_LANGUAGE)
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
     * Help generating the Target Table if it does not exist.
     *
     * @return
     *      create statement
     */
    public static String cqlCreateTable() {
        return SchemaBuilder.createTable(TABLE_NAME)
                .addPartitionKey(COLUMN_CODE, DataType.text())
                .addColumn(COLUMN_LANGUAGE, DataType.text())
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