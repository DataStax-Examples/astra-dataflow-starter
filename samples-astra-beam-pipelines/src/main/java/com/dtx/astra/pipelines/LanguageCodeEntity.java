package com.dtx.astra.pipelines;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.schemabuilder.Create;
import com.datastax.driver.core.schemabuilder.SchemaBuilder;
import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;
import com.google.api.services.bigquery.model.TableRow;
import org.checkerframework.checker.initialization.qual.Initialized;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.UnknownKeyFor;

import java.io.Serializable;

/**
 * DTO for Language Code.
 */
@Table(name = LanguageCodeEntity.TABLE_NAME)
public class LanguageCodeEntity implements Serializable {

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
    public LanguageCodeEntity() {
    }

    /**
     * Help generating the Target Table if it does not exist.
     *
     * @return
     *      create statement
     */
    public static LanguageCodeEntity fromTableRow(@UnknownKeyFor @NonNull @Initialized TableRow tableRow) {
        LanguageCodeEntity entity = new LanguageCodeEntity();
        entity.setCode((String) tableRow.get(COLUMN_CODE));
        entity.setLanguage((String) tableRow.get(COLUMN_LANGUAGE));
        return entity;
    }

    /**
     * Help generating the Target Table if it does not exist.
     *
     * @return
     *      create statement
     */
    public static Create createTableStatement() {
        return SchemaBuilder.createTable(TABLE_NAME)
                .addPartitionKey(COLUMN_CODE, DataType.text())
                .addColumn(COLUMN_LANGUAGE, DataType.text())
                .ifNotExists();
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