package com.dtx.astra.dataflow;

import com.datastax.driver.mapping.annotations.Table;

import java.io.Serializable;

/**
 * Sample Bean.
 */
@Table(name = "language")
public class LanguageCode implements Serializable {

    /** Country Code. */
    private String code;

    /** Country Name. */
    private String country;

    /**
     * Default constructor
     */
    public LanguageCode() {}

    /**
     * Build a constructor.
     *
     * @param code
     *      country code
     * @param country
     *      country name
     */
    public LanguageCode(String code, String country) {
        this.code = code;
        this.country = country;
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
     * Gets country
     *
     * @return value of country
     */
    public String getCountry() {
        return country;
    }

    /**
     * Set value for country
     *
     * @param country
     *         new value for country
     */
    public void setCountry(String country) {
        this.country = country;
    }
}
