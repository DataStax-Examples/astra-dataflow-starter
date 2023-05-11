package org.apache.beam.sdk.io.astra;

import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;

import java.io.Serializable;

@Table(name = "simpledata")
public class SimpleDataEntity implements Serializable {

    @PartitionKey
    protected int id;

    @Column
    protected String data;

    @Override
    public String toString() {
        return id + ", " + data;
    }

    public SimpleDataEntity() {
    }

    public SimpleDataEntity(int id, String data) {
        this.id = id;
        this.data = data;
    }

    /**
     * Gets id
     *
     * @return value of id
     */
    public int getId() {

        return id;
    }

    /**
     * Set value for id
     *
     * @param id
     *         new value for id
     */
    public void setId(int id) {
        this.id = id;
    }

    /**
     * Gets data
     *
     * @return value of data
     */
    public String getData() {
        return data;
    }

    /**
     * Set value for data
     *
     * @param data
     *         new value for data
     */
    public void setData(String data) {
        this.data = data;
    }
}
