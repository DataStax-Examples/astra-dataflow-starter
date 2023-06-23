package com.datastax.astra.beam.fable;

import com.datastax.oss.driver.api.core.data.CqlVector;
import com.datastax.oss.driver.api.mapper.annotations.CqlName;
import com.datastax.oss.driver.api.mapper.annotations.PartitionKey;

/**
 * CassIO Expects a specific format for the Entity.
 */
public abstract class AbstractCassIOEntity {

    @PartitionKey
    @CqlName("document_id")
    protected String documentId;

    @CqlName("document")
    protected String document;

    @CqlName("embedding_vector")
    protected CqlVector<Float> vector = CqlVector.builder().build();

    @CqlName("metadata_blob")
    protected String metaData;

    public static String cqlAlterTableForVectorSearch1(String keyspace, String tableName, int dimension) {
        StringBuilder cql = new StringBuilder();
        cql.append("ALTER TABLE %s.%s ADD embedding_vector VECTOR<FLOAT, %s>");
        return String.format(cql.toString(), keyspace, tableName, String.valueOf(dimension));
    }

    public static String cqlAlterTableForVectorSearch2(String keyspace, String tableName) {
        StringBuilder cql = new StringBuilder();
        cql.append("ALTER TABLE %s.%s ADD metadata_blob TEXT");
        return String.format(cql.toString(), keyspace, tableName);
    }

    public static String cqlCreateIndexForVectorSearch(String keyspace, String tableName) {
        StringBuilder cql = new StringBuilder();
        cql.append("CREATE CUSTOM INDEX IF NOT EXISTS %s_embedding_vector_idx ON %s.%s (embedding_vector)");
        cql.append("USING 'org.apache.cassandra.index.sai.StorageAttachedIndex';");
        return String.format(cql.toString(), tableName, keyspace, tableName);
    }

    /**
     * Gets documentId
     *
     * @return value of documentId
     */
    public String getDocumentId() {
        return documentId;
    }

    /**
     * Set value for documentId
     *
     * @param documentId
     *         new value for documentId
     */
    public void setDocumentId(String documentId) {
        this.documentId = documentId;
    }

    /**
     * Gets document
     *
     * @return value of document
     */
    public String getDocument() {
        return document;
    }

    /**
     * Set value for document
     *
     * @param document
     *         new value for document
     */
    public void setDocument(String document) {
        this.document = document;
    }

    /**
     * Gets vector
     *
     * @return value of vector
     */
    public CqlVector<Float> getVector() {
        return vector;
    }

    /**
     * Set value for vector
     *
     * @param vector
     *         new value for vector
     */
    public void setVector(CqlVector<Float> vector) {
        this.vector = vector;
    }

    /**
     * Gets metaData
     *
     * @return value of metaData
     */
    public String getMetaData() {
        return metaData;
    }

    /**
     * Set value for metaData
     *
     * @param metaData
     *         new value for metaData
     */
    public void setMetaData(String metaData) {
        this.metaData = metaData;
    }
}
