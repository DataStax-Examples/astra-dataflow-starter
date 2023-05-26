package com.datastax.astra.dataflow.transforms;

import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.TableMetadata;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import org.apache.beam.sdk.io.astra.db.AstraDbConnectionManager;
import org.apache.beam.sdk.io.astra.db.AstraDbIO;
import org.apache.beam.sdk.transforms.SerializableFunction;

import java.util.stream.Collectors;

/**
 * Will Convert a Cassandra Row into a Beam Row.
 */
public class CassandraToBigQuerySchemaMapperFn implements SerializableFunction<AstraDbIO.Read<?>, TableSchema > {

    /**
     * Current table.
     */
    private final String table;

    /**
     * Current keyspace.
     */
    private final String keyspace;

    /**
     * Access Table Schema.
     *
     * @param keyspace
     *      current keyspace
     * @param table
     *      current table
     */
    public CassandraToBigQuerySchemaMapperFn(String keyspace, String table) {
        this.keyspace = keyspace;
        this.table = table;
    }

    @Override
    public TableSchema apply(AstraDbIO.Read<?> astraSource) {
        return readTableSchemaFromCassandraTable(AstraDbConnectionManager
                .getInstance().getSession(astraSource), keyspace, table);
    }

    /**
     * This function is meant to build a schema for destination table based on cassandra table schema.
     * @param session
     *      current session
     * @param keyspace
     *      cassandra keyspace
     * @param table
     *      cassandra table
     * @return
     */
    public static TableSchema readTableSchemaFromCassandraTable(Session session, String keyspace, String table) {
        Metadata clusterMetadata = session.getCluster().getMetadata();
        KeyspaceMetadata keyspaceMetadata = clusterMetadata.getKeyspace(keyspace);
        TableMetadata tableMetadata = keyspaceMetadata .getTable(table);
        return new TableSchema().setFields(tableMetadata
                .getColumns().stream()
                .map(CassandraToBigQuerySchemaMapperFn::mapColumnDefinition)
                .collect(Collectors.toList()));
    }


    /**
     * Mapping for a column.
     *
     * @param cd
     *      current column
     * @return
     *      big query column
     */
    private static TableFieldSchema mapColumnDefinition(ColumnMetadata cd) {
        TableFieldSchema tfs = new TableFieldSchema();
        tfs.setName(cd.getName());
        tfs.setType(mapCassandraToBigQueryType(cd.getType()).name());
        if (cd.getParent().getPrimaryKey().contains(cd)) {
            tfs.setMode("REQUIRED");
        }

        if (cd.getType().isCollection()) {
            tfs.setMode("REPEATED");
        }
        return tfs;
    }

    /**
     * Map DataType to BigQuery StandardSQLTypeName.
     *
     * @param dataType
     *       cassandra type
     * @return
     *      SQL Type.
     */
    private static StandardSQLTypeName mapCassandraToBigQueryType(DataType dataType) {
        switch (dataType.getName()) {
            case BOOLEAN:
                return StandardSQLTypeName.BOOL;
            case INT:
            case BIGINT:
                return StandardSQLTypeName.INT64;
            case FLOAT:
            case DOUBLE:
                return StandardSQLTypeName.FLOAT64;
            case TIMESTAMP:
                return StandardSQLTypeName.TIMESTAMP;
            case LIST:
            case SET:
                return mapCassandraToBigQueryType(dataType.getTypeArguments().get(0));
            case MAP:
            case TUPLE:
                return StandardSQLTypeName.STRUCT;
            case ASCII:
            case TEXT:
            case VARCHAR:
                // Add more cases for other Cassandra types as needed
            default:
                // Default to STRING if no mapping found
                return StandardSQLTypeName.STRING;
        }
    }

}
