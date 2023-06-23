package com.datastax.astra.dataflow.transforms;


import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.ListType;
import com.datastax.oss.driver.api.core.type.SetType;
import com.datastax.oss.driver.api.core.type.TupleType;
import com.datastax.oss.protocol.internal.ProtocolConstants;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import org.apache.beam.sdk.io.astra.db.AstraDbIO;
import org.apache.beam.sdk.io.astra.db.CqlSessionHolder;
import org.apache.beam.sdk.transforms.SerializableFunction;

import java.util.ArrayList;
import java.util.List;
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
     *         current keyspace
     * @param table
     *         current table
     */
    public CassandraToBigQuerySchemaMapperFn(String keyspace, String table) {
        this.keyspace = keyspace;
        this.table = table;
    }

    @Override
    public TableSchema apply(AstraDbIO.Read<?> astraSource) {
        return readTableSchemaFromCassandraTable(
                CqlSessionHolder.getCqlSession(astraSource), keyspace, table);
    }

    /**
     * This function is meant to build a schema for destination table based on cassandra table schema.
     *
     * @param session
     *         current session
     * @param keyspace
     *         cassandra keyspace
     * @param table
     *         cassandra table
     * @return
     */
    public static TableSchema readTableSchemaFromCassandraTable(CqlSession session, String keyspace, String table) {
        Metadata clusterMetadata = session.getMetadata();
        KeyspaceMetadata keyspaceMetadata = clusterMetadata.getKeyspace(keyspace).get();
        TableMetadata tableMetadata = keyspaceMetadata.getTable(table).get();
        TableSchema tableSchema = new TableSchema();
        List<TableFieldSchema > fieldList = new ArrayList<>();
        for(ColumnMetadata columnMetadata : tableMetadata.getColumns().values()) {
            TableFieldSchema fieldSchema = new TableFieldSchema();
            fieldSchema.setName(columnMetadata.getName().toString());
            fieldSchema.setType(mapCassandraToBigQueryType(columnMetadata.getType()).name());
            fieldSchema.setMode("NULLABLE");
            if (tableMetadata.getPrimaryKey().contains(columnMetadata)) {
                fieldSchema.setMode("REQUIRED");
            }
            int protocolCode = columnMetadata.getType().getProtocolCode();
            if (protocolCode == ProtocolConstants.DataType.LIST ||
                protocolCode == ProtocolConstants.DataType.SET ||
                protocolCode == ProtocolConstants.DataType.TUPLE ||
                protocolCode == ProtocolConstants.DataType.MAP) {
                fieldSchema.setMode("REPEATED");
            }
            fieldList.add(fieldSchema);
        }
        tableSchema.setFields(fieldList);
        return tableSchema;
    }

    /**
     * Map DataType to BigQuery StandardSQLTypeName.
     *
     * @param dataType
     *         cassandra type
     * @return SQL Type.
     */
    private static StandardSQLTypeName mapCassandraToBigQueryType(DataType dataType) {
        switch (dataType.getProtocolCode()) {
            case ProtocolConstants.DataType.BOOLEAN:
                return StandardSQLTypeName.BOOL;
            case ProtocolConstants.DataType.INT:
            case ProtocolConstants.DataType.BIGINT:
                return StandardSQLTypeName.INT64;
            case ProtocolConstants.DataType.FLOAT:
            case ProtocolConstants.DataType.DOUBLE:
                return StandardSQLTypeName.FLOAT64;
            case ProtocolConstants.DataType.TIMESTAMP:
                return StandardSQLTypeName.TIMESTAMP;
            case ProtocolConstants.DataType.LIST:
                ListType listType = (ListType) dataType;
                return mapCassandraToBigQueryType(listType.getElementType());
            case ProtocolConstants.DataType.SET:
                SetType setType = (SetType) dataType;
                return mapCassandraToBigQueryType(setType.getElementType());
            case ProtocolConstants.DataType.TUPLE:
            case ProtocolConstants.DataType.MAP:
                return StandardSQLTypeName.STRUCT;
            case ProtocolConstants.DataType.ASCII:
            case ProtocolConstants.DataType.VARINT:
                // Add more cases for other Cassandra types as needed
            default:
                // Default to STRING if no mapping found
                return StandardSQLTypeName.STRING;
        }
    }
}