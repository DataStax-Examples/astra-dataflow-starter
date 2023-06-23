package com.datastax.astra.beam.fable;

import com.datastax.oss.driver.api.mapper.annotations.CqlName;
import com.datastax.oss.driver.api.mapper.annotations.Entity;
import lombok.Data;
import lombok.NoArgsConstructor;

@Entity
@Data
@NoArgsConstructor
@CqlName("fable")
public class Fable extends AbstractCassIOEntity {

    @CqlName("title")
    private String title;

    public Fable(String title, String rowId, String row) {
        this.title = title;
        this.documentId = rowId;
        this.document = row;
    }

    public static String cqlCreateTable(String keyspace) {
        StringBuilder cql = new StringBuilder();
        cql.append("CREATE TABLE IF NOT EXISTS %s.fable (");
        cql.append("  document_id %s PRIMARY KEY,");
        cql.append("  title TEXT,");
        cql.append("  document TEXT)");
        return String.format(cql.toString(), keyspace, "TEXT");
    }

    /**
     * Help generating the Target Table if it does not exist.
     *
     * @return
     *      create statement
     */
    public static Fable fromCsvRow(String csvRow) {
        String[] chunks = csvRow.split(";");
        return new Fable(chunks[0], chunks[1], chunks[2]);
    }
}
