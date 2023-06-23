package com.datastax.astra.beam.fable;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import org.apache.beam.sdk.io.astra.db.mapping.AstraDbMapper;
import org.apache.beam.sdk.transforms.SerializableFunction;

import java.util.concurrent.CompletionStage;

public class SimpleFableDbMapper implements SerializableFunction<CqlSession, AstraDbMapper<FableDto>> {
    @Override
    public AstraDbMapper<FableDto> apply(CqlSession cqlSession) {
        return new AstraDbMapper<FableDto>() {

            @Override
            public FableDto mapRow(Row row) {
                FableDto dto = new FableDto();
                dto.setTitle(row.getString("title"));
                dto.setDocument(row.getString("document"));
                dto.setDocumentId(row.getString("document_id"));
                return dto;
            }

            @Override
            public CompletionStage<Void> deleteAsync(FableDto entity) {
                return null;
            }

            @Override
            public CompletionStage<Void> saveAsync(FableDto entity) {
                return cqlSession.executeAsync(SimpleStatement.newInstance(
                        "INSERT INTO fable (document_id, title, document) VALUES (?, ?, ?)",
                        entity.getDocumentId(), entity.getTitle(), entity.getDocument()))
                        .thenAccept(rs -> {});
            }
        };
    }

}
