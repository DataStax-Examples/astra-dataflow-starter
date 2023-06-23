package com.datastax.astra.beam.fable;

import com.datastax.oss.driver.api.core.data.CqlVector;
import lombok.Data;

import java.io.Serializable;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

@Data
public class FableDto implements Serializable {

    private String title;

    private String documentId;

    private String document;

    private String metadata;

    private List<Float> vector;

    public FableDto() {}

    public FableDto(Fable p) {
        this.title      = p.getTitle();
        this.documentId = p.getDocumentId();
        this.document   = p.getDocument();
        this.metadata   = p.getMetaData();
        this.vector     = StreamSupport
                .stream(p.getVector().getValues().spliterator(), false)
                .collect(Collectors.toList());
    }

    public Fable toFable() {
        Fable p = new Fable(this.title, this.documentId, this.document);
        p.setMetaData(this.metadata);
        if (this.vector != null) {
            p.setVector(CqlVector.builder().addAll(this.vector).build());
        }
        return p;
    }
}

