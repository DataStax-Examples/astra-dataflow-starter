package org.apache.beam.sdk.io.astra.cql;

import org.apache.beam.sdk.io.astra.Mapper;
import org.apache.beam.sdk.transforms.DoFn;

import java.util.concurrent.ExecutionException;

/**
 * Delete an entity.
 *
 * @param <T>
 *          current delete
 */
public class CqlWriteFn<T> extends DoFn<T, Void> {

    /** Reference to the writer. */
    private final AstraCqlWrite<T> spec;

    /** Reference to the Mutator. */
    private transient CqlMutator<T> writer;

    /**
     * Default constructor.
     *
     * @param spec
     *      specs
     */
    public CqlWriteFn(AstraCqlWrite<T> spec) {
        this.spec = spec;
    }

    /** {@inheritDoc}. */
    @Setup
    public void setup() {
        writer = new CqlMutator<>(spec, Mapper::saveAsync, "writes");
    }

    /** {@inheritDoc}. */
    @ProcessElement
    public void processElement(ProcessContext c) throws ExecutionException, InterruptedException {
        writer.mutate(c.element());
    }

    /** {@inheritDoc}. */
    @FinishBundle
    public void finishBundle() throws Exception {
        writer.flush();
    }

    /** {@inheritDoc}. */
    @Teardown
    public void teardown() throws Exception {
        writer.close();
        writer = null;
    }
}
