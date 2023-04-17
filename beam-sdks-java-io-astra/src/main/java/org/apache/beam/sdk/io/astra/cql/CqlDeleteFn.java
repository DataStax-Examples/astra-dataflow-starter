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
public class CqlDeleteFn<T> extends DoFn<T, Void> {

    /** Reference to the writer. */
    private final AstraCqlWrite<T> spec;

    /** Reference to the Mutator. */
    private transient CqlMutator<T> deleter;

    /**
     * Default constructor.
     *
     * @param spec
     *      specs
     */
    public CqlDeleteFn(AstraCqlWrite<T> spec) {
        this.spec = spec;
    }

    /** {@inheritDoc}. */
    @Setup
    public void setup() {
        deleter = new CqlMutator<>(spec, Mapper::deleteAsync, "deletes");
    }

    /** {@inheritDoc}. */
    @ProcessElement
    public void processElement(ProcessContext c) throws ExecutionException, InterruptedException {
        deleter.mutate(c.element());
    }

    /** {@inheritDoc}. */
    @FinishBundle
    public void finishBundle() throws Exception {
        deleter.flush();
    }

    /** {@inheritDoc}. */
    @Teardown
    public void teardown() throws Exception {
        deleter.close();
        deleter = null;
    }
}
