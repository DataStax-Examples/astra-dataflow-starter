package org.apache.beam.sdk.io.astra.cql;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Reshuffle;
import org.apache.beam.sdk.values.PCollection;

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;

/**
 * Read All Operation.
 *
 * @param <T>
 */
public class AstraCqlReadAll<T> extends PTransform<PCollection<AstraCqlRead<T>>, PCollection<T>> {

    Coder<T> coder;

    public AstraCqlReadAll() {}

    public AstraCqlReadAll(Coder<T> coder) {
        this.coder = coder;
    }

    /**
     * Set value for coder
     *
     * @param coder
     *         new value for coder
     */
    public void setCoder(Coder<T> coder) {
        this.coder = coder;
    }

    /**
     * Gets coder
     *
     * @return value of coder
     */
    public Coder<T> getCoder() {
        return coder;
    }

    @Override
    public PCollection<T> expand(PCollection<AstraCqlRead<T>> input) {
        checkArgument(coder != null, "withCoder() is required");
        return input
                .apply("Reshuffle", Reshuffle.viaRandomKey())
                .apply("Read", ParDo.of(new CqlReadFn<T>()))
                .setCoder(this.coder);
    }

}

