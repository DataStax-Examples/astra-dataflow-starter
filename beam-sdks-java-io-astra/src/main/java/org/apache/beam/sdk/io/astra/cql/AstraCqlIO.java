/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.sdk.io.astra.cql;

import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.transforms.PTransform;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An IO to read and write from/to Astra.
 */
@Experimental(Kind.SOURCE_SINK)
public class AstraCqlIO {

  /**
   * Work with CQL and Astra.
   */
  private static final Logger LOG = LoggerFactory.getLogger(AstraCqlIO.class);

  /**
   * Hiding default constructor.
   */
  private AstraCqlIO() {}

  /**
   * Provide a Read {@link PTransform} to read data from a Cassandra database.
   */
  public static <T> AstraCqlReadBuilder<T> read() {
    return AstraCqlRead.<T>builder();
  }

  /** Provide a Write {@link PTransform} to write data to a Cassandra database. */
  public static <T> AstraCqlWriteBuilder<T> write() {
    AstraCqlWriteBuilder<T> builder = AstraCqlWrite.<T>builder();
    builder.mutationType = AstraCqlWrite.MutationType.WRITE;
    return builder;
  }

  /** Provide a Write {@link PTransform} to delete data to a Cassandra database. */
  public static <T> AstraCqlWriteBuilder<T> delete() {
      AstraCqlWriteBuilder<T> builder = AstraCqlWrite.<T>builder();
      builder.mutationType = AstraCqlWrite.MutationType.DELETE;
      return builder;
  }
}
