/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
<<<<<<< HEAD:shared/metrics/src/main/java/io/pravega/shared/metrics/Metric.java

package io.pravega.shared.metrics;

import io.micrometer.core.instrument.Meter.Id;

/**
 * Defines common methods for a Metric.
 */
interface Metric extends AutoCloseable {
    /**
     * Gets Id of metric.
     *
     * @return the id of metric.
     */
    Id getId();

    @Override
    void close();
=======
package io.pravega.client.tables;

import lombok.Data;
import lombok.NonNull;

/**
 * A {@link KeyValueTable} Entry.
 *
 * @param <KeyT>   Key Type.
 * @param <ValueT> Value Type
 */
@Data
public class TableEntry<KeyT, ValueT> {
    /**
     * The {@link TableKey}.
     */
    @NonNull
    private final TableKey<KeyT> key;

    /**
     * The Value.
     */
    private final ValueT value;
>>>>>>> Issue 4568: Key-Value Table Client Contracts (#4588):client/src/main/java/io/pravega/client/tables/TableEntry.java
}
