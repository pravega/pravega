/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.contracts.tables;

import com.google.common.annotations.Beta;
<<<<<<< HEAD
import io.pravega.common.util.AsyncIterator;
import io.pravega.common.util.BufferView;
=======
import io.pravega.common.util.ArrayView;
import io.pravega.common.util.AsyncIterator;
>>>>>>> Issue 4333: (Key-Value Tables) Table Segment Client (#4659)
import java.time.Duration;
import lombok.Builder;
import lombok.Data;
import lombok.NonNull;

/**
 * Arguments for {@link TableStore#keyIterator} and {@link TableStore#entryIterator(String, IteratorArgs)}.
 */
@Data
@Builder
public class IteratorArgs {
    /**
<<<<<<< HEAD
<<<<<<< HEAD
     * EXPERIMENTAL!
     * (Optional) A filter to apply to all returned Iterator Entries. If specified, only those entries whose keys begin
     * with this prefix will be included.
     * This option only applies to Sorted Table Segments (see {@link TableStore}. An attempt to use it on a non-Sorted
     * Table Segment will result in an {@link IllegalArgumentException}.
     */
    @Beta
    private final BufferView prefixFilter;
=======
=======
     * EXPERIMENTAL!
>>>>>>> Issue 4656: (KeyValue Tables) Sorted Table Segments (#4763)
     * (Optional) A filter to apply to all returned Iterator Entries. If specified, only those entries whose keys begin
     * with this prefix will be included.
     * This option only applies to Sorted Table Segments (see {@link TableStore}. An attempt to use it on a non-Sorted
     * Table Segment will result in an {@link IllegalArgumentException}.
     */
    @Beta
    private final ArrayView prefixFilter;
>>>>>>> Issue 4333: (Key-Value Tables) Table Segment Client (#4659)
    /**
     * (Optional) The serialized form of the State. This can be obtained from {@link IteratorItem#getState()}.
     * If provided, the iteration will resume from where it left off, otherwise it will start from the beginning.
     */
<<<<<<< HEAD
    private BufferView serializedState;
=======
    private ArrayView serializedState;
>>>>>>> Issue 4333: (Key-Value Tables) Table Segment Client (#4659)
    /**
     * Timeout for each invocation to {@link AsyncIterator#getNext()}.
     */
    @NonNull
    private Duration fetchTimeout;
}
