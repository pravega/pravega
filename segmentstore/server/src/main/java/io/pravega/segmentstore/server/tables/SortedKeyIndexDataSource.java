/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.server.tables;

<<<<<<< HEAD
<<<<<<< HEAD
import io.pravega.common.util.BufferView;
=======
import io.pravega.common.util.ArrayView;
>>>>>>> Issue 4656: (KeyValue Tables) Sorted Table Segments (#4763)
=======
import io.pravega.common.util.BufferView;
>>>>>>> Issue 4569: (Key-Value Tables) Merge latest master with feature-key-value-tables (#4892)
import io.pravega.segmentstore.contracts.tables.TableEntry;
import io.pravega.segmentstore.contracts.tables.TableKey;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import lombok.Data;
import lombok.NonNull;

/**
 * Data source for {@link ContainerSortedKeyIndex} and {@link SegmentSortedKeyIndexImpl}.
 */
@Data
class SortedKeyIndexDataSource {
    static final KeyTranslator EXTERNAL_TRANSLATOR = KeyTranslator.partitioned((byte) 'E');
    static final KeyTranslator INTERNAL_TRANSLATOR = KeyTranslator.partitioned((byte) 'I');

    /**
     * A Function that will be invoked when a Table Segment's Sorted Key Index nodes need to be persisted.
     */
    @NonNull
    private final Update update;
    /**
     * A Function that will be invoked when a Table Segment's Sorted Key Index nodes need to be deleted.
     */
    @NonNull
    private final Delete delete;
    /**
     * A Function that will be invoked when a Table Segment's Sorted Key Index nodes need to be retrieved.
     */
    @NonNull
    private final Read read;

    /**
     * Gets a value indicating whether the given Key should be excluded from indexing or not.
     *
     * @param keyContents The Key contents.
     * @return True if the key is an internal key (exclude), false otherwise.
     */
<<<<<<< HEAD
<<<<<<< HEAD
    boolean isKeyExcluded(BufferView keyContents) {
=======
    boolean isKeyExcluded(ArrayView keyContents) {
>>>>>>> Issue 4656: (KeyValue Tables) Sorted Table Segments (#4763)
=======
    boolean isKeyExcluded(BufferView keyContents) {
>>>>>>> Issue 4569: (Key-Value Tables) Merge latest master with feature-key-value-tables (#4892)
        return INTERNAL_TRANSLATOR.isInternal(keyContents);
    }

    @FunctionalInterface
    public interface Update {
        CompletableFuture<?> apply(String segmentName, List<TableEntry> entries, Duration timeout);
    }

    @FunctionalInterface
    public interface Delete {
        CompletableFuture<?> apply(String segmentName, Collection<TableKey> keys, Duration timeout);
    }

    @FunctionalInterface
    public interface Read {
<<<<<<< HEAD
<<<<<<< HEAD
        CompletableFuture<List<TableEntry>> apply(String segmentName, List<BufferView> keys, Duration timeout);
=======
        CompletableFuture<List<TableEntry>> apply(String segmentName, List<ArrayView> keys, Duration timeout);
>>>>>>> Issue 4656: (KeyValue Tables) Sorted Table Segments (#4763)
=======
        CompletableFuture<List<TableEntry>> apply(String segmentName, List<BufferView> keys, Duration timeout);
>>>>>>> Issue 4569: (Key-Value Tables) Merge latest master with feature-key-value-tables (#4892)
    }
}
