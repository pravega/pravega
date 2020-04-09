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

import io.pravega.common.util.ArrayView;
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
    /**
     * A {@link KeyTranslator} for internal keys.
     */
    @NonNull
    private final KeyTranslator keyTranslator;
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
    private final SortedKeyIndexDataSource.Read read;

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
        CompletableFuture<List<TableEntry>> apply(String segmentName, List<ArrayView> keys, Duration timeout);
    }
}
