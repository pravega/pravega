/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.contracts.tables;

import io.pravega.common.util.AsyncIterator;
import io.pravega.segmentstore.contracts.BadSegmentTypeException;
import io.pravega.segmentstore.contracts.StreamSegmentNotExistsException;
import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;

/**
 * Helps create Table Iterators.
 */
public interface IteratorBuilder {
    /**
     * Assigns an initial State to the iterator. If provided, the iteration will resume from where it left off, otherwise
     * it will start from the beginning.
     *
     * @param serializedState A byte array representing the serialized form of the State. This can be obtained from
     *                        {@link IteratorItem#getState()}.
     * @return This instance.
     * @throws IOException If the given serialization cannot be parsed into an Iterator State.
     */
    IteratorBuilder fromState(byte[] serializedState) throws IOException;

    /**
     * Assigns a timeout for each invocation of the Iterator's {@link AsyncIterator#getNext()} invocation.
     * @param timeout Timeout.
     * @return This instance.
     */
    IteratorBuilder fetchTimeout(Duration timeout);

    /**
     * Returns an {@link AsyncIterator} that will iterate over the {@link TableKey}s in the Table using the information
     * collected in this builder.
     *
     * @return A CompletableFuture that, when completed, will return an {@link AsyncIterator} that can be used to iterate
     * over the Keys in given Table Segment. If the operation failed, the Future will be failed with the causing
     * exception. Notable exceptions:
     * <ul>
     * <li>{@link StreamSegmentNotExistsException} If the Table Segment does not exist.
     * <li>{@link BadSegmentTypeException} If segmentName refers to a non-Table Segment.
     * </ul>
     */
    CompletableFuture<AsyncIterator<IteratorItem<TableKey>>> keyIterator();

    /**
     * Returns an {@link AsyncIterator} that will iterate over the {@link TableEntry} instances in the Table using the
     * information collected in this builder.
     *
     * @return A CompletableFuture that, when completed, will return an {@link AsyncIterator} that can be used to iterate
     * over the Entries in given Table Segment. If the operation failed, the Future will be failed with the causing
     * exception. Notable exceptions:
     * <ul>
     * <li>{@link StreamSegmentNotExistsException} If the Table Segment does not exist.
     * <li>{@link BadSegmentTypeException} If segmentName refers to a non-Table Segment.
     * </ul>
     */
    CompletableFuture<AsyncIterator<IteratorItem<TableEntry>>> entryIterator();
}
