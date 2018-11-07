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
import java.io.IOException;

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
     * Returns an {@link AsyncIterator} that will iterate over the {@link TableKey}s in the Table using the information
     * collected in this builder.
     *
     * @return A new {@link AsyncIterator}.
     */
    AsyncIterator<IteratorItem<TableKey>> keyIterator();

    /**
     * Returns an {@link AsyncIterator} that will iterate over the {@link TableEntry} instances in the Table using the
     * information collected in this builder.
     *
     * @return A new {@link AsyncIterator}.
     */
    AsyncIterator<IteratorItem<TableEntry>> entryIterator();
}
