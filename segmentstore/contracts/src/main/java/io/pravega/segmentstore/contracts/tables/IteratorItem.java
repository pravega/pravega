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

import io.pravega.common.util.ArrayView;
import io.pravega.common.util.AsyncIterator;
import java.util.Collection;

/**
 * Defines an iteration result that is returned by the {@link AsyncIterator} when invoking {@link TableStore#keyIterator}
 * or {@link TableStore#entryIterator}.
 */
public interface IteratorItem<T> {
    /**
     * Gets an array that represents the current state of the iteration. This value can be used to to reinvoke
     * {@link TableStore#keyIterator} or {@link TableStore#entryIterator} if a previous iteration has been interrupted.
     * @return An {@link ArrayView} containing the serialized state.
     */
    ArrayView getState();

    /**
     * Gets a Collection of items that are contained in this instance. The items in this list are not necessarily related
     * to each other, nor are they guaranteed to be in any particular order.
     *
     * @return Items contained in this instance (not necessarily related to each other or in order)
     */
    Collection<T> getEntries();
}
