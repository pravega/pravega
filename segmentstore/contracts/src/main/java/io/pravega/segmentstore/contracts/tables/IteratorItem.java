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
import java.util.List;
import lombok.Data;

/**
 * Defines an iteration result that is returned by the {@link AsyncIterator} when invoking {@link TableStore#iterator}.
 */
@Data
public class IteratorItem<T> {
    /**
     * Gets a byte array that represents the current state of the iteration. This value can be used to to reinvoke
     * {@link TableStore#iterator} if a previous iteration has been interrupted.
     */
    private final byte[] state;

    /**
     * Gets a List of items that are contained in this instance. The items in this list are not necessarily related
     * to each other, nor are they guaranteed to be in any particular order.
     */
    private final List<T> entries;
}
