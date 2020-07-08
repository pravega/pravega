/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.tables;

import io.pravega.common.util.AsyncIterator;
import java.util.List;
import lombok.Data;

/**
 * An iteration result item returned by {@link AsyncIterator} when invoking {@link KeyValueTable#entryIterator}
 * or {@link KeyValueTable#keyIterator}.
 *
 * @param <T> Iterator Item type.
 */
@Data
public class IteratorItem<T> {
    /**
     * Gets an {@link IteratorState} that can be used to reinvoke {@link KeyValueTable#entryIterator} or
     * {@link KeyValueTable#keyIterator} if a previous iteration has been interrupted (by losing the pointer to the
     * {@link AsyncIterator}), system restart, etc.
     */
    private final IteratorState state;
    /**
     * A List of items that are contained in this instance.
     */
    private final List<T> items;
}
