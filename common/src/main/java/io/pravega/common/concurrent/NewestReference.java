/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.common.concurrent;

import lombok.RequiredArgsConstructor;
import lombok.Synchronized;

/**
 * Keeps the largest value in a thread safe way. Analogous to AtomicRefrence except that is utilizes
 * the fact that its values are comparable to ensure that the value held never decreases.
 */
@RequiredArgsConstructor
public final class NewestReference<T extends Comparable<T>> {
    private T value;

    @Synchronized
    public T get() {
        return value;
    }

    @Synchronized
    public void update(T newValue) {
        if (newValue != null && (value == null || value.compareTo(newValue) < 0)) {
            value = newValue;
        }
    }
}
