/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.emc.pravega.common.concurrent;

import lombok.RequiredArgsConstructor;
import lombok.Synchronized;

/**
 * Keeps the largest value in a thread safe way. Analogous to AtomicReference except that is utilizes
 * the fact that its values are comparable to ensure that the value held never decreases.
 */
@RequiredArgsConstructor
public final class NewestReference<T extends Comparable<T>> {
    private T value;

    @Synchronized
    public T get() {
        return value;
    }

    /**
     * Updates the value if the given value is larger than the current value.
     * @param newValue The latest value for the reference
     */
    @Synchronized
    public void update(T newValue) {
        if (newValue != null && (value == null || value.compareTo(newValue) < 0)) {
            value = newValue;
        }
    }
}
