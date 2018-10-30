/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.common.util;

/**
 * Defines a Map that contains Key-Value pairs accessible by an index.
 *
 * @param <KeyType>   Type of the Keys in the Map.
 * @param <ValueType> Type of the Values in the Map.
 */
public interface IndexedMap<KeyType, ValueType> {
    /**
     * Gets a value representing the number of Entries in this Map.
     *
     * @return The count.
     */
    int getCount();

    /**
     * Gets the Key located at the given position.
     *
     * @param position The position for which to get the Key.
     * @return The Key
     */
    KeyType getKey(int position);

    /**
     * Gets the Value located at the given position.
     *
     * @param position The position for which to get the Value.
     * @return The Value.
     */
    ValueType getValue(int position);
}
