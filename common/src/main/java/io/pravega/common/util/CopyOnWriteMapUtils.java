/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.common.util;

import javax.annotation.concurrent.NotThreadSafe;
import java.util.HashMap;
import java.util.Map;

/**
 * Utils class that contains copy-on-write methods for maps.
 */
@NotThreadSafe
public class CopyOnWriteMapUtils {

    /**
     * Generates a new map object consisting of applying put(key, value) on the oldMap object.
     *
     * @param oldMap Base map to copy.
     * @param key    Key to insert.
     * @param value  Value to insert associated with key.
     * @param <T>    Type of keys in the map.
     * @param <R>    Type of values in the map.
     * @return       A new map object consisting of applying put(key, value) on the oldMap object.
     */
    public static <T, R> Map<T, R> put(Map<T, R> oldMap, T key, R value) {
        Map<T, R> newMap = new HashMap<>(oldMap);
        newMap.put(key, value);
        return newMap;
    }

    /**
     * Generates a new map object consisting of applying remove(key) on the oldMap object.
     *
     * @param oldMap Base map to copy.
     * @param key    Key to insert.
     * @param <T>    Type of keys in the map.
     * @param <R>    Type of values in the map.
     * @return       A new map object consisting of applying remove(key) on the oldMap object.
     */
    public static <T, R> Map<T, R> remove(Map<T, R> oldMap, Object key) {
        Map<T, R> newMap = new HashMap<>(oldMap);
        newMap.remove(key);
        return newMap;
    }

    // End region
}