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

import net.jcip.annotations.NotThreadSafe;

import java.util.HashMap;
import java.util.Map;

/**
 * Map implementation that generates a copy of itself on each update, so all references to the previous version of the
 * map are safe from modifications on the new version. All the collection references to the map elements on this class
 * are read-only.
 */
@NotThreadSafe
public class CopyOnWriteMapUtils {

    public static <T, R> Map<T, R> put(Map<T, R> oldMap, T key, R value) {
        Map<T, R> newMap = new HashMap<>(oldMap);
        newMap.put(key, value);
        return newMap;
    }

    public static <T, R> Map<T, R> remove(Map<T, R> oldMap, Object key) {
        Map<T, R> newMap = new HashMap<>(oldMap);
        newMap.remove(key);
        return newMap;
    }

    // End region
}