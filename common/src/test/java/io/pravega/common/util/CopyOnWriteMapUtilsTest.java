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

import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class CopyOnWriteMapUtilsTest {

    @Test
    public void testCopyOnWritePut() {
        Map<String, Integer> oldMap = new HashMap<>();
        oldMap.put("a", 0);
        oldMap.put("b", 1);
        Map<String, Integer> newMap = CopyOnWriteMapUtils.put(oldMap, "c", 2);
        // Check that the new map contains the previous elements plus the new one.
        Assert.assertTrue(newMap.containsKey("a"));
        Assert.assertTrue(newMap.containsKey("c"));
        // Check that the oldMap and the newMap are actually distinct objects.
        Assert.assertNotSame(newMap, oldMap);
        oldMap.remove("a");
        Assert.assertNotNull(newMap.get("a"));
    }

    @Test
    public void testCopyOnWriteRemove() {
        Map<String, Integer> oldMap = new HashMap<>();
        oldMap.put("a", 0);
        oldMap.put("b", 1);
        Map<String, Integer> newMap = CopyOnWriteMapUtils.remove(oldMap, "b");
        // Check that the new map does not contain the removed element.
        Assert.assertFalse(newMap.containsKey("b"));
        Assert.assertTrue(oldMap.containsKey("b"));
        // Check that the oldMap and the newMap are actually distinct objects.
        Assert.assertNotSame(newMap, oldMap);
        oldMap.remove("a");
        Assert.assertNotNull(newMap.get("a"));
    }
}
