/**
 * Copyright Pravega Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.pravega.common.util;

import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class CopyOnWriteHashMapTest {

    @Test
    public void testCopyOnWritePut() {
        Map<String, Integer> oldMap = new HashMap<>();
        oldMap.put("a", 0);
        oldMap.put("b", 1);
        CopyOnWriteHashMap<String, Integer> onWriteHashMap = new CopyOnWriteHashMap<>(oldMap);
        onWriteHashMap.put("c", 2);
        // Check that the new map contains the previous elements plus the new one.
        Assert.assertTrue(onWriteHashMap.containsKey("a"));
        Assert.assertTrue(onWriteHashMap.containsValue(0));
        Assert.assertTrue(onWriteHashMap.containsKey("c"));
        Assert.assertTrue(onWriteHashMap.containsValue(2));
        // Check that the oldMap and the newMap are actually distinct objects.
        Assert.assertNotSame(onWriteHashMap.getInnerMap(), oldMap);
        // Remove element from the original map and check that the copyOnWrite map is not impacted.
        Assert.assertEquals(0, (int) oldMap.remove("a"));
        Assert.assertTrue(onWriteHashMap.containsKey("a"));
        Assert.assertNotNull(onWriteHashMap.get("a"));
        Assert.assertTrue(onWriteHashMap.containsValue(0));
        // Check Non-existent keys and values.
        Assert.assertFalse(onWriteHashMap.containsKey("d"));
        Assert.assertFalse(onWriteHashMap.containsValue(3));
        // Clear the map.
        onWriteHashMap.clear();
        Assert.assertEquals(0, onWriteHashMap.size());
    }

    @Test
    public void testCopyOnWriteRemove() {
        CopyOnWriteHashMap<String, Integer> onWriteHashMap = new CopyOnWriteHashMap<>();
        Map<String, Integer> oldMap = new HashMap<>();
        oldMap.put("a", 0);
        oldMap.put("b", 1);
        // Exercise putAll this time.
        onWriteHashMap.putAll(oldMap);
        onWriteHashMap.remove("b");
        // Check that the new map does not contain the removed element.
        Assert.assertFalse(onWriteHashMap.containsKey("b"));
        Assert.assertTrue(oldMap.containsKey("b"));
        // Check that the oldMap and the newMap are actually distinct objects.
        Assert.assertNotSame(onWriteHashMap.getInnerMap(), oldMap);
        Assert.assertEquals(0, (int) oldMap.remove("a"));
        // Check that removal on the old map does not impact on the copyOnWrite map.
        Assert.assertNotNull(onWriteHashMap.get("a"));
        Assert.assertTrue(onWriteHashMap.containsKey("a"));
        Assert.assertFalse(onWriteHashMap.containsValue(1));
        // Clear the map.
        onWriteHashMap.clear();
        Assert.assertEquals(0, onWriteHashMap.size());
    }

    @Test
    public void testReplace() {
        CopyOnWriteHashMap<String, Integer> onWriteHashMap = new CopyOnWriteHashMap<>();
        Map<String, Integer> oldMap = new HashMap<>();
        oldMap.put("a", 0);
        oldMap.put("b", 1);
        onWriteHashMap.putAll(oldMap);
        // Exercise putIfAbsent call.
        Assert.assertNotNull(onWriteHashMap.putIfAbsent("a", 1));
        Assert.assertNull(onWriteHashMap.putIfAbsent("c", 2));
        Assert.assertEquals(3, onWriteHashMap.entrySet().size());
        // Check that the oldMap is not impacted.
        Assert.assertNull(oldMap.get("c"));
        // Replace values in copyOnWrite map.
        Assert.assertEquals(2, (int) onWriteHashMap.replace("c", 3));
        Assert.assertFalse(onWriteHashMap.replace("c", 2, 2));
        Assert.assertTrue(onWriteHashMap.replace("c", 3, 4));
    }
}
