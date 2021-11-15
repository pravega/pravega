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
package io.pravega.cli.admin.serializers;

import io.pravega.test.common.AssertExtensions;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static io.pravega.cli.admin.serializers.AbstractSerializer.appendField;
import static io.pravega.cli.admin.serializers.AbstractSerializer.getAndRemoveIfExists;
import static io.pravega.cli.admin.serializers.AbstractSerializer.parseStringData;
import static java.util.stream.IntStream.range;

public class SerializerTest {

    @Test
    public void testAppendField() {
        String testKey = "key";
        String testValue = "value";
        StringBuilder testBuilder = new StringBuilder();
        appendField(testBuilder, testKey, testValue);
        Assert.assertTrue(testBuilder.toString().contains(String.format("%s=%s;", testKey, testValue)));
    }

    @Test
    public void testParseStringData() {
        int total = 4;
        StringBuilder testBuilder = new StringBuilder();
        range(1, total).forEach(i -> appendField(testBuilder, "key" + i, "value" + i));
        Map<String, String> dataMap = parseStringData(testBuilder.toString());
        range(1, total).forEach(i -> {
            Assert.assertTrue(dataMap.containsKey("key" + i));
            Assert.assertEquals("value" + i, dataMap.get("key" + i));
        });
    }

    @Test
    public void testGetAndRemoveIfExists() {
        String testKey = "key1";
        String testValue = "value1";
        Map<String, String> testMap = new HashMap<>();
        testMap.put(testKey, testValue);
        Assert.assertEquals(testValue, getAndRemoveIfExists(testMap, testKey));
        Assert.assertFalse(testMap.containsKey(testKey));
        AssertExtensions.assertThrows(IllegalArgumentException.class, () -> getAndRemoveIfExists(testMap, testKey));
    }
}
