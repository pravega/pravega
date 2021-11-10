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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.pravega.controller.store.stream.records.StreamSegmentRecord;
import io.pravega.test.common.AssertExtensions;
import org.apache.curator.shaded.com.google.common.base.Charsets;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import static io.pravega.cli.admin.serializers.AbstractSerializer.appendField;
import static io.pravega.cli.admin.serializers.AbstractSerializer.applyDeserializer;
import static io.pravega.cli.admin.serializers.AbstractSerializer.convertCollectionToString;
import static io.pravega.cli.admin.serializers.AbstractSerializer.convertMapToString;
import static io.pravega.cli.admin.serializers.AbstractSerializer.convertStreamSegmentRecordToString;
import static io.pravega.cli.admin.serializers.AbstractSerializer.convertStringToCollection;
import static io.pravega.cli.admin.serializers.AbstractSerializer.convertStringToMap;
import static io.pravega.cli.admin.serializers.AbstractSerializer.convertStringToStreamSegmentRecord;
import static io.pravega.cli.admin.serializers.AbstractSerializer.getAndRemoveIfExists;
import static io.pravega.cli.admin.serializers.AbstractSerializer.parseStringData;
import static io.pravega.cli.admin.utils.TestUtils.generateStreamSegmentRecordString;
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

    @Test
    public void testApplyDeserializer() {
        String field = "test";
        String value = "hello";
        Map<String, Function<String, String>> fieldMap = ImmutableMap.of(field, s -> s);
        String testString = field + "=" + value + ";";
        ByteBuffer bb = ByteBuffer.wrap(value.getBytes(Charsets.UTF_8));
        Assert.assertEquals(testString, applyDeserializer(bb, bytes -> new String(bytes, StandardCharsets.UTF_8), fieldMap));
    }

    @Test
    public void testConvertCollectionToString() {
        Assert.assertEquals("EMPTY", convertCollectionToString(ImmutableList.of(), s -> ""));
        Assert.assertEquals("1,2,3", convertCollectionToString(ImmutableList.of(1, 2, 3), String::valueOf));
    }

    @Test
    public void testConvertStringToCollection() {
        Assert.assertEquals(Collections.EMPTY_LIST, convertStringToCollection("EMPTY", s -> ""));
        Assert.assertEquals(ImmutableList.of(1, 2, 3), convertStringToCollection("1,2,3", Integer::parseInt));
    }

    @Test
    public void testConvertMapToString() {
        Assert.assertEquals("EMPTY", convertMapToString(ImmutableMap.of(), s -> "", s -> ""));
        Assert.assertEquals("1:2,3:4,5:6", convertMapToString(ImmutableMap.of(1, 2, 3, 4, 5, 6), String::valueOf, String::valueOf));
    }

    @Test
    public void testConvertStringToMap() {
        Assert.assertEquals(Collections.EMPTY_MAP, convertStringToMap("EMPTY", s -> s, s -> s, "test"));
        Assert.assertEquals(ImmutableMap.of(1, 2, 3, 4, 5, 6), convertStringToMap("1:2,3:4,5:6",
                Integer::parseInt, Integer::parseInt, "test"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConvertStringToMapArgumentFailure() {
        convertStringToMap("1:2,3:,5:6", Integer::parseInt, Integer::parseInt, "test");
    }

    @Test
    public void testConvertStreamSegmentRecordToString() {
        Assert.assertEquals(generateStreamSegmentRecordString(1, 2, 2L, 0.1, 0.9),
                convertStreamSegmentRecordToString(new StreamSegmentRecord(1, 2, 2L, 0.1, 0.9)));
    }

    @Test
    public void testConvertStringToStreamSegmentRecord() {
        StreamSegmentRecord expected = new StreamSegmentRecord(1, 2, 2L, 0.1, 0.9);
        StreamSegmentRecord actual = convertStringToStreamSegmentRecord(generateStreamSegmentRecordString(1, 2, 2L, 0.1, 0.9));
        Assert.assertEquals(expected.getSegmentNumber(), actual.getSegmentNumber());
        Assert.assertEquals(expected.getCreationEpoch(), actual.getCreationEpoch());
        Assert.assertEquals(expected.getCreationTime(), actual.getCreationTime());
        Assert.assertEquals(expected.getKeyStart(), actual.getKeyStart(), 0.0);
        Assert.assertEquals(expected.getKeyEnd(), actual.getKeyEnd(), 0.0);
    }
}
