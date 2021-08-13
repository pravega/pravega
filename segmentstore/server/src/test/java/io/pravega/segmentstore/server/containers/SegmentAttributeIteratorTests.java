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
package io.pravega.segmentstore.server.containers;

import com.google.common.collect.Maps;
import io.pravega.segmentstore.contracts.AttributeId;
import io.pravega.segmentstore.server.AttributeIterator;
import io.pravega.segmentstore.server.SegmentMetadata;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.ThreadPooledTestSuite;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import lombok.Builder;
import lombok.Singular;
import lombok.val;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for the {@link SegmentAttributeIterator} class.
 */
public class SegmentAttributeIteratorTests extends ThreadPooledTestSuite {
    private static final int ITERATOR_COUNT = 20;
    private static final int METADATA_COUNT = 100;
    private static final double ATTRIBUTE_OVERLAP_RATIO = 0.4; // How many attributes in Metadata overlap base Attributes.

    @Override
    protected int getThreadPoolSize() {
        return 1;
    }

    /**
     * Tests a scenario where all the attributes come only from the base iterators.
     */
    @Test
    public void testBaseOnly() {
        val testData = createTestData(ITERATOR_COUNT, 0);
        test(testData);
    }

    /**
     * Tests a scenario where all attributes come only from the Segment Metadata.
     */
    @Test
    public void testMetadataOnly() {
        val testData = createTestData(0, METADATA_COUNT);
        test(testData);
    }

    /**
     * Test a scenario where attributes come both from the base iterators and from the metadata.
     */
    @Test
    public void testAllSources() {
        val testData = createTestData(ITERATOR_COUNT, METADATA_COUNT);
        test(testData);
    }

    /**
     * Tests a scenario where the base iterators returned attributes out of order.
     */
    @Test
    public void testNotSorted() {
        val testData = createTestData(ITERATOR_COUNT, METADATA_COUNT);
        testData.baseIteratorAttributes.forEach(list -> {
            if (list.size() > 1) {
                val tmp = list.get(0);
                list.set(0, list.get(1));
                list.set(1, tmp);
            }
        });

        AttributeId fromId = testData.sortedAttributeIds.get(0);
        AttributeId toId = testData.sortedAttributeIds.get(testData.sortedAttributeIds.size() - 1);
        val iterator = new SegmentAttributeIterator(testData.getAttributeIterator(fromId, toId), testData.segmentMetadata, fromId, toId);
        AssertExtensions.assertSuppliedFutureThrows(
                "getNext() did not throw when iterators returned data out of order.",
                () -> iterator.forEachRemaining(CompletableFuture::completedFuture, executorService()),
                ex -> ex instanceof IllegalArgumentException);
    }

    /**
     * Tests a scenario where the base iterators return data out of range.
     */
    @Test
    public void testOutOfRange() {
        val testData = createTestData(ITERATOR_COUNT, METADATA_COUNT);
        testData.baseIteratorAttributes.forEach(list -> {
            if (list.size() > 1) {
                val tmp = list.get(0);
                list.set(0, list.get(1));
                list.set(1, tmp);
            }
        });

        // We generate the iterators to include more data than we provide to the mixer.
        AttributeId fromId = testData.sortedAttributeIds.get(1);
        AttributeId toId = testData.sortedAttributeIds.get(testData.sortedAttributeIds.size() - 1);
        val iterator = new SegmentAttributeIterator(testData.getAttributeIterator(fromId, toId), testData.segmentMetadata, fromId, toId);
        AssertExtensions.assertSuppliedFutureThrows(
                "getNext() did not throw when iterators returned data out of range.",
                () -> iterator.forEachRemaining(CompletableFuture::completedFuture, executorService()),
                ex -> ex instanceof IllegalArgumentException);
    }

    private void test(TestData testData) {
        for (int i = 0; i < testData.sortedAttributeIds.size() / 2; i++) {
            AttributeId fromId = testData.sortedAttributeIds.get(i);
            AttributeId toId = testData.sortedAttributeIds.get(testData.sortedAttributeIds.size() - i - 1);
            val iterator = new SegmentAttributeIterator(testData.getAttributeIterator(fromId, toId), testData.segmentMetadata, fromId, toId);
            val finalResult = new ArrayList<Map.Entry<AttributeId, Long>>();
            val ids = new HashSet<AttributeId>();
            iterator.forEachRemaining(intermediateResult -> {
                for (val e : intermediateResult) {
                    Assert.assertTrue("Duplicate key found: " + e.getKey(), ids.add(e.getKey()));
                    finalResult.add(e);
                }
            }, executorService()).join();

            val expectedResult = testData.expectedResult
                    .stream()
                    .filter(e -> isBetween(e.getKey(), fromId, toId))
                    .collect(Collectors.toList());

            AssertExtensions.assertListEquals("Unexpected final result.", expectedResult, finalResult,
                    (e1, e2) -> e1.getKey().equals(e2.getKey()) && e1.getValue().equals(e2.getValue()));
        }
    }

    private static boolean isBetween(AttributeId toCheck, AttributeId fromId, AttributeId toId) {
        return fromId.compareTo(toCheck) <= 0 && toId.compareTo(toCheck) >= 0;
    }

    private TestData createTestData(int baseIteratorCount, int metadataAttributeCount) {
        val rnd = new Random(0);
        val builder = TestData.builder();
        val expectedResult = new HashMap<AttributeId, Long>();
        val sortedAttributeIds = new ArrayList<AttributeId>();
        int nextAttributeId = 0;

        // Create base iterators.
        int iteratorSize = 0;
        for (int i = 0; i < baseIteratorCount; i++) {
            val indexItems = new ArrayList<Map.Entry<AttributeId, Long>>(iteratorSize);
            for (int j = 0; j < iteratorSize; j++) {
                AttributeId attributeId = AttributeId.uuid(nextAttributeId, nextAttributeId);
                long attributeValue = rnd.nextLong();
                indexItems.add(Maps.immutableEntry(attributeId, attributeValue));
                sortedAttributeIds.add(attributeId);
                expectedResult.put(attributeId, attributeValue);
                nextAttributeId++;
            }

            builder.baseIteratorAttribute(indexItems);
            iteratorSize++; // Next iterator will have one more item.
        }

        // Create metadata
        val metadata = new StreamSegmentMetadata("Segment", 0, 0);
        metadata.setLength(0);
        for (int i = 0; i < metadataAttributeCount; i++) {
            AttributeId attributeId = baseIteratorCount <= 1 ? null : sortedAttributeIds.get(rnd.nextInt(sortedAttributeIds.size()));
            if (attributeId == null) {
                attributeId = AttributeId.uuid(nextAttributeId, nextAttributeId);
                nextAttributeId++;
            } else if (rnd.nextDouble() > ATTRIBUTE_OVERLAP_RATIO) {
                // Do not reuse attribute id, but choose one nearby.
                attributeId = AttributeId.uuid(attributeId.getBitGroup(0) + 1, attributeId.getBitGroup(1) + 1);
            }

            long attributeValue = rnd.nextLong();
            expectedResult.put(attributeId, attributeValue);
            metadata.updateAttributes(Collections.singletonMap(attributeId, attributeValue));
        }

        sortedAttributeIds.addAll(metadata.getAttributes().keySet());
        sortedAttributeIds.sort(AttributeId::compareTo);

        return builder.segmentMetadata(metadata)
                      .expectedResult(expectedResult.entrySet().stream().sorted(Comparator.comparing(Map.Entry::getKey)).collect(Collectors.toList()))
                      .sortedAttributeIds(sortedAttributeIds)
                      .build();
    }

    @Builder
    private static class TestData {
        private final List<AttributeId> sortedAttributeIds;
        @Singular
        private final List<List<Map.Entry<AttributeId, Long>>> baseIteratorAttributes;
        private final SegmentMetadata segmentMetadata;
        private final List<Map.Entry<AttributeId, Long>> expectedResult;

        AttributeIterator getAttributeIterator(AttributeId fromId, AttributeId toId) {
            val baseIterator = baseIteratorAttributes.iterator();
            return () -> CompletableFuture.completedFuture(
                    baseIterator.hasNext()
                            ? baseIterator.next().stream().filter(e -> isBetween(e.getKey(), fromId, toId)).collect(Collectors.toList())
                            : null);
        }
    }
}
