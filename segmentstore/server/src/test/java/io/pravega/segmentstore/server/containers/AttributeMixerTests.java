/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.server.containers;

import io.pravega.segmentstore.server.SegmentMetadata;
import io.pravega.test.common.AssertExtensions;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.stream.Collectors;
import lombok.Builder;
import lombok.Singular;
import lombok.val;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for the {@link AttributeMixer} class.
 */
public class AttributeMixerTests {
    private static final int ITERATOR_COUNT = 20;
    private static final int METADATA_COUNT = 100;
    private static final double ATTRIBUTE_OVERLAP_RATIO = 0.4; // How many attributes in Metadata overlap base Attributes.

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
        testData.baseIterators.forEach(list -> {
            if (list.size() > 1) {
                val tmp = list.get(0);
                list.set(0, list.get(1));
                list.set(1, tmp);
            }
        });

        UUID fromId = testData.sortedAttributeIds.get(0);
        UUID toId = testData.sortedAttributeIds.get(testData.sortedAttributeIds.size() - 1);
        val iterators = createIterators(testData, fromId, toId);
        val mixer = new AttributeMixer(testData.segmentMetadata, fromId, toId);
        AssertExtensions.assertThrows(
                "mix() did not throw when iterators returned data out of order.",
                () -> iterators.forEach(mixer::mix),
                ex -> ex instanceof IllegalArgumentException);
    }

    /**
     * Tests a scenario where the base iterators return data out of range.
     */
    @Test
    public void testOutOfRange() {
        val testData = createTestData(ITERATOR_COUNT, METADATA_COUNT);
        testData.baseIterators.forEach(list -> {
            if (list.size() > 1) {
                val tmp = list.get(0);
                list.set(0, list.get(1));
                list.set(1, tmp);
            }
        });

        // We generate the iterators to include more data than we provide to the mixer.
        UUID toId = testData.sortedAttributeIds.get(testData.sortedAttributeIds.size() - 1);
        val iterators = createIterators(testData, testData.sortedAttributeIds.get(0), toId);
        val mixer = new AttributeMixer(testData.segmentMetadata, testData.sortedAttributeIds.get(1), toId);
        AssertExtensions.assertThrows(
                "mix() did not throw when iterators returned data out of range.",
                () -> iterators.forEach(mixer::mix),
                ex -> ex instanceof IllegalArgumentException);
    }

    private void test(TestData testData) {
        for (int i = 0; i < testData.sortedAttributeIds.size() / 2; i++) {
            UUID fromId = testData.sortedAttributeIds.get(i);
            UUID toId = testData.sortedAttributeIds.get(testData.sortedAttributeIds.size() - i - 1);
            val iterators = createIterators(testData, fromId, toId);
            val mixer = new AttributeMixer(testData.segmentMetadata, fromId, toId);

            val finalResult = new HashMap<UUID, Long>();
            for (int j = 0; j < iterators.size(); j++) {
                val intermediateResult = mixer.mix(iterators.get(j));
                if (intermediateResult == null) {
                    Assert.assertEquals("Not expecting a null result for non-terminal input.", iterators.size() - 1, j);
                } else {
                    for (val e : intermediateResult.entrySet()) {
                        Assert.assertFalse("Duplicate key found: " + e.getKey(), finalResult.containsKey(e.getKey()));
                        finalResult.put(e.getKey(), e.getValue());
                    }
                }
            }

            val expectedResult = testData.expectedResult
                    .entrySet().stream()
                    .filter(e -> isBetween(e.getKey(), fromId, toId))
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

            AssertExtensions.assertMapEquals("Unexpected final result.", expectedResult, finalResult);
        }
    }

    private List<Iterator<Map.Entry<UUID, Long>>> createIterators(TestData testData, UUID fromId, UUID toId) {
        val result = testData.baseIterators
                .stream()
                .map(list -> list.stream().filter(e -> isBetween(e.getKey(), fromId, toId)).iterator())
                .collect(Collectors.toList());
        result.add(null); // To indicate it's done.
        return result;
    }

    private boolean isBetween(UUID toCheck, UUID fromId, UUID toId) {
        return fromId.compareTo(toCheck) <= 0 && toId.compareTo(toCheck) >= 0;
    }

    private TestData createTestData(int baseIteratorCount, int metadataAttributeCount) {
        val rnd = new Random(0);
        val builder = TestData.builder();
        val expectedResult = new HashMap<UUID, Long>();
        val sortedAttributeIds = new ArrayList<UUID>();
        int nextAttributeId = 0;

        // Create base iterators.
        int iteratorSize = 0;
        for (int i = 0; i < baseIteratorCount; i++) {
            val iteratorItems = new ArrayList<Map.Entry<UUID, Long>>(iteratorSize);
            for (int j = 0; j < iteratorSize; j++) {
                UUID attributeId = new UUID(nextAttributeId, nextAttributeId);
                long attributeValue = rnd.nextLong();
                iteratorItems.add(new AbstractMap.SimpleImmutableEntry<>(attributeId, attributeValue));
                sortedAttributeIds.add(attributeId);
                expectedResult.put(attributeId, attributeValue);
                nextAttributeId++;
            }

            builder.baseIterator(iteratorItems);
            iteratorSize++; // Next iterator will have one more item.
        }

        // Create metadata
        val metadata = new StreamSegmentMetadata("Segment", 0, 0);
        metadata.setLength(0);
        for (int i = 0; i < metadataAttributeCount; i++) {
            UUID attributeId = baseIteratorCount <= 1 ? null : sortedAttributeIds.get(rnd.nextInt(sortedAttributeIds.size()));
            if (attributeId == null) {
                attributeId = new UUID(nextAttributeId, nextAttributeId);
                nextAttributeId++;
            } else if (rnd.nextDouble() > ATTRIBUTE_OVERLAP_RATIO) {
                // Do not reuse attribute id, but choose one nearby.
                attributeId = new UUID(attributeId.getMostSignificantBits() + 1, attributeId.getLeastSignificantBits() + 1);
            }

            long attributeValue = rnd.nextLong();
            expectedResult.put(attributeId, attributeValue);
            metadata.updateAttributes(Collections.singletonMap(attributeId, attributeValue));
        }

        sortedAttributeIds.addAll(metadata.getAttributes().keySet());
        sortedAttributeIds.sort(UUID::compareTo);
        builder.sortedAttributeIds(sortedAttributeIds);

        return builder.segmentMetadata(metadata)
                      .expectedResult(expectedResult)
                      .build();
    }

    @Builder
    private static class TestData {
        @Singular
        private final List<List<Map.Entry<UUID, Long>>> baseIterators;
        private final List<UUID> sortedAttributeIds;
        private final SegmentMetadata segmentMetadata;
        private final Map<UUID, Long> expectedResult;
    }
}
