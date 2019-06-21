/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.server.containers;

import io.pravega.common.util.ImmutableDate;
import io.pravega.segmentstore.contracts.Attributes;
import io.pravega.segmentstore.server.SegmentMetadataComparer;
import io.pravega.segmentstore.server.UpdateableSegmentMetadata;
import io.pravega.test.common.AssertExtensions;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.function.BiPredicate;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.val;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

/**
 * Unit tests for StreamSegmentMetadata class.
 */
public class StreamSegmentMetadataTests {
    private static final String SEGMENT_NAME = "Segment";
    private static final long SEGMENT_ID = 1;
    private static final int CONTAINER_ID = 1234567;
    private static final int ATTRIBUTE_COUNT = 100;
    @Rule
    public Timeout globalTimeout = Timeout.seconds(10);

    /**
     * Tests that Attributes are properly recorded and updated
     */
    @Test
    public void testAttributes() {
        StreamSegmentMetadata metadata = new StreamSegmentMetadata(SEGMENT_NAME, SEGMENT_ID, CONTAINER_ID);

        // Step 1: initial set of attributes.
        Random rnd = new Random(0);
        val expectedAttributes = generateAttributes(rnd);

        metadata.updateAttributes(expectedAttributes);
        SegmentMetadataComparer.assertSameAttributes("Unexpected attributes after initial set.", expectedAttributes, metadata);

        // Step 2: Update half of attributes and add 50% more.
        int count = 0;
        val keyIterator = expectedAttributes.keySet().iterator();
        val attributeUpdates = new HashMap<UUID, Long>();

        // Update
        while (count < ATTRIBUTE_COUNT / 2 && keyIterator.hasNext()) {
            attributeUpdates.put(keyIterator.next(), rnd.nextLong());
            count++;
        }

        // Now add a few more.
        while (attributeUpdates.size() < ATTRIBUTE_COUNT) {
            attributeUpdates.put(UUID.randomUUID(), rnd.nextLong());
        }

        attributeUpdates.forEach(expectedAttributes::put);
        metadata.updateAttributes(attributeUpdates);
        SegmentMetadataComparer.assertSameAttributes("Unexpected attributes after update.", expectedAttributes, metadata);

        // Check getAttributes(filter).
        BiPredicate<UUID, Long> filter = (key, value) -> key.getLeastSignificantBits() % 2 == 0;
        val expectedFilteredAttributes = expectedAttributes.entrySet().stream()
                                                           .filter(e -> filter.test(e.getKey(), e.getValue()))
                                                           .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        val actualFilteredAttributes = metadata.getAttributes(filter);
        AssertExtensions.assertMapEquals("Unexpected result from getAttributes(Filter).", expectedFilteredAttributes, actualFilteredAttributes);

        // Step 3: Remove all attributes (Note that attributes are not actually removed; they're set to the NULL_ATTRIBUTE_VALUE).
        expectedAttributes.entrySet().forEach(e -> e.setValue(Attributes.NULL_ATTRIBUTE_VALUE));
        metadata.updateAttributes(expectedAttributes);
        SegmentMetadataComparer.assertSameAttributes("Unexpected attributes after removal.", expectedAttributes, metadata);
    }

    /**
     * Tests the ability to cleanup Extended Attributes.
     */
    @Test
    public void testCleanupAttributes() {
        final UUID coreAttributeId = Attributes.EVENT_COUNT;
        final int attributeCount = 10000;
        final int maxAttributeCount = attributeCount / 10;

        // Initial population.
        StreamSegmentMetadata metadata = new StreamSegmentMetadata(SEGMENT_NAME, SEGMENT_ID, CONTAINER_ID);
        val extendedAttributes = new ArrayList<UUID>();
        val expectedValues = new HashMap<UUID, Long>();
        expectedValues.put(coreAttributeId, 1000L);
        metadata.updateAttributes(Collections.singletonMap(coreAttributeId, 1000L));
        for (int i = 0; i < attributeCount; i++) {
            UUID attributeId = new UUID(0, (long) i);
            extendedAttributes.add(attributeId);
            metadata.setLastUsed(i);
            metadata.updateAttributes(Collections.singletonMap(attributeId, (long) i));
            expectedValues.put(attributeId, (long) i);
        }
        checkAttributesEqual(expectedValues, metadata.getAttributes());

        // Evict first half of the attributes.
        int half = attributeCount / 2;
        int step = maxAttributeCount / 10;
        for (int i = 0; i <= half; i += step) {
            int evicted = metadata.cleanupAttributes(maxAttributeCount, i);
            if (i == 0) {
                Assert.assertEquals("Not expecting any evictions.", 0, evicted);
            } else {
                Assert.assertEquals("Unexpected number of evictions", step, evicted);
                for (int j = i - step; j < i; j++) {
                    expectedValues.remove(extendedAttributes.get(j));
                }
            }

            checkAttributesEqual(expectedValues, metadata.getAttributes());
        }

        // For the second half, every 3rd attribute is not touched, every 3rd+1 is updated and every 3rd+2 is fetched.
        // We then verify that only the untouched ones will get evicted.
        int expectedEvicted = 0;
        long cutoff = metadata.getLastUsed();
        metadata.setLastUsed(cutoff + 1);
        for (int i = half; i < attributeCount; i++) {
            val attributeId = extendedAttributes.get(i);
            if (i % 3 == 1) {
                // We reuse the same value; it's simpler.
                metadata.updateAttributes(Collections.singletonMap(attributeId, (long) i));
            } else if (i % 3 == 2) {
                metadata.getAttributes().get(attributeId);
            } else {
                expectedValues.remove(attributeId);
                expectedEvicted++;
            }
        }

        // Force an eviction of all attributes, and verify that only the ones eligible for removal were removed.
        int evicted = metadata.cleanupAttributes(maxAttributeCount, cutoff + 1);
        Assert.assertEquals("Unexpected final eviction count.", expectedEvicted, evicted);
        checkAttributesEqual(expectedValues, metadata.getAttributes());
    }

    /**
     * Verifies the given maps are equal without actually invoking get() or getOrDefault() on actual; to prevent lastUsed
     * from being updated.
     */
    private void checkAttributesEqual(Map<UUID, Long> expected, Map<UUID, Long> actual) {
        Assert.assertEquals("Sizes differ.", expected.size(), actual.size());
        for (val e : actual.entrySet()) {
            Assert.assertEquals("Unexpected value found.", expected.get(e.getKey()), e.getValue());
        }
    }

    /**
     * Tests the copyFrom() method.
     */
    @Test
    public void testCopyFrom() {
        Stream.<Consumer<UpdateableSegmentMetadata>>of(
                UpdateableSegmentMetadata::markMerged,
                m -> m.setStartOffset(1200),
                UpdateableSegmentMetadata::markSealedInStorage)
                .forEach(c -> {
                    val metadata = new StreamSegmentMetadata(SEGMENT_NAME, SEGMENT_ID, CONTAINER_ID);
                    metadata.markSealed();
                    metadata.setLength(3235342);
                    c.accept(metadata);
                    testCopyFrom(metadata);
                });
    }

    private void testCopyFrom(StreamSegmentMetadata baseMetadata) {
        baseMetadata.setStorageLength(1233);
        baseMetadata.updateAttributes(generateAttributes(new Random(0)));
        baseMetadata.setLastModified(new ImmutableDate());
        baseMetadata.markDeleted();
        baseMetadata.markDeletedInStorage();
        baseMetadata.markInactive();
        baseMetadata.setLastUsed(1545895);
        baseMetadata.markPinned();

        // Normal metadata copy.
        StreamSegmentMetadata newMetadata = new StreamSegmentMetadata(baseMetadata.getName(), baseMetadata.getId(), baseMetadata.getContainerId());
        newMetadata.copyFrom(baseMetadata);
        Assert.assertTrue("copyFrom copied the Active flag too.", newMetadata.isActive());
        SegmentMetadataComparer.assertEquals("Metadata copy:", baseMetadata, newMetadata);
        Assert.assertEquals("Metadata copy: getLastUsed differs.",
                baseMetadata.getLastUsed(), newMetadata.getLastUsed());

        // Verify we cannot copy from different StreamSegments.
        AssertExtensions.assertThrows(
                "copyFrom allowed copying from a metadata with a different Segment Name",
                () -> new StreamSegmentMetadata("foo", SEGMENT_ID, CONTAINER_ID).copyFrom(baseMetadata),
                ex -> ex instanceof IllegalArgumentException);

        AssertExtensions.assertThrows(
                "copyFrom allowed copying from a metadata with a different Segment Id",
                () -> new StreamSegmentMetadata(SEGMENT_NAME, -SEGMENT_ID, CONTAINER_ID).copyFrom(baseMetadata),
                ex -> ex instanceof IllegalArgumentException);
    }

    private Map<UUID, Long> generateAttributes(Random rnd) {
        val result = new HashMap<UUID, Long>();
        for (int i = 0; i < ATTRIBUTE_COUNT; i++) {
            result.put(UUID.randomUUID(), rnd.nextLong());
        }

        return result;
    }
}
