/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.server;

import io.pravega.segmentstore.contracts.SegmentProperties;
import java.util.Map;
import java.util.UUID;
import lombok.val;
import org.junit.Assert;

/**
 * Helps compare SegmentMetadata instances.
 */
public final class SegmentMetadataComparer {

    /**
     * Verifies that the given SegmentMetadata instances contain the same data.
     *
     * @param message  The message to include in the assertion failures.
     * @param expected The SegmentMetadata instance to compare against.
     * @param actual   The SegmentMetadata instance to compare.
     */
    public static void assertEquals(String message, SegmentMetadata expected, SegmentMetadata actual) {
        String idPrefix = message + " SegmentId " + expected.getId();
        Assert.assertEquals(idPrefix + " getId() mismatch.", expected.getId(), actual.getId());
        Assert.assertEquals(idPrefix + " isDeleted() mismatch.", expected.isDeleted(), actual.isDeleted());
        Assert.assertEquals(idPrefix + " getStorageLength() mismatch.", expected.getStorageLength(), actual.getStorageLength());
        Assert.assertEquals(idPrefix + " getStartOffset() mismatch.", expected.getStartOffset(), actual.getStartOffset());
        Assert.assertEquals(idPrefix + " getLength() mismatch.", expected.getLength(), actual.getLength());
        Assert.assertEquals(idPrefix + " getName() mismatch.", expected.getName(), actual.getName());
        Assert.assertEquals(idPrefix + " isSealed() mismatch.", expected.isSealed(), actual.isSealed());
        Assert.assertEquals(idPrefix + " isMerged() mismatch.", expected.isMerged(), actual.isMerged());
        assertSameAttributes(idPrefix + " getAttributes() mismatch:", expected.getAttributes(), actual);
        Assert.assertEquals(idPrefix + " isPinned() mismatch.", expected.isPinned(), actual.isPinned());
    }

    /**
     * Verifies that the given SegmentMetadata instance has the expected Attributes.
     *
     * @param message  The message to include in the assertion failures.
     * @param expected The expected set of Attributes.
     * @param toCheck The SegmentProperties instance to verify.
     */
    public static void assertSameAttributes(String message, Map<UUID, Long> expected, SegmentProperties toCheck) {
        val actual = toCheck.getAttributes();
        if (expected == null) {
            Assert.assertEquals(message + " No attributes expected.", 0, actual.size());
            return;
        }

        Assert.assertEquals(message + " Counts differ.", expected.size(), actual.size());
        for (Map.Entry<UUID, Long> e : expected.entrySet()) {
            Assert.assertTrue(" attribute not found " + e.getKey(), actual.containsKey(e.getKey()));
            long actualValue = actual.get(e.getKey());
            Assert.assertEquals(message + " value differs.", (long) e.getValue(), actualValue);
        }
    }
}
