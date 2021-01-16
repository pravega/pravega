/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.storage.chunklayer;

import io.pravega.common.util.ConfigurationException;
import io.pravega.common.util.Property;
import io.pravega.common.util.TypedProperties;
import io.pravega.test.common.AssertExtensions;
import lombok.val;
import org.junit.Assert;
import org.junit.Test;

import java.util.Properties;

/**
 * Tests for {@link ChunkedSegmentStorageConfig}.
 */
public class ChunkedSegmentStorageConfigTests {
    @Test
    public void testProvidedValues() {
        Properties props = new Properties();
        props.setProperty(ChunkedSegmentStorageConfig.APPENDS_ENABLED.getFullName(ChunkedSegmentStorageConfig.COMPONENT_CODE), "false");
        props.setProperty(ChunkedSegmentStorageConfig.LAZY_COMMIT_ENABLED.getFullName(ChunkedSegmentStorageConfig.COMPONENT_CODE), "false");
        props.setProperty(ChunkedSegmentStorageConfig.INLINE_DEFRAG_ENABLED.getFullName(ChunkedSegmentStorageConfig.COMPONENT_CODE), "false");
        props.setProperty(ChunkedSegmentStorageConfig.MAX_BUFFER_SIZE_FOR_APPENDS.getFullName(ChunkedSegmentStorageConfig.COMPONENT_CODE), "1");
        props.setProperty(ChunkedSegmentStorageConfig.MAX_SIZE_LIMIT_FOR_CONCAT.getFullName(ChunkedSegmentStorageConfig.COMPONENT_CODE), "2");
        props.setProperty(ChunkedSegmentStorageConfig.MIN_SIZE_LIMIT_FOR_CONCAT.getFullName(ChunkedSegmentStorageConfig.COMPONENT_CODE), "3");
        props.setProperty(ChunkedSegmentStorageConfig.MAX_INDEXED_SEGMENTS.getFullName(ChunkedSegmentStorageConfig.COMPONENT_CODE), "4");
        props.setProperty(ChunkedSegmentStorageConfig.MAX_INDEXED_CHUNKS.getFullName(ChunkedSegmentStorageConfig.COMPONENT_CODE), "5");
        props.setProperty(ChunkedSegmentStorageConfig.MAX_INDEXED_CHUNKS_PER_SEGMENTS.getFullName(ChunkedSegmentStorageConfig.COMPONENT_CODE), "6");
        props.setProperty(ChunkedSegmentStorageConfig.DEFAULT_ROLLOVER_SIZE.getFullName(ChunkedSegmentStorageConfig.COMPONENT_CODE), "7");
        props.setProperty(ChunkedSegmentStorageConfig.SELF_CHECK_LATE_WARNING_THRESHOLD.getFullName(ChunkedSegmentStorageConfig.COMPONENT_CODE), "8");
        props.setProperty(ChunkedSegmentStorageConfig.GARBAGE_COLLECTION_DELAY.getFullName(ChunkedSegmentStorageConfig.COMPONENT_CODE), "9");
        props.setProperty(ChunkedSegmentStorageConfig.GARBAGE_COLLECTION_MAX_QUEUE_SIZE.getFullName(ChunkedSegmentStorageConfig.COMPONENT_CODE), "10");
        props.setProperty(ChunkedSegmentStorageConfig.GARBAGE_COLLECTION_MAX_CONCURRENCY.getFullName(ChunkedSegmentStorageConfig.COMPONENT_CODE), "11");
        props.setProperty(ChunkedSegmentStorageConfig.GARBAGE_COLLECTION_SLEEP.getFullName(ChunkedSegmentStorageConfig.COMPONENT_CODE), "12");
        props.setProperty(ChunkedSegmentStorageConfig.GARBAGE_COLLECTION_MAX_ATTEMPS.getFullName(ChunkedSegmentStorageConfig.COMPONENT_CODE), "13");

        TypedProperties typedProperties = new TypedProperties(props, "storage");
        ChunkedSegmentStorageConfig config = new ChunkedSegmentStorageConfig(typedProperties);
        Assert.assertFalse(config.isAppendEnabled());
        Assert.assertFalse(config.isLazyCommitEnabled());
        Assert.assertFalse(config.isInlineDefragEnabled());
        Assert.assertEquals(config.getMaxBufferSizeForChunkDataTransfer(), 1);
        Assert.assertEquals(config.getMaxSizeLimitForConcat(), 2);
        Assert.assertEquals(config.getMinSizeLimitForConcat(), 0); // Don't use appends for concat when appends are disabled.
        Assert.assertEquals(config.getMaxIndexedSegments(), 4);
        Assert.assertEquals(config.getMaxIndexedChunks(), 5);
        Assert.assertEquals(config.getMaxIndexedChunksPerSegment(), 6);
        Assert.assertEquals(config.getDefaultRollingPolicy().getMaxLength(), 7);
        Assert.assertEquals(config.getLateWarningThresholdInMillis(), 8);
        Assert.assertEquals(config.getGarbageCollectionDelay().toSeconds(), 9);
        Assert.assertEquals(config.getGarbageCollectionMaxQueueSize(), 10);
        Assert.assertEquals(config.getGarbageCollectionMaxConcurrency(), 11);
        Assert.assertEquals(config.getGarbageCollectionSleep().toSeconds(), 12);
        Assert.assertEquals(config.getGarbageCollectionMaxAttempts(), 13);
    }

    @Test
    public void testDefaultValues() {
        Properties props = new Properties();

        TypedProperties typedProperties = new TypedProperties(props, "storage");
        ChunkedSegmentStorageConfig config = new ChunkedSegmentStorageConfig(typedProperties);
        Assert.assertEquals(config.isAppendEnabled(), ChunkedSegmentStorageConfig.DEFAULT_CONFIG.isAppendEnabled());
        Assert.assertEquals(config.isLazyCommitEnabled(), ChunkedSegmentStorageConfig.DEFAULT_CONFIG.isLazyCommitEnabled());
        Assert.assertEquals(config.isInlineDefragEnabled(), ChunkedSegmentStorageConfig.DEFAULT_CONFIG.isInlineDefragEnabled());
        Assert.assertEquals(config.getMaxBufferSizeForChunkDataTransfer(), ChunkedSegmentStorageConfig.DEFAULT_CONFIG.getMaxBufferSizeForChunkDataTransfer());
        Assert.assertEquals(config.getMaxSizeLimitForConcat(), ChunkedSegmentStorageConfig.DEFAULT_CONFIG.getMaxSizeLimitForConcat());
        Assert.assertEquals(config.getMinSizeLimitForConcat(), ChunkedSegmentStorageConfig.DEFAULT_CONFIG.getMinSizeLimitForConcat());
        Assert.assertEquals(config.getMaxIndexedSegments(), ChunkedSegmentStorageConfig.DEFAULT_CONFIG.getMaxIndexedSegments());
        Assert.assertEquals(config.getMaxIndexedChunks(), ChunkedSegmentStorageConfig.DEFAULT_CONFIG.getMaxIndexedChunks());
        Assert.assertEquals(config.getMaxIndexedChunksPerSegment(), ChunkedSegmentStorageConfig.DEFAULT_CONFIG.getMaxIndexedChunksPerSegment());
        Assert.assertEquals(config.getDefaultRollingPolicy().getMaxLength(), ChunkedSegmentStorageConfig.DEFAULT_CONFIG.getDefaultRollingPolicy().getMaxLength());
        Assert.assertEquals(config.getLateWarningThresholdInMillis(), ChunkedSegmentStorageConfig.DEFAULT_CONFIG.getLateWarningThresholdInMillis());
        Assert.assertEquals(config.getGarbageCollectionDelay(), ChunkedSegmentStorageConfig.DEFAULT_CONFIG.getGarbageCollectionDelay());
        Assert.assertEquals(config.getGarbageCollectionMaxConcurrency(), ChunkedSegmentStorageConfig.DEFAULT_CONFIG.getGarbageCollectionMaxConcurrency());
        Assert.assertEquals(config.getGarbageCollectionMaxQueueSize(), ChunkedSegmentStorageConfig.DEFAULT_CONFIG.getGarbageCollectionMaxQueueSize());
        Assert.assertEquals(config.getGarbageCollectionSleep(), ChunkedSegmentStorageConfig.DEFAULT_CONFIG.getGarbageCollectionSleep());
        Assert.assertEquals(config.getGarbageCollectionMaxAttempts(), ChunkedSegmentStorageConfig.DEFAULT_CONFIG.getGarbageCollectionMaxAttempts());
    }

    @Test
    public void testNonNegativeIntegers() {
        Property[] properties = new Property[] {
                ChunkedSegmentStorageConfig.MAX_INDEXED_SEGMENTS,
                ChunkedSegmentStorageConfig.MAX_INDEXED_CHUNKS_PER_SEGMENTS,
                ChunkedSegmentStorageConfig.MAX_INDEXED_CHUNKS,
                ChunkedSegmentStorageConfig.SELF_CHECK_LATE_WARNING_THRESHOLD,
                ChunkedSegmentStorageConfig.GARBAGE_COLLECTION_MAX_CONCURRENCY,
                ChunkedSegmentStorageConfig.GARBAGE_COLLECTION_MAX_QUEUE_SIZE,
                ChunkedSegmentStorageConfig.GARBAGE_COLLECTION_MAX_ATTEMPS
        };
        for (Property p : properties) {
            assertThrows(p, "-1");
            assertThrows(p, Long.toString(1L + Integer.MAX_VALUE));
            assertNotThrows(p, Integer.toString(Integer.MAX_VALUE - 1));
            assertNotThrows(p, Integer.toString(0));
        }
    }

    @Test
    public void testPositiveIntegers() {
        Property[] properties = new Property[] {
                ChunkedSegmentStorageConfig.GARBAGE_COLLECTION_DELAY,
                ChunkedSegmentStorageConfig.GARBAGE_COLLECTION_SLEEP,
        };
        for (Property p : properties) {
            assertThrows(p, "-1");
            assertThrows(p, Long.toString(1L + Integer.MAX_VALUE));
            assertThrows(p, Integer.toString(0));
            assertNotThrows(p, Integer.toString(Integer.MAX_VALUE - 1));
            assertNotThrows(p, "1");
        }
    }

    @Test
    public void testNonNegativeLongs() {
        Property[] properties = new Property[] {
                ChunkedSegmentStorageConfig.MIN_SIZE_LIMIT_FOR_CONCAT,
                ChunkedSegmentStorageConfig.MAX_SIZE_LIMIT_FOR_CONCAT
        };
        for (Property p : properties) {
            assertThrows(p, "-1");
            assertThrows(p, Double.toString(1L + Long.MAX_VALUE));
            assertNotThrows(p, Long.toString(Long.MAX_VALUE - 1));
            assertNotThrows(p, Long.toString(0));
        }
    }

    @Test
    public void testPositiveLongs() {
        Property[] properties = new Property[] {
                ChunkedSegmentStorageConfig.DEFAULT_ROLLOVER_SIZE,
        };
        for (Property p : properties) {
            assertThrows(p, "-1");
            assertThrows(p, Double.toString(1L + Long.MAX_VALUE));
            assertThrows(p, Long.toString(0));
            assertNotThrows(p, Long.toString(Long.MAX_VALUE - 1));
            assertNotThrows(p, "1");
        }
    }

    public <T> void assertThrows(Property<T> property, String value) {
        Properties props = new Properties();
        val fullName = property.getFullName(ChunkedSegmentStorageConfig.COMPONENT_CODE);
        props.setProperty(fullName, value);
        TypedProperties typedProperties = new TypedProperties(props, "storage");
        AssertExtensions.assertThrows(String.format("Property %s should throw an exception", fullName),
                () -> new ChunkedSegmentStorageConfig(typedProperties),
                ex -> ex instanceof ConfigurationException);
    }

    public <T> void assertNotThrows(Property<T> property, String value) {
        Properties props = new Properties();
        val fullName = property.getFullName(ChunkedSegmentStorageConfig.COMPONENT_CODE);
        props.setProperty(fullName, value);
        TypedProperties typedProperties = new TypedProperties(props, "storage");
        new ChunkedSegmentStorageConfig(typedProperties);
    }
}
