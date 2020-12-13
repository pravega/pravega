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

import io.pravega.common.util.TypedProperties;
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
    }
}
