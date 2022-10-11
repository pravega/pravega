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
package io.pravega.segmentstore.storage.chunklayer;

import io.pravega.common.util.ConfigurationException;
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
        props.setProperty(ChunkedSegmentStorageConfig.GARBAGE_COLLECTION_MAX_ATTEMPTS.getFullName(ChunkedSegmentStorageConfig.COMPONENT_CODE), "13");
        props.setProperty(ChunkedSegmentStorageConfig.READ_INDEX_BLOCK_SIZE.getFullName(ChunkedSegmentStorageConfig.COMPONENT_CODE), "14");
        props.setProperty(ChunkedSegmentStorageConfig.MAX_METADATA_ENTRIES_IN_BUFFER.getFullName(ChunkedSegmentStorageConfig.COMPONENT_CODE), "15");
        props.setProperty(ChunkedSegmentStorageConfig.MAX_METADATA_ENTRIES_IN_CACHE.getFullName(ChunkedSegmentStorageConfig.COMPONENT_CODE), "16");
        props.setProperty(ChunkedSegmentStorageConfig.GARBAGE_COLLECTION_MAX_TXN_BATCH_SIZE.getFullName(ChunkedSegmentStorageConfig.COMPONENT_CODE), "17");
        props.setProperty(ChunkedSegmentStorageConfig.MAX_SAFE_SIZE.getFullName(ChunkedSegmentStorageConfig.COMPONENT_CODE), "18");
        props.setProperty(ChunkedSegmentStorageConfig.ENABLE_SAFE_SIZE_CHECK.getFullName(ChunkedSegmentStorageConfig.COMPONENT_CODE), "false");
        props.setProperty(ChunkedSegmentStorageConfig.SAFE_SIZE_CHECK_FREQUENCY.getFullName(ChunkedSegmentStorageConfig.COMPONENT_CODE), "19");
        props.setProperty(ChunkedSegmentStorageConfig.RELOCATE_ON_TRUNCATE_ENABLED.getFullName(ChunkedSegmentStorageConfig.COMPONENT_CODE), "false");
        props.setProperty(ChunkedSegmentStorageConfig.MIN_TRUNCATE_RELOCATION_SIZE_BYTES.getFullName(ChunkedSegmentStorageConfig.COMPONENT_CODE), "20");
        props.setProperty(ChunkedSegmentStorageConfig.MIN_TRUNCATE_RELOCATION_PERCENT.getFullName(ChunkedSegmentStorageConfig.COMPONENT_CODE), "21");
        props.setProperty(ChunkedSegmentStorageConfig.MAX_TRUNCATE_RELOCATION_SIZE_BYTES.getFullName(ChunkedSegmentStorageConfig.COMPONENT_CODE), "22");
        props.setProperty(ChunkedSegmentStorageConfig.SELF_CHECK_DATA_INTEGRITY.getFullName(ChunkedSegmentStorageConfig.COMPONENT_CODE), "true");
        props.setProperty(ChunkedSegmentStorageConfig.SELF_CHECK_METADATA_INTEGRITY.getFullName(ChunkedSegmentStorageConfig.COMPONENT_CODE), "true");

        TypedProperties typedProperties = new TypedProperties(props, "storage");
        ChunkedSegmentStorageConfig config = new ChunkedSegmentStorageConfig(typedProperties);
        Assert.assertFalse(config.isAppendEnabled());
        Assert.assertFalse(config.isInlineDefragEnabled());
        Assert.assertEquals(config.getMaxBufferSizeForChunkDataTransfer(), 1);
        Assert.assertEquals(config.getMaxSizeLimitForConcat(), 2);
        Assert.assertEquals(config.getMinSizeLimitForConcat(), 0); // Don't use appends for concat when appends are disabled.
        Assert.assertEquals(config.getMaxIndexedSegments(), 4);
        Assert.assertEquals(config.getMaxIndexedChunks(), 5);
        Assert.assertEquals(config.getMaxIndexedChunksPerSegment(), 6);
        Assert.assertEquals(config.getStorageMetadataRollingPolicy().getMaxLength(), 7);
        Assert.assertEquals(config.getLateWarningThresholdInMillis(), 8);
        Assert.assertEquals(config.getGarbageCollectionDelay().toSeconds(), 9);
        Assert.assertEquals(config.getGarbageCollectionMaxQueueSize(), 10);
        Assert.assertEquals(config.getGarbageCollectionMaxConcurrency(), 11);
        Assert.assertEquals(config.getGarbageCollectionSleep().toMillis(), 12);
        Assert.assertEquals(config.getGarbageCollectionMaxAttempts(), 13);
        Assert.assertEquals(config.getIndexBlockSize(), 14);
        Assert.assertEquals(config.getMaxEntriesInTxnBuffer(), 15);
        Assert.assertEquals(config.getMaxEntriesInCache(), 16);
        Assert.assertEquals(config.getGarbageCollectionTransactionBatchSize(), 17);
        Assert.assertEquals(config.getMaxSafeStorageSize(), 18);
        Assert.assertFalse(config.isSafeStorageSizeCheckEnabled());
        Assert.assertEquals(config.getSafeStorageSizeCheckFrequencyInSeconds(), 19);
        Assert.assertFalse(config.isRelocateOnTruncateEnabled());
        Assert.assertEquals(config.getMinSizeForTruncateRelocationInbytes(), 20);
        Assert.assertEquals(config.getMinPercentForTruncateRelocation(), 21);
        Assert.assertEquals(config.getMaxSizeForTruncateRelocationInbytes(), 22);
        Assert.assertEquals(config.isSelfCheckForDataEnabled(), true);
        Assert.assertEquals(config.isSelfCheckForMetadataEnabled(), true);
    }

    @Test
    public void testDefaultValues() {
        Properties props = new Properties();

        TypedProperties typedProperties = new TypedProperties(props, "storage");
        ChunkedSegmentStorageConfig config = new ChunkedSegmentStorageConfig(typedProperties);
        testDefaultValues(config);
    }

    private void testDefaultValues(ChunkedSegmentStorageConfig config) {
        Assert.assertEquals(config.isAppendEnabled(), ChunkedSegmentStorageConfig.DEFAULT_CONFIG.isAppendEnabled());
        Assert.assertEquals(config.isInlineDefragEnabled(), ChunkedSegmentStorageConfig.DEFAULT_CONFIG.isInlineDefragEnabled());
        Assert.assertEquals(config.getMaxBufferSizeForChunkDataTransfer(), ChunkedSegmentStorageConfig.DEFAULT_CONFIG.getMaxBufferSizeForChunkDataTransfer());
        Assert.assertEquals(config.getMaxSizeLimitForConcat(), ChunkedSegmentStorageConfig.DEFAULT_CONFIG.getMaxSizeLimitForConcat());
        Assert.assertEquals(config.getMinSizeLimitForConcat(), ChunkedSegmentStorageConfig.DEFAULT_CONFIG.getMinSizeLimitForConcat());
        Assert.assertEquals(config.getMaxIndexedSegments(), ChunkedSegmentStorageConfig.DEFAULT_CONFIG.getMaxIndexedSegments());
        Assert.assertEquals(config.getMaxIndexedChunks(), ChunkedSegmentStorageConfig.DEFAULT_CONFIG.getMaxIndexedChunks());
        Assert.assertEquals(config.getMaxIndexedChunksPerSegment(), ChunkedSegmentStorageConfig.DEFAULT_CONFIG.getMaxIndexedChunksPerSegment());
        Assert.assertEquals(config.getStorageMetadataRollingPolicy().getMaxLength(), ChunkedSegmentStorageConfig.DEFAULT_CONFIG.getStorageMetadataRollingPolicy().getMaxLength());
        Assert.assertEquals(config.getLateWarningThresholdInMillis(), ChunkedSegmentStorageConfig.DEFAULT_CONFIG.getLateWarningThresholdInMillis());
        Assert.assertEquals(config.getGarbageCollectionDelay(), ChunkedSegmentStorageConfig.DEFAULT_CONFIG.getGarbageCollectionDelay());
        Assert.assertEquals(config.getGarbageCollectionMaxConcurrency(), ChunkedSegmentStorageConfig.DEFAULT_CONFIG.getGarbageCollectionMaxConcurrency());
        Assert.assertEquals(config.getGarbageCollectionMaxQueueSize(), ChunkedSegmentStorageConfig.DEFAULT_CONFIG.getGarbageCollectionMaxQueueSize());
        Assert.assertEquals(config.getGarbageCollectionSleep(), ChunkedSegmentStorageConfig.DEFAULT_CONFIG.getGarbageCollectionSleep());
        Assert.assertEquals(config.getGarbageCollectionMaxAttempts(), ChunkedSegmentStorageConfig.DEFAULT_CONFIG.getGarbageCollectionMaxAttempts());
        Assert.assertEquals(config.getIndexBlockSize(), ChunkedSegmentStorageConfig.DEFAULT_CONFIG.getIndexBlockSize());
        Assert.assertEquals(config.getMaxEntriesInTxnBuffer(), ChunkedSegmentStorageConfig.DEFAULT_CONFIG.getMaxEntriesInTxnBuffer());
        Assert.assertEquals(config.getMaxEntriesInCache(), ChunkedSegmentStorageConfig.DEFAULT_CONFIG.getMaxEntriesInCache());
        Assert.assertEquals(config.getGarbageCollectionTransactionBatchSize(), ChunkedSegmentStorageConfig.DEFAULT_CONFIG.getGarbageCollectionTransactionBatchSize());
        Assert.assertEquals(config.getMaxSafeStorageSize(), ChunkedSegmentStorageConfig.DEFAULT_CONFIG.getMaxSafeStorageSize());
        Assert.assertEquals(config.isSafeStorageSizeCheckEnabled(), ChunkedSegmentStorageConfig.DEFAULT_CONFIG.isSafeStorageSizeCheckEnabled());
        Assert.assertEquals(config.getSafeStorageSizeCheckFrequencyInSeconds(), ChunkedSegmentStorageConfig.DEFAULT_CONFIG.getSafeStorageSizeCheckFrequencyInSeconds());
        Assert.assertEquals(config.isRelocateOnTruncateEnabled(), ChunkedSegmentStorageConfig.DEFAULT_CONFIG.isRelocateOnTruncateEnabled());
        Assert.assertEquals(config.getMinSizeForTruncateRelocationInbytes(), ChunkedSegmentStorageConfig.DEFAULT_CONFIG.getMinSizeForTruncateRelocationInbytes());
        Assert.assertEquals(config.getMaxSizeForTruncateRelocationInbytes(), ChunkedSegmentStorageConfig.DEFAULT_CONFIG.getMaxSizeForTruncateRelocationInbytes());
        Assert.assertEquals(config.getMinPercentForTruncateRelocation(), ChunkedSegmentStorageConfig.DEFAULT_CONFIG.getMinPercentForTruncateRelocation());
        Assert.assertEquals(config.isSelfCheckForDataEnabled(), ChunkedSegmentStorageConfig.DEFAULT_CONFIG.isSelfCheckForDataEnabled());
        Assert.assertEquals(config.isSelfCheckForMetadataEnabled(), ChunkedSegmentStorageConfig.DEFAULT_CONFIG.isSelfCheckForMetadataEnabled());
    }

    @Test
    public void testDefaultBuildValues() {
        testDefaultValues(ChunkedSegmentStorageConfig.builder().build());
    }

    @Test
    public void testInvalidValues() {
        testGetPositiveValue(ChunkedSegmentStorageConfig.MAX_SAFE_SIZE.getFullName(ChunkedSegmentStorageConfig.COMPONENT_CODE));

        testGetNonNegativeValue(ChunkedSegmentStorageConfig.MAX_INDEXED_SEGMENTS.getFullName(ChunkedSegmentStorageConfig.COMPONENT_CODE));
        testGetPositiveValue(ChunkedSegmentStorageConfig.MAX_INDEXED_CHUNKS_PER_SEGMENTS.getFullName(ChunkedSegmentStorageConfig.COMPONENT_CODE));
        testGetPositiveValue(ChunkedSegmentStorageConfig.MAX_INDEXED_CHUNKS.getFullName(ChunkedSegmentStorageConfig.COMPONENT_CODE));
        testGetPositiveValue(ChunkedSegmentStorageConfig.GARBAGE_COLLECTION_DELAY.getFullName(ChunkedSegmentStorageConfig.COMPONENT_CODE));
        testGetPositiveValue(ChunkedSegmentStorageConfig.GARBAGE_COLLECTION_MAX_QUEUE_SIZE.getFullName(ChunkedSegmentStorageConfig.COMPONENT_CODE));
        testGetPositiveValue(ChunkedSegmentStorageConfig.GARBAGE_COLLECTION_SLEEP.getFullName(ChunkedSegmentStorageConfig.COMPONENT_CODE));
        testGetPositiveValue(ChunkedSegmentStorageConfig.GARBAGE_COLLECTION_MAX_ATTEMPTS.getFullName(ChunkedSegmentStorageConfig.COMPONENT_CODE));
        testGetPositiveValue(ChunkedSegmentStorageConfig.GARBAGE_COLLECTION_MAX_TXN_BATCH_SIZE.getFullName(ChunkedSegmentStorageConfig.COMPONENT_CODE));
        testGetPositiveValue(ChunkedSegmentStorageConfig.JOURNAL_SNAPSHOT_UPDATE_FREQUENCY.getFullName(ChunkedSegmentStorageConfig.COMPONENT_CODE));
        testGetPositiveValue(ChunkedSegmentStorageConfig.MAX_PER_SNAPSHOT_UPDATE_COUNT.getFullName(ChunkedSegmentStorageConfig.COMPONENT_CODE));
        testGetPositiveValue(ChunkedSegmentStorageConfig.MAX_JOURNAL_READ_ATTEMPTS.getFullName(ChunkedSegmentStorageConfig.COMPONENT_CODE));
        testGetPositiveValue(ChunkedSegmentStorageConfig.MAX_METADATA_ENTRIES_IN_BUFFER.getFullName(ChunkedSegmentStorageConfig.COMPONENT_CODE));
        testGetPositiveValue(ChunkedSegmentStorageConfig.READ_INDEX_BLOCK_SIZE.getFullName(ChunkedSegmentStorageConfig.COMPONENT_CODE));
        testGetPositiveValue(ChunkedSegmentStorageConfig.MAX_METADATA_ENTRIES_IN_CACHE.getFullName(ChunkedSegmentStorageConfig.COMPONENT_CODE));
        testGetPositiveValue(ChunkedSegmentStorageConfig.MAX_SAFE_SIZE.getFullName(ChunkedSegmentStorageConfig.COMPONENT_CODE));
        testGetPositiveValue(ChunkedSegmentStorageConfig.SAFE_SIZE_CHECK_FREQUENCY.getFullName(ChunkedSegmentStorageConfig.COMPONENT_CODE));
        testGetPositiveValue(ChunkedSegmentStorageConfig.MIN_TRUNCATE_RELOCATION_SIZE_BYTES.getFullName(ChunkedSegmentStorageConfig.COMPONENT_CODE));
        testGetPositiveValue(ChunkedSegmentStorageConfig.MIN_TRUNCATE_RELOCATION_PERCENT.getFullName(ChunkedSegmentStorageConfig.COMPONENT_CODE));
        testGetPositiveValue(ChunkedSegmentStorageConfig.SELF_CHECK_LATE_WARNING_THRESHOLD.getFullName(ChunkedSegmentStorageConfig.COMPONENT_CODE));
    }

    /**
     * Test for non negative value
     * Valid values : 0, 1
     * Invalid values : -1
     * @param name  Tha name of the property
     */
    private void testGetNonNegativeValue(String name) {
        testValueThrowsException(name, "-1");
        testNonFailingValue(name, "0");
        testNonFailingValue(name, "1");
    }

    /**
     * Test for positive value
     * Valid value : 1
     * Invalid values : 0, -1
     * @param name The name of the property
     */
    private void testGetPositiveValue(String name) {
        testValueThrowsException(name, "0");
        testValueThrowsException(name, "-1");
        testNonFailingValue(name, "1");
    }

    /**
     * Testing for which particular value does the test throw exception
     * @param name The name of the property
     * @param value The value associated with the property
     */
    private void testValueThrowsException(String name, String value) {
        Properties props = new Properties();
        props.setProperty(name, value);

        TypedProperties typedProperties = new TypedProperties(props, "storage");
        AssertExtensions.assertThrows(
                " read should throw exception.",
                () -> new ChunkedSegmentStorageConfig(typedProperties),
                ex -> ex instanceof ConfigurationException);
    }

    /**
     * Testing if the value of the property passes the test for particular values without any failure
     * @param name The name of the property
     * @param value The value associated with the property
     */
    private void testNonFailingValue(String name, String value) {
        Properties props = new Properties();
        props.setProperty(name, value);

        TypedProperties typedProperties = new TypedProperties(props, "storage");
        val config = new ChunkedSegmentStorageConfig(typedProperties);
    }
}
