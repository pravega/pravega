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

import io.pravega.common.util.ConfigBuilder;
import io.pravega.common.util.ConfigurationException;
import io.pravega.common.util.Property;
import io.pravega.common.util.TypedProperties;
import io.pravega.segmentstore.storage.SegmentRollingPolicy;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;

import java.time.Duration;

/**
 * Configuration for {@link ChunkedSegmentStorage}.
 */
@AllArgsConstructor
@Builder(toBuilder = true, builderMethodName = "instanceBuilder")
public class ChunkedSegmentStorageConfig {
    public static final Property<Long> MIN_SIZE_LIMIT_FOR_CONCAT = Property.named("concat.size.bytes.min", 0L);
    public static final Property<Long> MAX_SIZE_LIMIT_FOR_CONCAT = Property.named("concat.size.bytes.max", Long.MAX_VALUE);
    public static final Property<Integer> MAX_BUFFER_SIZE_FOR_APPENDS = Property.named("appends.buffer.size.bytes.max", 1024 * 1024);

    public static final Property<Integer> MAX_INDEXED_SEGMENTS = Property.named("readindex.segments.max", 1024);
    public static final Property<Integer> MAX_INDEXED_CHUNKS_PER_SEGMENTS = Property.named("readindex.chunksPerSegment.max", 1024);
    public static final Property<Integer> MAX_INDEXED_CHUNKS = Property.named("readindex.chunks.max", 16 * 1024);
    public static final Property<Long> READ_INDEX_BLOCK_SIZE = Property.named("readindex.block.size", 1024 * 1024 * 1024L);

    public static final Property<Boolean> APPENDS_ENABLED = Property.named("appends.enable", true);
    public static final Property<Boolean> INLINE_DEFRAG_ENABLED = Property.named("defrag.inline.enable", true);

    public static final Property<Long> DEFAULT_ROLLOVER_SIZE = Property.named("metadata.rollover.size.bytes.max", 128 * 1024 * 1024L);

    public static final Property<Integer> GARBAGE_COLLECTION_DELAY = Property.named("garbage.collection.delay.seconds", 60);
    public static final Property<Integer> GARBAGE_COLLECTION_MAX_CONCURRENCY = Property.named("garbage.collection.concurrency.max", 10);
    public static final Property<Integer> GARBAGE_COLLECTION_MAX_QUEUE_SIZE = Property.named("garbage.collection.queue.size.max", 16 * 1024);
    public static final Property<Integer> GARBAGE_COLLECTION_SLEEP = Property.named("garbage.collection.sleep.millis", 10);
    public static final Property<Integer> GARBAGE_COLLECTION_MAX_ATTEMPTS = Property.named("garbage.collection.attempts.max", 3);
    public static final Property<Integer> GARBAGE_COLLECTION_MAX_TXN_BATCH_SIZE = Property.named("garbage.collection.txn.batch.size.max", 5000);

    public static final Property<Integer> MAX_METADATA_ENTRIES_IN_BUFFER = Property.named("metadata.buffer.size.max", 1024);
    public static final Property<Integer> MAX_METADATA_ENTRIES_IN_CACHE = Property.named("metadata.cache.size.max", 5000);

    public static final Property<Integer> JOURNAL_SNAPSHOT_UPDATE_FREQUENCY = Property.named("journal.snapshot.update.frequency.minutes", 5);
    public static final Property<Integer> MAX_PER_SNAPSHOT_UPDATE_COUNT = Property.named("journal.snapshot.update.count.max", 100);
    public static final Property<Integer> MAX_JOURNAL_READ_ATTEMPTS = Property.named("journal.snapshot.attempts.read.max", 100);
    public static final Property<Integer> MAX_JOURNAL_WRITE_ATTEMPTS = Property.named("journal.snapshot.attempts.write.max", 10);

    public static final Property<Boolean> SELF_CHECK_ENABLED = Property.named("self.check.enable", false);
    public static final Property<Integer> SELF_CHECK_LATE_WARNING_THRESHOLD = Property.named("self.check.late", 100);
    public static final Property<Boolean> SELF_CHECK_DATA_INTEGRITY = Property.named("self.check.integrity.data", false);
    public static final Property<Boolean> SELF_CHECK_METADATA_INTEGRITY = Property.named("self.check.integrity.metadata", false);
    public static final Property<Boolean> SELF_CHECK_SNAPSHOT_INTEGRITY = Property.named("self.check.integrity.snapshot", false);

    public static final Property<Long> MAX_SAFE_SIZE = Property.named("safe.size.bytes.max", Long.MAX_VALUE);
    public static final Property<Boolean> ENABLE_SAFE_SIZE_CHECK = Property.named("safe.size.check.enable", true);
    public static final Property<Integer> SAFE_SIZE_CHECK_FREQUENCY = Property.named("safe.size.check.frequency.seconds", 60);

    public static final Property<Boolean> RELOCATE_ON_TRUNCATE_ENABLED = Property.named("truncate.relocate.enable", true);
    public static final Property<Long> MIN_TRUNCATE_RELOCATION_SIZE_BYTES = Property.named("truncate.relocate.size.bytes.min", 64 * 1024 * 1024L);
    public static final Property<Long> MAX_TRUNCATE_RELOCATION_SIZE_BYTES = Property.named("truncate.relocate.size.bytes.max", 1 * 1024 * 1024 * 1024L);

    public static final Property<Integer> MIN_TRUNCATE_RELOCATION_PERCENT = Property.named("truncate.relocate.percent.min", 80);

    /**
     * Default configuration for {@link ChunkedSegmentStorage}.
     */
    public static final ChunkedSegmentStorageConfig DEFAULT_CONFIG = ChunkedSegmentStorageConfig.instanceBuilder()
            .minSizeLimitForConcat(0L)
            .maxSizeLimitForConcat(Long.MAX_VALUE)
            .storageMetadataRollingPolicy(new SegmentRollingPolicy(128 * 1024 * 1024L))
            .maxBufferSizeForChunkDataTransfer(1024 * 1024)
            .maxIndexedSegments(1024)
            .maxIndexedChunksPerSegment(1024)
            .maxIndexedChunks(16 * 1024)
            .appendEnabled(true)
            .inlineDefragEnabled(true)
            .lateWarningThresholdInMillis(100)
            .garbageCollectionDelay(Duration.ofSeconds(60))
            .garbageCollectionMaxConcurrency(10)
            .garbageCollectionMaxQueueSize(16 * 1024)
            .garbageCollectionSleep(Duration.ofMillis(10))
            .garbageCollectionMaxAttempts(3)
            .garbageCollectionTransactionBatchSize(5000)
            .indexBlockSize(1024 * 1024 * 1024)
            .maxEntriesInCache(5000)
            .maxEntriesInTxnBuffer(1024)
            .journalSnapshotInfoUpdateFrequency(Duration.ofMinutes(5))
            .maxJournalUpdatesPerSnapshot(100)
            .maxJournalReadAttempts(100)
            .maxJournalWriteAttempts(10)
            .selfCheckEnabled(false)
            .selfCheckForDataEnabled(false)
            .selfCheckForMetadataEnabled(false)
            .maxSafeStorageSize(Long.MAX_VALUE)
            .safeStorageSizeCheckEnabled(true)
            .safeStorageSizeCheckFrequencyInSeconds(60)
            .relocateOnTruncateEnabled(true)
            .minSizeForTruncateRelocationInbytes(64 * 1024 * 1024L)
            .maxSizeForTruncateRelocationInbytes(1 * 1024 * 1024 * 1024L)
            .minPercentForTruncateRelocation(80)
            .build();

    static final String COMPONENT_CODE = "storage";

    /**
     * Size of chunk in bytes above which it is no longer considered a small object.
     * For small source objects, concat is not used and instead.
     */
    @Getter
    final private long minSizeLimitForConcat;

    /**
     * Size of chunk in bytes above which it is no longer considered for concat.
     */
    @Getter
    final private long maxSizeLimitForConcat;

    /**
     * A SegmentRollingPolicy to apply to storage metadata segments.
     */
    @Getter
    @NonNull
    final private SegmentRollingPolicy storageMetadataRollingPolicy;

    /**
     * Maximum size for the buffer used while copying of data from one chunk to other.
     */
    @Getter
    final private int maxBufferSizeForChunkDataTransfer;

    /**
     * Max number of indexed segments to keep in read cache.
     */
    @Getter
    final private int maxIndexedSegments;

    /**
     * Max number of indexed chunks to keep per segment in read cache.
     */
    @Getter
    final private int maxIndexedChunksPerSegment;

    /**
     * Max number of indexed chunks to keep in cache.
     */
    @Getter
    final private int maxIndexedChunks;

    /**
     * The fixed block size used for creating block index entries.
     */
    @Getter
    final private long indexBlockSize;

    /**
     * Whether the append functionality is enabled or disabled.
     */
    @Getter
    final private boolean appendEnabled;

    /**
     * Whether the inline defrag functionality is enabled or disabled.
     */
    @Getter
    final private boolean inlineDefragEnabled;

    /**
     * Whether the relocation on truncate functionality is enabled or disabled.
     */
    @Getter
    final private boolean relocateOnTruncateEnabled;

    /**
     * Minimum size of chunk required for it to be eligible for relocation.
     */
    @Getter
    final private long minSizeForTruncateRelocationInbytes;

    /**
     * Maximum size of chunk after which it is not eligible for relocation.
     */
    @Getter
    final private long maxSizeForTruncateRelocationInbytes;

    /**
     * Minimum percentage of wasted space required for it to be eligible for relocation.
     */
    @Getter
    final private int minPercentForTruncateRelocation;

    @Getter
    final private int lateWarningThresholdInMillis;

    /**
     * Minimum delay in seconds between when garbage chunks are marked for deletion and actually deleted.
     */
    @Getter
    final private Duration garbageCollectionDelay;

    /**
     * Number of chunks deleted concurrently.
     * This number should be small enough so that it does interfere foreground requests.
     */
    @Getter
    final private int garbageCollectionMaxConcurrency;

    /**
     * Max size of garbage collection queue.
     */
    @Getter
    final private int garbageCollectionMaxQueueSize;

    /**
     * Duration for which garbage collector sleeps if there is no work.
     */
    @Getter
    final private Duration garbageCollectionSleep;


    /**
     * Max number of attempts per chunk for garbage collection.
     */
    @Getter
    final private int garbageCollectionMaxAttempts;

    /**
     * Max number of metadata entries to update in a single transaction during garbage collection.
     */
    @Getter
    final private int garbageCollectionTransactionBatchSize;

    /**
     * Maximum number of metadata entries to keep in recent transaction buffer.
     */
    @Getter
    final private int maxEntriesInTxnBuffer;

    /**
     * Maximum number of metadata entries to keep in recent transaction buffer.
     */
    @Getter
    final private int maxEntriesInCache;

    /**
     * Duration between two system journal snapshot.
     */
    @Getter
    final private Duration journalSnapshotInfoUpdateFrequency;

    /**
     * Number of journal writes since last snapshot after which new snapshot is taken.
     */
    @Getter
    final private int maxJournalUpdatesPerSnapshot;

    /**
     * Max number of times snapshot read is retried.
     */
    @Getter
    final private int maxJournalReadAttempts;

    /**
     * Max number of times snapshot write is retried.
     */
    @Getter
    final private int maxJournalWriteAttempts;

    /**
     * When enabled, SLTS will perform extra validation.
     */
    @Getter
    final private boolean selfCheckEnabled;


    /**
     * When enabled, SLTS will perform extra validation for data.
     */
    @Getter
    final private boolean selfCheckForDataEnabled;

    /**
     * When enabled, SLTS will perform extra validation for metadata.
     */
    @Getter
    final private boolean selfCheckForMetadataEnabled;

    /**
     * When enabled, SLTS will perform extra validation for snapshot.
     */
    @Getter
    final private boolean selfCheckForSnapshotEnabled;

    /**
     * Maximum storage size in bytes below which operations are considered safe.
     * Above this value any non-critical writes are not allowed.
     */
    @Getter
    final private long maxSafeStorageSize;

    /**
     * When enabled, SLTS will periodically check storage usage stats.
     */
    @Getter
    final private boolean safeStorageSizeCheckEnabled;

    /**
     * Frequency in seconds of how often storage size checks is performed.
     */
    @Getter
    final private int safeStorageSizeCheckFrequencyInSeconds;

    /**
     * Creates a new instance of the ChunkedSegmentStorageConfig class.
     *
     * @param properties The TypedProperties object to read Properties from.
     */
    ChunkedSegmentStorageConfig(TypedProperties properties) throws ConfigurationException {
        this.appendEnabled = properties.getBoolean(APPENDS_ENABLED);
        this.inlineDefragEnabled = properties.getBoolean(INLINE_DEFRAG_ENABLED);
        this.maxBufferSizeForChunkDataTransfer = properties.getPositiveInt(MAX_BUFFER_SIZE_FOR_APPENDS);
        // Don't use appends for concat when appends are disabled.
        this.minSizeLimitForConcat = this.appendEnabled ? properties.getLong(MIN_SIZE_LIMIT_FOR_CONCAT) : 0;
        this.maxSizeLimitForConcat = properties.getPositiveLong(MAX_SIZE_LIMIT_FOR_CONCAT);
        this.maxIndexedSegments = properties.getNonNegativeInt(MAX_INDEXED_SEGMENTS);
        this.maxIndexedChunksPerSegment = properties.getPositiveInt(MAX_INDEXED_CHUNKS_PER_SEGMENTS);
        this.maxIndexedChunks = properties.getPositiveInt(MAX_INDEXED_CHUNKS);
        this.storageMetadataRollingPolicy = new SegmentRollingPolicy(properties.getLong(DEFAULT_ROLLOVER_SIZE));
        this.lateWarningThresholdInMillis = properties.getPositiveInt(SELF_CHECK_LATE_WARNING_THRESHOLD);
        this.garbageCollectionDelay = Duration.ofSeconds(properties.getPositiveInt(GARBAGE_COLLECTION_DELAY));
        this.garbageCollectionMaxConcurrency = properties.getPositiveInt(GARBAGE_COLLECTION_MAX_CONCURRENCY);
        this.garbageCollectionMaxQueueSize = properties.getPositiveInt(GARBAGE_COLLECTION_MAX_QUEUE_SIZE);
        this.garbageCollectionSleep = Duration.ofMillis(properties.getPositiveInt(GARBAGE_COLLECTION_SLEEP));
        this.garbageCollectionMaxAttempts = properties.getPositiveInt(GARBAGE_COLLECTION_MAX_ATTEMPTS);
        this.garbageCollectionTransactionBatchSize = properties.getPositiveInt(GARBAGE_COLLECTION_MAX_TXN_BATCH_SIZE);
        this.journalSnapshotInfoUpdateFrequency = Duration.ofMinutes(properties.getPositiveInt(JOURNAL_SNAPSHOT_UPDATE_FREQUENCY));
        this.maxJournalUpdatesPerSnapshot =  properties.getPositiveInt(MAX_PER_SNAPSHOT_UPDATE_COUNT);
        this.maxJournalReadAttempts = properties.getPositiveInt(MAX_JOURNAL_READ_ATTEMPTS);
        this.maxJournalWriteAttempts = properties.getPositiveInt(MAX_JOURNAL_WRITE_ATTEMPTS);
        this.selfCheckEnabled = properties.getBoolean(SELF_CHECK_ENABLED);
        this.selfCheckForDataEnabled = properties.getBoolean(SELF_CHECK_DATA_INTEGRITY);
        this.selfCheckForMetadataEnabled = properties.getBoolean(SELF_CHECK_METADATA_INTEGRITY);
        this.selfCheckForSnapshotEnabled = properties.getBoolean(SELF_CHECK_SNAPSHOT_INTEGRITY);
        this.indexBlockSize = properties.getPositiveLong(READ_INDEX_BLOCK_SIZE);
        this.maxEntriesInTxnBuffer = properties.getPositiveInt(MAX_METADATA_ENTRIES_IN_BUFFER);
        this.maxEntriesInCache = properties.getPositiveInt(MAX_METADATA_ENTRIES_IN_CACHE);
        this.maxSafeStorageSize = properties.getPositiveLong(MAX_SAFE_SIZE);
        this.safeStorageSizeCheckEnabled = properties.getBoolean(ENABLE_SAFE_SIZE_CHECK);
        this.safeStorageSizeCheckFrequencyInSeconds = properties.getPositiveInt(SAFE_SIZE_CHECK_FREQUENCY);
        this.relocateOnTruncateEnabled = properties.getBoolean(RELOCATE_ON_TRUNCATE_ENABLED);
        this.minSizeForTruncateRelocationInbytes = properties.getPositiveLong(MIN_TRUNCATE_RELOCATION_SIZE_BYTES);
        this.maxSizeForTruncateRelocationInbytes = properties.getPositiveLong(MAX_TRUNCATE_RELOCATION_SIZE_BYTES);
        this.minPercentForTruncateRelocation = properties.getPositiveInt(MIN_TRUNCATE_RELOCATION_PERCENT);
    }

    /**
     * Creates a new ConfigBuilder that can be used to create instances of this class.
     *
     * @return A new Builder for this class.
     */
    public static ConfigBuilder<ChunkedSegmentStorageConfig> builder() {
        return new ConfigBuilder<>(COMPONENT_CODE, ChunkedSegmentStorageConfig::new);
    }
}
