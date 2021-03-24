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
    public static final Property<Long> READ_INDEX_BLOCK_SIZE = Property.named("readindex.block.size", 1024 * 1024L);
    public static final Property<Boolean> APPENDS_ENABLED = Property.named("appends.enable", true);
    public static final Property<Boolean> LAZY_COMMIT_ENABLED = Property.named("commit.lazy.enable", true);
    public static final Property<Boolean> INLINE_DEFRAG_ENABLED = Property.named("defrag.inline.enable", true);
    public static final Property<Long> DEFAULT_ROLLOVER_SIZE = Property.named("metadata.rollover.size.bytes.max", SegmentRollingPolicy.MAX_CHUNK_LENGTH);
    public static final Property<Integer> SELF_CHECK_LATE_WARNING_THRESHOLD = Property.named("self.check.late", 100);
    public static final Property<Integer> GARBAGE_COLLECTION_DELAY = Property.named("garbage.collection.delay.seconds", 60);
    public static final Property<Integer> GARBAGE_COLLECTION_MAX_CONCURRENCY = Property.named("garbage.collection.concurrency.max", 10);
    public static final Property<Integer> GARBAGE_COLLECTION_MAX_QUEUE_SIZE = Property.named("garbage.collection.queue.size.max", 16 * 1024);
    public static final Property<Integer> GARBAGE_COLLECTION_SLEEP = Property.named("garbage.collection.sleep.millis", 10);
    public static final Property<Integer> GARBAGE_COLLECTION_MAX_ATTEMPTS = Property.named("garbage.collection.attempts.max", 3);


    /**
     * Default configuration for {@link ChunkedSegmentStorage}.
     */
    public static final ChunkedSegmentStorageConfig DEFAULT_CONFIG = ChunkedSegmentStorageConfig.instanceBuilder()
            .minSizeLimitForConcat(0L)
            .maxSizeLimitForConcat(Long.MAX_VALUE)
            .defaultRollingPolicy(SegmentRollingPolicy.NO_ROLLING)
            .maxBufferSizeForChunkDataTransfer(1024 * 1024)
            .maxIndexedSegments(1024)
            .maxIndexedChunksPerSegment(1024)
            .maxIndexedChunks(16 * 1024)
            .appendEnabled(true)
            .lazyCommitEnabled(true)
            .inlineDefragEnabled(true)
            .lateWarningThresholdInMillis(100)
            .garbageCollectionDelay(Duration.ofSeconds(60))
            .garbageCollectionMaxConcurrency(10)
            .garbageCollectionMaxQueueSize(16 * 1024)
            .garbageCollectionSleep(Duration.ofMillis(10))
            .garbageCollectionMaxAttempts(3)
            .indexBlockSize(1024 * 1024)
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
     * A SegmentRollingPolicy to apply to every StreamSegment that does not have its own policy defined.
     */
    @Getter
    @NonNull
    final private SegmentRollingPolicy defaultRollingPolicy;

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
     * Whether the lazy commit functionality is enabled or disabled.
     * Underlying implementation might buffer frequently or recently updated metadata keys to optimize read/write performance.
     * To further optimize it may provide "lazy committing" of changes where there is application specific way to recover from failures.(Eg. when only length of chunk is changed.)
     * Note that otherwise for each commit the data is written to underlying key-value store.
     */
    @Getter
    final private boolean lazyCommitEnabled;

    /**
     * Whether the inline defrag functionality is enabled or disabled.
     */
    @Getter
    final private boolean inlineDefragEnabled;

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
     * Creates a new instance of the ChunkedSegmentStorageConfig class.
     *
     * @param properties The TypedProperties object to read Properties from.
     */
    ChunkedSegmentStorageConfig(TypedProperties properties) throws ConfigurationException {
        this.appendEnabled = properties.getBoolean(APPENDS_ENABLED);
        this.lazyCommitEnabled = properties.getBoolean(LAZY_COMMIT_ENABLED);
        this.inlineDefragEnabled = properties.getBoolean(INLINE_DEFRAG_ENABLED);
        this.maxBufferSizeForChunkDataTransfer = properties.getInt(MAX_BUFFER_SIZE_FOR_APPENDS);
        // Don't use appends for concat when appends are disabled.
        this.minSizeLimitForConcat = this.appendEnabled ? properties.getLong(MIN_SIZE_LIMIT_FOR_CONCAT) : 0;
        this.maxSizeLimitForConcat = properties.getLong(MAX_SIZE_LIMIT_FOR_CONCAT);
        this.maxIndexedSegments = properties.getInt(MAX_INDEXED_SEGMENTS);
        this.maxIndexedChunksPerSegment = properties.getInt(MAX_INDEXED_CHUNKS_PER_SEGMENTS);
        this.maxIndexedChunks = properties.getInt(MAX_INDEXED_CHUNKS);
        long defaultMaxLength = properties.getLong(DEFAULT_ROLLOVER_SIZE);
        this.defaultRollingPolicy = new SegmentRollingPolicy(defaultMaxLength);
        this.lateWarningThresholdInMillis = properties.getInt(SELF_CHECK_LATE_WARNING_THRESHOLD);
        this.garbageCollectionDelay = Duration.ofSeconds(properties.getInt(GARBAGE_COLLECTION_DELAY));
        this.garbageCollectionMaxConcurrency = properties.getInt(GARBAGE_COLLECTION_MAX_CONCURRENCY);
        this.garbageCollectionMaxQueueSize = properties.getInt(GARBAGE_COLLECTION_MAX_QUEUE_SIZE);
        this.garbageCollectionSleep = Duration.ofMillis(properties.getInt(GARBAGE_COLLECTION_SLEEP));
        this.garbageCollectionMaxAttempts = properties.getInt(GARBAGE_COLLECTION_MAX_ATTEMPTS);
        this.indexBlockSize = properties.getLong(READ_INDEX_BLOCK_SIZE);
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
