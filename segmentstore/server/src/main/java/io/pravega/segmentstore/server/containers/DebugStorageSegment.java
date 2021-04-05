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

import com.google.common.util.concurrent.Runnables;
import io.pravega.common.TimeoutTimer;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.io.SerializationException;
import io.pravega.common.util.AsyncIterator;
import io.pravega.common.util.BufferView;
import io.pravega.segmentstore.contracts.AttributeUpdate;
import io.pravega.segmentstore.contracts.AttributeUpdateType;
import io.pravega.segmentstore.contracts.Attributes;
import io.pravega.segmentstore.contracts.ReadResult;
import io.pravega.segmentstore.contracts.SegmentProperties;
import io.pravega.segmentstore.contracts.tables.TableEntry;
import io.pravega.segmentstore.contracts.tables.TableKey;
import io.pravega.segmentstore.server.AttributeIndex;
import io.pravega.segmentstore.server.AttributeIterator;
import io.pravega.segmentstore.server.CacheManager;
import io.pravega.segmentstore.server.CachePolicy;
import io.pravega.segmentstore.server.DirectSegmentAccess;
import io.pravega.segmentstore.server.attributes.AttributeIndexConfig;
import io.pravega.segmentstore.server.attributes.ContainerAttributeIndex;
import io.pravega.segmentstore.server.attributes.ContainerAttributeIndexFactoryImpl;
import io.pravega.segmentstore.server.reading.StreamSegmentStorageReader;
import io.pravega.segmentstore.server.tables.AsyncTableEntryReader;
import io.pravega.segmentstore.server.tables.EntrySerializer;
import io.pravega.segmentstore.server.tables.IndexReader;
import io.pravega.segmentstore.server.tables.KeyHasher;
import io.pravega.segmentstore.server.tables.TableBucketReader;
import io.pravega.segmentstore.storage.Storage;
import io.pravega.segmentstore.storage.cache.NoOpCache;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import lombok.AccessLevel;
import lombok.Data;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.val;

/**
 * Provides direct access to a Segment (main segment or attribute segment) in LTS Storage. This class should be used
 * for debugging/diagnosing purposes only. NEVER USE IT FOR PRODUCTION PURPOSES (i.e., on the main path).
 */
public class DebugStorageSegment implements AutoCloseable {
    //region Members

    private static final EntrySerializer TABLE_SEGMENT_SERIALIZER = new EntrySerializer();
    private static final KeyHasher TABLE_SEGMENT_KEY_HASHER = KeyHasher.sha256();
    private static final int READ_BLOCK_SIZE = 1024 * 1024;
    private static final int CONTAINER_ID = 9999;
    private static final long SEGMENT_ID = 99999L; // Used for artificially mapping a segment to this ID.
    private static final Duration DEFAULT_TIMEOUT = Duration.ofSeconds(30);
    private final String segmentName;
    private final Storage storage;
    private final ScheduledExecutorService executor;
    private final CacheManager cacheManager;
    private final ContainerAttributeIndex containerAttributeIndex;
    private final StreamSegmentContainerMetadata containerMetadata;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the {@link DebugStorageSegment} class.
     * @param segmentName The name of the segment.
     * @param storage A {@link Storage} adapter implementation.
     * @param executor A {@link ScheduledExecutorService}.
     */
    public DebugStorageSegment(@NonNull String segmentName, @NonNull Storage storage, @NonNull ScheduledExecutorService executor) {
        this.segmentName = segmentName;
        this.storage = storage;
        this.executor = executor;

        this.containerMetadata = new StreamSegmentContainerMetadata(CONTAINER_ID, 1000);
        this.containerMetadata.mapStreamSegmentId(segmentName, SEGMENT_ID);
        this.cacheManager = new CacheManager(CachePolicy.INFINITE, new NoOpCache(), executor);
        val attributeIndexFactory = new ContainerAttributeIndexFactoryImpl(AttributeIndexConfig.builder().build(), this.cacheManager, executor);
        this.containerAttributeIndex = attributeIndexFactory.createContainerAttributeIndex(this.containerMetadata, this.storage);
    }

    //endregion

    //region AutoCloseable implementation

    @Override
    public void close() {
        this.containerAttributeIndex.close();
        this.cacheManager.close();
    }

    //endregion

    //region Segment Operations

    /**
     * Gets Segment Information for the Segment.
     * NOTE: this is applicable both for main segments and attribute index segments.
     *
     * @return A CompletableFuture which will contain a {@link SegmentProperties} representing the info about the segment.
     */
    public CompletableFuture<SegmentProperties> getSegmentInfo() {
        return this.storage.getStreamSegmentInfo(this.segmentName, DEFAULT_TIMEOUT);
    }

    /**
     * Reads a range of bytes from the Segment.
     * NOTE: this is applicable both for main segments and attribute index segments.
     *
     * @param offset    The offset to begin reading from.
     * @param maxLength The maximum number of bytes to read.
     * @return A CompletableFuture which will contain a {@link ReadResult} to read from.
     */
    public CompletableFuture<ReadResult> read(long offset, int maxLength) {
        return asDirectSegmentAccess()
                .thenApplyAsync(s -> s.read(offset, maxLength, DEFAULT_TIMEOUT), this.executor);
    }

    /**
     * Retrieves a set of Segment Attributes from the Segment.
     * NOTE: this is applicable only for main segments (attribute segment index will be derived if necessary).
     *
     * @param keys A Collection of Attribute Ids to fetch.
     * @return A CompletableFuture which will contain the result.
     */
    public CompletableFuture<Map<UUID, Long>> getAttributes(@NonNull Collection<UUID> keys) {
        return asDirectSegmentAccess()
                .thenComposeAsync(s -> s.getAttributes(keys, false, DEFAULT_TIMEOUT), this.executor);
    }

    /**
     * Updates a set of Segment Attributes for the Segment.
     *
     * @param newValues A map of Attributes to values. If an attribute should be removed, its value can either be {@code null}
     *                  or {@link Attributes#NULL_ATTRIBUTE_VALUE}.
     * @return A CompletableFuture that will indicate when the operation completed.
     */
    public CompletableFuture<Void> updateAttributes(@NonNull Map<UUID, Long> newValues) {
        val updates = newValues.entrySet().stream()
                .map(e -> new AttributeUpdate(e.getKey(), AttributeUpdateType.Replace, e.getValue() == null ? Attributes.NULL_ATTRIBUTE_VALUE : e.getValue()))
                .collect(Collectors.toList());
        return asDirectSegmentAccess()
                .thenComposeAsync(s -> s.updateAttributes(updates, DEFAULT_TIMEOUT), this.executor);
    }

    /**
     * Iterates through all the Segment Attributes for a Segment.
     * NOTE: this is applicable only for main segments (attribute segment index will be derived if necessary).
     *
     * @return A CompletableFuture which will contain an {@link AttributeIterator}.
     */
    public CompletableFuture<AttributeIterator> iterateAttributes() {
        return asDirectSegmentAccess()
                .thenComposeAsync(s -> s.attributeIterator(new UUID(Long.MIN_VALUE, Long.MIN_VALUE), new UUID(Long.MAX_VALUE, Long.MAX_VALUE), DEFAULT_TIMEOUT), this.executor);
    }

    /**
     * Retrieves a single {@link TableEntry} from a Table Segment.
     * NOTE: this is applicable only for main segments that are of type TableSegment.
     *
     * @param key The key to retrieve.
     * @return A CompletableFuture which will contain the result.
     */
    public CompletableFuture<TableEntryInfo> getTableEntry(@NonNull BufferView key) {
        val indexReader = new IndexReader(this.executor);
        return asDirectSegmentAccess().thenComposeAsync(s -> {
            val bucketReader = TableBucketReader.entry(s, indexReader::getBackpointerOffset, this.executor);
            val hash = TABLE_SEGMENT_KEY_HASHER.hash(key);
            return indexReader.locateBuckets(s, Collections.singleton(hash), new TimeoutTimer(DEFAULT_TIMEOUT))
                    .thenComposeAsync(buckets -> {
                        val b = buckets.get(hash);
                        if (b == null) {
                            return CompletableFuture.completedFuture(null);
                        }
                        return bucketReader.find(key, b.getSegmentOffset(), new TimeoutTimer(DEFAULT_TIMEOUT))
                                .thenApply(e -> new ValidTableEntryInfo(b.getHash(), b.getSegmentOffset(), e, b.getSegmentOffset()));
                    }, this.executor);
        });
    }

    /**
     * Iterates through all the {@link TableEntry} instances in a Table Segment that are accessible from the index.
     * NOTE: this is applicable only for main segments that are of type TableSegment.
     * NOTE: this is different from {@link #scanTableSegment}. This method only returns entries that are accessible from
     * the index.
     *
     * @return A CompletableFuture which will contain an {@link AsyncIterator}.
     */
    public CompletableFuture<AsyncIterator<List<TableEntryInfo>>> iterateTableEntriesFromIndex() {
        val indexReader = new IndexReader(this.executor);
        return asDirectSegmentAccess().thenComposeAsync(
                s -> s.attributeIterator(KeyHasher.MIN_HASH, KeyHasher.MAX_HASH, DEFAULT_TIMEOUT)
                        .thenApplyAsync(ai -> ai.thenCompose(buckets -> {
                            val bucketReader = TableBucketReader.entry(s, indexReader::getBackpointerOffset, this.executor);
                            val result = Collections.synchronizedList(new ArrayList<TableEntryInfo>());
                            return Futures.loop(buckets,
                                    bucket -> iterateBucket(bucket, bucketReader, result::add).thenApply(v -> true),
                                    this.executor)
                                    .thenApply(v -> result);
                        }), this.executor),
                this.executor);
    }

    private CompletableFuture<Void> iterateBucket(Map.Entry<UUID, Long> bucket, TableBucketReader<TableEntry> bucketReader,
                                                  Consumer<TableEntryInfo> acceptEntry) {
        BiConsumer<TableEntry, Long> findEntry = (entry, offset) -> {
            val keyHash = TABLE_SEGMENT_KEY_HASHER.hash(entry.getKey().getKey());
            val r = new ValidTableEntryInfo(bucket.getKey(), bucket.getValue(), entry, offset);
            if (keyHash != bucket.getKey()) {
                r.extraInfo("IndexHash/KeyHash mismatch. Actual hash: %s", keyHash);
            }

            acceptEntry.accept(new ValidTableEntryInfo(bucket.getKey(), bucket.getValue(), entry, offset));
        };
        return bucketReader.findAll(bucket.getValue(), findEntry, new TimeoutTimer(DEFAULT_TIMEOUT))
                .exceptionally(ex -> {
                    acceptEntry.accept(new InvalidTableEntryInfo(bucket.getKey(), bucket.getValue(), ex, -1L, -1L));
                    return null;
                });
    }

    /**
     * Iterates through all the {@link TableEntry} instances in a Table Segment by reading the Table Segment file.
     * NOTE: this is applicable only for main segments that are of type TableSegment.
     * NOTE: this is different from {@link #iterateTableEntriesFromIndex()}. This method reads from the main segment, which
     * may include deleted or updated entries.
     * @param startOffset Offset to start scanning at.
     * @param maxLength   Maximum number of bytes to read.
     *
     * @return A CompletableFuture which will contain an {@link AsyncIterator}.
     */
    public CompletableFuture<Iterator<TableEntryInfo>> scanTableSegment(long startOffset, int maxLength) {
        return asDirectSegmentAccess()
                .thenApplyAsync(s -> s.read(startOffset, maxLength, DEFAULT_TIMEOUT), this.executor)
                .thenApplyAsync(rr -> {
                    val buffers = rr.readRemaining(maxLength, DEFAULT_TIMEOUT);
                    val builder = BufferView.builder();
                    buffers.forEach(builder::add);
                    return new TableEntryIterator(builder.build(), startOffset);
                }, this.executor);
    }

    /**
     * Returns a {@link DirectSegmentAccess} implementation that can be used to access the underlying segment.
     *
     * @return A CompletableFuture that will contain a {@link DirectSegmentAccess} instance.
     */
    public CompletableFuture<DirectSegmentAccess> asDirectSegmentAccess() {
        return getSegmentInfo().thenApply(DirectSegmentWrapper::new);
    }

    private CompletableFuture<AttributeIndex> getAttributeIndex() {
        return this.containerAttributeIndex.forSegment(SEGMENT_ID, DEFAULT_TIMEOUT);
    }

    //endregion

    //region TableEntryIterator

    private static class TableEntryIterator implements Iterator<TableEntryInfo> {
        private BufferView buffer;
        private long startOffset;

        TableEntryIterator(@NonNull BufferView buffer, long startOffset) {
            this.buffer = buffer;
            this.startOffset = startOffset;
        }

        @Override
        public boolean hasNext() {
            return this.buffer.getLength() > 0;
        }

        @Override
        public TableEntryInfo next() {
            val entryOffset = this.startOffset;
            try {
                try {
                    val e = AsyncTableEntryReader.readEntryComponents(this.buffer.getBufferViewReader(), entryOffset, TABLE_SEGMENT_SERIALIZER);

                    // If we made it this far, we have a valid entry.
                    advance(e.getHeader().getTotalLength());
                    if (e.getValue() == null) {
                        // Removal
                        return new ValidTableEntryInfo(TABLE_SEGMENT_KEY_HASHER.hash(e.getKey()), entryOffset, TableKey.versioned(e.getKey(), e.getVersion()), entryOffset);
                    } else {
                        return new ValidTableEntryInfo(TABLE_SEGMENT_KEY_HASHER.hash(e.getKey()), entryOffset, TableEntry.versioned(e.getKey(), e.getValue(), e.getVersion()), entryOffset);
                    }
                } catch (SerializationException ex) {
                    // Invalid entry - scan for next one.
                    long skipCount = 1;
                    advance(1);
                    while (hasNext()) {
                        try {
                            AsyncTableEntryReader.readEntryComponents(this.buffer.getBufferViewReader(), this.startOffset, TABLE_SEGMENT_SERIALIZER);
                            break;
                        } catch (SerializationException ex2) {
                            skipCount++;
                            advance(1);
                        }
                    }
                    return new InvalidTableEntryInfo(null, -1, ex, entryOffset, skipCount);
                }
            } catch (BufferView.Reader.OutOfBoundsException oob) {
                int len = this.buffer.getLength();
                this.buffer = BufferView.empty();
                return new InvalidTableEntryInfo(null, -1, oob, entryOffset, len);
            }
        }

        private void advance(int skipBytes) {
            this.buffer = this.buffer.slice(skipBytes, this.buffer.getLength() - skipBytes);
            this.startOffset += skipBytes;
        }
    }

    //endregion

    //region DirectSegmentWrapper

    @Data
    private class DirectSegmentWrapper implements DirectSegmentAccess {
        private final SegmentProperties info;

        @Override
        public long getSegmentId() {
            return SEGMENT_ID;
        }

        @Override
        public ReadResult read(long offset, int maxLength, Duration timeout) {
            return StreamSegmentStorageReader.read(this.info, offset, maxLength, READ_BLOCK_SIZE, DebugStorageSegment.this.storage);
        }

        @Override
        public CompletableFuture<Map<UUID, Long>> getAttributes(Collection<UUID> attributeIds, boolean cache, Duration timeout) {
            return getAttributeIndex()
                    .thenComposeAsync(idx -> idx.get(attributeIds, DEFAULT_TIMEOUT), DebugStorageSegment.this.executor);
        }

        @Override
        public CompletableFuture<AttributeIterator> attributeIterator(UUID fromId, UUID toId, Duration timeout) {
            return getAttributeIndex()
                    .thenApplyAsync(idx -> idx.iterator(fromId, toId, DEFAULT_TIMEOUT), DebugStorageSegment.this.executor);
        }

        @Override
        public CompletableFuture<Void> updateAttributes(Collection<AttributeUpdate> attributeUpdates, Duration timeout) {
            val updates = attributeUpdates.stream().collect(Collectors.toMap(AttributeUpdate::getAttributeId, AttributeUpdate::getValue));
            return getAttributeIndex()
                    .thenComposeAsync(idx -> idx.update(updates, timeout), DebugStorageSegment.this.executor)
                    .thenRun(Runnables.doNothing());
        }

        //region Unsupported Operations

        @Override
        public CompletableFuture<Long> append(BufferView data, Collection<AttributeUpdate> attributeUpdates, Duration timeout) {
            throw new UnsupportedOperationException();
        }

        @Override
        public CompletableFuture<Long> append(BufferView data, Collection<AttributeUpdate> attributeUpdates, long offset, Duration timeout) {
            throw new UnsupportedOperationException();
        }

        @Override
        public CompletableFuture<Long> seal(Duration timeout) {
            throw new UnsupportedOperationException();
        }

        @Override
        public CompletableFuture<Void> truncate(long offset, Duration timeout) {
            throw new UnsupportedOperationException();
        }

        //endregion
    }

    //endregion

    //region TableEntryIfo and derived classes

    /**
     * Contextual information about a Table Entry.
     */
    @RequiredArgsConstructor(access = AccessLevel.PRIVATE)
    @Getter
    public static class TableEntryInfo {
        private final UUID bucketHash;
        private final long bucketOffset;
        private final long entryOffset;
        private final long length;
    }

    /**
     * Represents a valid Table Entry (whether an update or removal).
     */
    @Getter
    public static class ValidTableEntryInfo extends TableEntryInfo {
        private final TableEntry entry;
        private final boolean deleted;
        private String extraInfo;

        private ValidTableEntryInfo(UUID bucketHash, long bucketOffset, @NonNull TableEntry entry, long entryOffset) {
            super(bucketHash, bucketOffset, entryOffset, TABLE_SEGMENT_SERIALIZER.getUpdateLength(entry));
            this.entry = entry;
            this.deleted = false;
        }

        private ValidTableEntryInfo(UUID bucketHash, long bucketOffset, @NonNull TableKey removedKey, long keyOffset) {
            super(bucketHash, bucketOffset, keyOffset, TABLE_SEGMENT_SERIALIZER.getRemovalLength(removedKey));
            this.entry = TableEntry.versioned(removedKey.getKey(), BufferView.empty(), removedKey.getVersion());
            this.deleted = true;
        }

        private ValidTableEntryInfo extraInfo(String msgFormat, Object... args) {
            val e = String.format(msgFormat, args);
            this.extraInfo = this.extraInfo == null ? e : this.extraInfo + "; " + e;
            return this;
        }
    }

    /**
     * Represents a segment range that cannot be interpreted as a valid Table Entry.
     */
    @Getter
    public static class InvalidTableEntryInfo extends TableEntryInfo {
        /**
         * The exception that was thrown when attempting to deserialize this segment range and interpret as Table Entry.
         */
        private final Throwable exception;

        private InvalidTableEntryInfo(UUID bucketHash, long bucketOffset, @NonNull Throwable exception, long entryOffset, long length) {
            super(bucketHash, bucketOffset, entryOffset, length);
            this.exception = exception;
        }
    }

    //endregion
}
