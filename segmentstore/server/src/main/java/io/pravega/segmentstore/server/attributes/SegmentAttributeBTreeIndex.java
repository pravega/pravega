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
package io.pravega.segmentstore.server.attributes;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import io.pravega.common.Exceptions;
import io.pravega.common.TimeoutTimer;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.util.AsyncIterator;
import io.pravega.common.util.BufferView;
import io.pravega.common.util.ByteArraySegment;
import io.pravega.common.util.IllegalDataFormatException;
import io.pravega.common.util.Retry;
import io.pravega.common.util.btree.BTreeIndex;
import io.pravega.common.util.btree.PageEntry;
import io.pravega.segmentstore.contracts.AttributeId;
import io.pravega.segmentstore.contracts.Attributes;
import io.pravega.segmentstore.contracts.BadOffsetException;
import io.pravega.segmentstore.contracts.SegmentProperties;
import io.pravega.segmentstore.contracts.StreamSegmentExistsException;
import io.pravega.segmentstore.contracts.StreamSegmentNotExistsException;
import io.pravega.segmentstore.contracts.StreamSegmentSealedException;
import io.pravega.segmentstore.contracts.StreamSegmentTruncatedException;
import io.pravega.segmentstore.server.AttributeIndex;
import io.pravega.segmentstore.server.AttributeIterator;
import io.pravega.segmentstore.server.CacheManager;
import io.pravega.segmentstore.server.DataCorruptionException;
import io.pravega.segmentstore.server.SegmentMetadata;
import io.pravega.segmentstore.storage.SegmentHandle;
import io.pravega.segmentstore.storage.Storage;
import io.pravega.segmentstore.storage.cache.CacheFullException;
import io.pravega.segmentstore.storage.cache.CacheStorage;
import io.pravega.shared.NameUtils;
import java.io.InputStream;
import java.io.SequenceInputStream;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import javax.annotation.concurrent.GuardedBy;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

/**
 * Attribute Index for a single Segment, backed by a B+Tree Index implementation.
 */
@Slf4j
class SegmentAttributeBTreeIndex implements AttributeIndex, CacheManager.Client, AutoCloseable {
    //region Members

    /**
     * For Attribute Segment Appends, we want to write conditionally based on the offset, and retry the operation if
     * it failed for that reason. That guarantees that we won't be losing any data if we get concurrent calls to update().
     */
    private static final Retry.RetryAndThrowBase<Exception> UPDATE_RETRY = Retry
            .withExpBackoff(10, 2, 10, 1000)
            .retryingOn(BadOffsetException.class)
            .throwingOn(Exception.class);

    /**
     * Calls to get() and put() can execute concurrently, which means we can have concurrent reads and writes from/to the
     * Attribute Segment, which in turn means we can truncate the segment while reading from it. We need to retry reads
     * if we stumble upon a segment truncation.
     */
    private static final Retry.RetryAndThrowBase<Exception> READ_RETRY = Retry
            .withExpBackoff(10, 2, 10, 1000)
            .retryingOn(StreamSegmentTruncatedException.class)
            .throwingOn(Exception.class);

    private static final int DEFAULT_KEY_LENGTH = Attributes.ATTRIBUTE_ID_LENGTH.byteCount();
    private static final int VALUE_LENGTH = Long.BYTES;
    private final SegmentMetadata segmentMetadata;
    private final AtomicReference<SegmentHandle> handle;
    private final Storage storage;
    @GuardedBy("cacheEntries")
    private final CacheStorage cacheStorage;
    @GuardedBy("cacheEntries")
    private int currentCacheGeneration;
    @GuardedBy("cacheEntries")
    private final Map<Long, CacheEntry> cacheEntries;
    @GuardedBy("cacheEntries")
    private boolean cacheDisabled;
    @GuardedBy("pendingReads")
    private final Map<Long, PendingRead> pendingReads;
    private final BTreeIndex index;
    private final AttributeIndexConfig config;
    private final ScheduledExecutorService executor;
    private final String traceObjectId;
    private final AtomicBoolean closed;
    private final KeySerializer keySerializer;

    //endregion

    //region Constructor and Initialization

    /**
     * Creates a new instance of the SegmentAttributeIndex class.
     *
     * @param segmentMetadata The SegmentMetadata of the Segment whose attributes we want to manage.
     * @param storage         A Storage adapter which can be used to access the Attribute Segment.
     * @param config          Attribute Index Configuration.
     * @param executor        An Executor to run async tasks.
     */
    SegmentAttributeBTreeIndex(@NonNull SegmentMetadata segmentMetadata, @NonNull Storage storage, @NonNull CacheStorage cacheStorage,
                               @NonNull AttributeIndexConfig config, @NonNull ScheduledExecutorService executor) {
        this.segmentMetadata = segmentMetadata;
        this.storage = storage;
        this.cacheStorage = cacheStorage;
        this.config = config;
        this.executor = executor;
        this.handle = new AtomicReference<>();
        this.traceObjectId = String.format("AttributeIndex[%d-%d]", this.segmentMetadata.getContainerId(), this.segmentMetadata.getId());
        this.keySerializer = getKeySerializer(segmentMetadata);
        this.index = BTreeIndex.builder()
                               .keyLength(this.keySerializer.getKeyLength())
                               .valueLength(VALUE_LENGTH)
                               .maxPageSize(this.config.getMaxIndexPageSize())
                               .executor(this.executor)
                               .getLength(this::getLength)
                               .readPage(this::readPage)
                               .writePages(this::writePages)
                               .maintainStatistics(shouldMaintainStatistics(segmentMetadata))
                               .traceObjectId(this.traceObjectId)
                               .build();

        this.cacheEntries = new HashMap<>();
        this.pendingReads = new HashMap<>();
        this.closed = new AtomicBoolean();
        this.cacheDisabled = false;
    }

    private KeySerializer getKeySerializer(SegmentMetadata segmentMetadata) {
        long keyLength = segmentMetadata.getAttributeIdLength();
        Preconditions.checkArgument(keyLength <= AttributeId.MAX_LENGTH, "Invalid value %s for attribute `%s` for Segment `%s'. Expected at most %s.",
                Attributes.ATTRIBUTE_ID_LENGTH, segmentMetadata.getName(), AttributeId.MAX_LENGTH);
        return keyLength <= 0 ? new UUIDKeySerializer() : new BufferKeySerializer((int) keyLength);
    }

    private boolean shouldMaintainStatistics(SegmentMetadata segmentMetadata) {
        return segmentMetadata.getType().isFixedKeyLengthTableSegment();
    }

    /**
     * Initializes the SegmentAttributeIndex.
     *
     * @param timeout Timeout for the operation.
     * @return A CompletableFuture that, when completed, will indicate the operation has succeeded.
     */
    CompletableFuture<Void> initialize(Duration timeout) {
        TimeoutTimer timer = new TimeoutTimer(timeout);
        Preconditions.checkState(!this.index.isInitialized(), "SegmentAttributeIndex is already initialized.");
        String attributeSegmentName = NameUtils.getAttributeSegmentName(this.segmentMetadata.getName());

        // Attempt to open the Attribute Segment; if it does not exist do not create it now. It will be created when we
        // make the first write.
        return Futures
                .exceptionallyExpecting(
                        this.storage.openWrite(attributeSegmentName).thenAccept(this.handle::set),
                        ex -> ex instanceof StreamSegmentNotExistsException,
                        null)
                .thenComposeAsync(v -> this.index.initialize(timer.getRemaining()), this.executor)
                .thenRun(() -> log.debug("{}: Initialized.", this.traceObjectId))
                .exceptionally(this::handleIndexOperationException);
    }

    /**
     * Deletes all the Attribute data associated with the given Segment. This operation will have no effect if the
     * Attribute Segment associated with the given Segment does not exist.
     *
     * @param segmentName The name of the Segment whose attribute data should be deleted.
     * @param storage     A Storage Adapter to execute the deletion on.
     * @param timeout     Timeout for the operation.
     * @return A CompletableFuture that, when completed, will indicate that the operation finished successfully.
     */
    static CompletableFuture<Void> delete(String segmentName, Storage storage, Duration timeout) {
        TimeoutTimer timer = new TimeoutTimer(timeout);
        String attributeSegmentName = NameUtils.getAttributeSegmentName(segmentName);
        return Futures.exceptionallyExpecting(
                storage.openWrite(attributeSegmentName)
                       .thenCompose(handle -> storage.delete(handle, timer.getRemaining())),
                ex -> ex instanceof StreamSegmentNotExistsException,
                null);
    }

    //endregion

    //region AutoCloseable Implementation

    @Override
    public void close() {
        if (!this.closed.getAndSet(true)) {
            // Close storage reader (and thus cancel those reads).
            this.executor.execute(() -> {
                removeAllCacheEntries();
                cancelPendingReads();
                log.info("{}: Closed.", this.traceObjectId);
            });
        }
    }

    private void cancelPendingReads() {
        List<PendingRead> toCancel;
        synchronized (this.pendingReads) {
            toCancel = new ArrayList<>(this.pendingReads.values());
            this.pendingReads.clear();
        }
        toCancel.forEach(f -> f.completion.cancel(true));
    }

    /**
     * Removes all entries from the cache.
     */
    @VisibleForTesting
    void removeAllCacheEntries() {
        List<CacheEntry> entries;
        synchronized (this.cacheEntries) {
            entries = new ArrayList<>(this.cacheEntries.values());
            this.cacheEntries.clear();
        }

        removeFromCache(entries);
        if (entries.size() > 0) {
            log.debug("{}: Cleared all cache entries ({}).", this.traceObjectId, entries.size());
        }
    }

    //endregion

    //region CacheManager.Client implementation

    @Override
    public CacheManager.CacheStatus getCacheStatus() {
        synchronized (this.cacheEntries) {
            return CacheManager.CacheStatus.fromGenerations(
                    this.cacheEntries.values().stream().filter(Objects::nonNull).map(CacheEntry::getGeneration).iterator());
        }
    }

    @Override
    public boolean updateGenerations(int currentGeneration, int oldestGeneration, boolean essentialOnly) {
        Exceptions.checkNotClosed(this.closed.get(), this);

        // Remove those entries that have a generation below the oldest permissible one.
        boolean anyRemoved;
        synchronized (this.cacheEntries) {
            // All of our data are non-essential as we only cache BTreeIndex pages that are already durable persisted.
            this.cacheDisabled = essentialOnly;
            this.currentCacheGeneration = currentGeneration;
            ArrayList<CacheEntry> toRemove = new ArrayList<>();
            for (val entry : this.cacheEntries.values()) {
                if (entry.getGeneration() < oldestGeneration) {
                    toRemove.add(entry);
                }
            }

            removeFromCache(toRemove);
            anyRemoved = !toRemove.isEmpty();
        }

        return anyRemoved;
    }

    //endregion

    //region AttributeIndex Implementation

    @Override
    public CompletableFuture<Long> update(@NonNull Map<AttributeId, Long> values, @NonNull Duration timeout) {
        ensureInitialized();
        if (values.isEmpty()) {
            // Nothing to do.
            return CompletableFuture.completedFuture(null);
        }

        Collection<PageEntry> entries = values.entrySet().stream().map(this::serialize).collect(Collectors.toList());
        return executeConditionally(tm -> this.index.update(entries, tm), timeout);
    }

    @Override
    public CompletableFuture<Map<AttributeId, Long>> get(@NonNull Collection<AttributeId> keys, @NonNull Duration timeout) {
        ensureInitialized();
        if (keys.isEmpty()) {
            // Nothing to do.
            return CompletableFuture.completedFuture(Collections.emptyMap());
        }

        // Keep two lists, one of keys (in some order) and one of serialized keys (in the same order).
        val keyList = new ArrayList<AttributeId>(keys.size());
        val serializedKeys = new ArrayList<ByteArraySegment>(keys.size());
        for (AttributeId key : keys) {
            keyList.add(key);
            serializedKeys.add(this.keySerializer.serialize(key));
        }

        // Fetch the raw data from the index, but retry (as needed) if we run across a truncated part of the attribute
        // segment file (see READ_RETRY Javadoc).
        return READ_RETRY.runAsync(() -> this.index.get(serializedKeys, timeout), this.executor)
                .thenApply(entries -> {
                    assert entries.size() == keys.size() : "Unexpected number of entries returned by the index search.";

                    // The index search result is a list of values in the same order as the keys we passed in, so we need
                    // to use the list index to match them.
                    Map<AttributeId, Long> result = new HashMap<>();
                    for (int i = 0; i < keyList.size(); i++) {
                        ByteArraySegment v = entries.get(i);
                        if (v != null) {
                            // BTreeIndex will return null if a key is not present; however we exclude that from our result.
                            result.put(keyList.get(i), deserializeValue(v));
                        }
                    }

                    return result;
                })
                .exceptionally(this::handleIndexOperationException);

    }

    @Override
    public CompletableFuture<Void> seal(@NonNull Duration timeout) {
        ensureInitialized();
        SegmentHandle handle = this.handle.get();
        if (handle == null) {
            // Empty Attribute Index. There is no point in sealing since we won't be allowed to update anything new from now on.
            return CompletableFuture.completedFuture(null);
        }

        return Futures.exceptionallyExpecting(
                this.storage.seal(handle, timeout)
                            .thenRun(() -> log.info("{}: Sealed.", this.traceObjectId)),
                ex -> ex instanceof StreamSegmentSealedException,
                null);
    }

    @Override
    public AttributeIterator iterator(AttributeId fromId, AttributeId toId, Duration fetchTimeout) {
        ensureInitialized();
        return new AttributeIteratorImpl(fromId, (id, inclusive) ->
                this.index.iterator(this.keySerializer.serialize(id), inclusive, this.keySerializer.serialize(toId), true, fetchTimeout));
    }

    @Override
    public long getCount() {
        val s = this.index.getStatistics();
        return s == null ? -1 : s.getEntryCount();
    }

    //endregion

    //region Helpers

    /**
     * Gets a pointer to the SegmentHandle for the Attribute Segment.
     */
    @VisibleForTesting
    SegmentHandle getAttributeSegmentHandle() {
        return this.handle.get();
    }

    @VisibleForTesting
    int getPendingReadCount() {
        synchronized (this.pendingReads) {
            return this.pendingReads.size();
        }
    }

    @Override
    public String toString() {
        return this.traceObjectId;
    }

    /**
     * Creates the Attribute Segment in Storage if it is not already created. If it exists already, the given task is
     * executed immediately, otherwise it is executed only after the Attribute Segment is created.
     *
     * @param toRun   The task to execute.
     * @param timeout Timeout for the operation.
     * @param <T>     Return type.
     * @return A CompletableFuture that will be completed with the result (or failure cause) of the given task toRun.
     */
    private <T> CompletableFuture<T> createAttributeSegmentIfNecessary(Supplier<CompletableFuture<T>> toRun, Duration timeout) {
        if (this.handle.get() == null) {
            String attributeSegmentName = NameUtils.getAttributeSegmentName(this.segmentMetadata.getName());
            return Futures
                    .exceptionallyComposeExpecting(
                            this.storage.create(attributeSegmentName, this.config.getAttributeSegmentRollingPolicy(), timeout),
                            ex -> ex instanceof StreamSegmentExistsException,
                            () -> {
                                log.info("{}: Attribute Segment did not exist in Storage when initialize() was called, but does now.", this.traceObjectId);
                                return this.storage.openWrite(attributeSegmentName);
                            })
                    .thenComposeAsync(handle -> {
                        this.handle.set(handle);
                        return toRun.get();
                    }, this.executor);
        } else {
            // Attribute Segment already exists.
            return toRun.get();
        }
    }

    /**
     * Executes the given Index Operation with retries. Retries are only performed in case of conditional update failures,
     * represented by BadOffsetException.
     *
     * @param indexOperation A Function, that, when invoked, returns a CompletableFuture which indicates when the index
     *                       operation completes.
     * @param timeout        Timeout for the operation.
     * @return A CompletableFuture that will indicate when the operation completes.
     */
    private CompletableFuture<Long> executeConditionally(Function<Duration, CompletableFuture<Long>> indexOperation, Duration timeout) {
        TimeoutTimer timer = new TimeoutTimer(timeout);
        return UPDATE_RETRY
                .runAsync(() -> executeConditionallyOnce(indexOperation, timer), this.executor)
                .exceptionally(this::handleIndexOperationException);
    }

    /**
     * Executes the given Index Operation once, without performing retries. In case of failure with BadOffsetException
     * (which indicates a conditional update failure), the BTreeIndex is reinitialized to the most up-to-date state.
     *
     * @param indexOperation A Function, that, when invoked, returns a CompletableFuture which indicates when the index
     *                       operation completes.
     * @param timer          Timer for the operation.
     * @return A CompletableFuture that will indicate when the operation completes.
     */
    private CompletableFuture<Long> executeConditionallyOnce(Function<Duration, CompletableFuture<Long>> indexOperation, TimeoutTimer timer) {
        return Futures.exceptionallyCompose(
                indexOperation.apply(timer.getRemaining()),
                ex -> {
                    if (Exceptions.unwrap(ex) instanceof BadOffsetException) {
                        BadOffsetException boe = (BadOffsetException) Exceptions.unwrap(ex);
                        if (boe.getExpectedOffset() != this.index.getIndexLength()) {
                            log.warn("{}: Conditional Index Update failed (expected {}, given {}). Reinitializing index.",
                                    this.traceObjectId, boe.getExpectedOffset(), boe.getGivenOffset());
                            return this.index.initialize(timer.getRemaining())
                                             .thenCompose(v -> Futures.failedFuture(ex));
                        }
                    }

                    // Make sure the exception bubbles up.
                    return Futures.failedFuture(ex);
                });
    }

    private PageEntry serialize(Map.Entry<AttributeId, Long> entry) {
        return new PageEntry(this.keySerializer.serialize(entry.getKey()), serializeValue(entry.getValue()));
    }

    private ByteArraySegment serializeValue(Long value) {
        if (value == null || value == Attributes.NULL_ATTRIBUTE_VALUE) {
            // Deletion.
            return null;
        }

        ByteArraySegment result = new ByteArraySegment(new byte[VALUE_LENGTH]);
        result.setLong(0, value);
        return result;
    }

    private long deserializeValue(ByteArraySegment value) {
        Preconditions.checkArgument(value.getLength() == VALUE_LENGTH, "Unexpected value length.");
        return value.getLong(0);
    }

    private CompletableFuture<BTreeIndex.IndexInfo> getLength(Duration timeout) {
        Exceptions.checkNotClosed(this.closed.get(), this);
        SegmentHandle handle = this.handle.get();
        if (handle == null) {
            return CompletableFuture.completedFuture(BTreeIndex.IndexInfo.EMPTY);
        }

        return this.storage.getStreamSegmentInfo(handle.getSegmentName(), timeout)
                .thenApply(segmentInfo -> {
                    // Get the root pointer from the Segment's Core Attributes.
                    long rootPointer = getRootPointerIfNeeded(segmentInfo);
                    return new BTreeIndex.IndexInfo(segmentInfo.getLength(), rootPointer);
                });
    }

    /**
     * Extracts the {@link Attributes#ATTRIBUTE_SEGMENT_ROOT_POINTER} from the given {@link SegmentProperties} if necessary.
     * If {@link Storage#supportsAtomicWrites()} is true for {@link #storage}, then a negative value is returned and
     * the information from the given {@link SegmentProperties} is ignored.
     *
     * @param segmentInfo The {@link SegmentProperties} to extract from.
     * @return The extracted root pointer or a negative value if not needed.
     */
    private long getRootPointerIfNeeded(SegmentProperties segmentInfo) {
        // Get the root pointer from the Segment's Core Attributes.
        long rootPointer = BTreeIndex.IndexInfo.EMPTY.getRootPointer(); // -1;
        if (this.storage.supportsAtomicWrites()) {
            // No need to worry about Root Pointers if the underlying Storage supports atomic writes. We only need this
            // for RollingStorage which does not make such guarantees.
            return rootPointer;
        }

        rootPointer = this.segmentMetadata.getAttributes().getOrDefault(Attributes.ATTRIBUTE_SEGMENT_ROOT_POINTER, rootPointer);
        if (rootPointer != BTreeIndex.IndexInfo.EMPTY.getRootPointer() && rootPointer < segmentInfo.getStartOffset()) {
            // The Root Pointer is invalid as it points to an offset prior to the Attribute Segment's Start Offset.
            // The Attribute Segment is updated in 3 sequential steps: 1) Write new BTree pages, 2) Truncate and
            // 3) Update root Pointer.
            // The purpose of the Root Pointer is to provide a location of a consistently written update in case
            // step 1) above fails (it is not atomic). However, if both 1) and 2) complete but 3) doesn't, then
            // it's possible that the existing Root Pointer has been truncated out. In this case, it should be
            // safe to ignore it and let the BTreeIndex read the file from the end (as it does in this case).
            log.info("{}: Root Pointer ({}) is below Attribute Segment's StartOffset ({}). Ignoring.", this.traceObjectId, rootPointer, segmentInfo.getStartOffset());
            rootPointer = BTreeIndex.IndexInfo.EMPTY.getRootPointer();
        }
        return rootPointer;
    }

    private CompletableFuture<ByteArraySegment> readPage(long offset, int length, boolean cacheResult, Duration timeout) {
        // First, check in the cache.
        byte[] fromCache = getFromCache(offset, length);
        if (fromCache != null) {
            return CompletableFuture.completedFuture(new ByteArraySegment(fromCache));
        }

        // Cache miss; load data from Storage.
        SegmentHandle handle = this.handle.get();
        if (handle == null) {
            // Attribute Segment does not exist.
            if (offset == 0 && length == 0) {
                // Reading 0 bytes at offset 0 is a valid operation (inexistent Attribute Segment is equivalent to an empty one).
                return CompletableFuture.completedFuture(new ByteArraySegment(new byte[0]));
            } else {
                return Futures.failedFuture(new ArrayIndexOutOfBoundsException(String.format(
                        "Attribute Index Segment has not been created yet. Cannot read %d byte(s) from offset (%d).",
                        length, offset)));
            }
        }

        return readPageFromStorage(handle, offset, length, cacheResult, timeout);
    }

    /**
     * Reads a BTreeIndex page from Storage and inserts its into the cache (no cache lookups are performed).
     * Handles read concurrency on the same page by piggybacking on existing running reads on that page (identified by
     * page offset). If more than one concurrent request is issued for the same page, only one will be sent to Storage,
     * and subsequent ones will be attached to the original one.
     *
     * @param handle      {@link SegmentHandle} to read from.
     * @param offset      Page offset.
     * @param length      Page length.
     * @param cacheResult If true, the result of this operation should be cached, if possible.
     * @param timeout     Timeout for the operation.
     * @return A CompletableFuture that will contain the result.
     */
    @VisibleForTesting
    CompletableFuture<ByteArraySegment> readPageFromStorage(SegmentHandle handle, long offset, int length, boolean cacheResult, Duration timeout) {
        PendingRead pr;
        synchronized (this.pendingReads) {
            pr = this.pendingReads.get(offset);
            if (pr == null) {
                // Nobody else waiting for this offset. Register ourselves.
                pr = new PendingRead(offset, length);
                pr.completion.whenComplete((r, ex) -> unregisterPendingRead(offset));
                this.pendingReads.put(offset, pr);
            } else if (pr.length < length) {
                // Somehow the previous request wanted to read less than us. This shouldn't be the case, yet it is
                // a situation we should handle. In his case, we will not be recording the PendingRead.
                log.warn("{}: Concurrent read at (Offset={}, OldLength={}), but with different length ({}). Not piggybacking.", this.traceObjectId, offset, pr.length, length);
                pr = new PendingRead(offset, length);
            } else {
                // Piggyback on the existing read.
                log.debug("{}: Concurrent read (Offset={}, Length={}, NewCount={}). Piggybacking.", this.traceObjectId, offset, pr.length, pr.count);
                pr.count.incrementAndGet();
                return pr.completion;
            }
        }

        // Issue the read request.
        return readPageFromStorage(handle, pr, cacheResult, timeout);
    }

    private CompletableFuture<ByteArraySegment> readPageFromStorage(SegmentHandle handle, PendingRead pr, boolean cacheResult, Duration timeout) {
        byte[] buffer = new byte[pr.length];
        Futures.completeAfter(
                () -> this.storage.read(handle, pr.offset, buffer, 0, pr.length, timeout)
                        .thenApplyAsync(bytesRead -> {
                            Preconditions.checkArgument(pr.length == bytesRead, "Unexpected number of bytes read.");
                            if (cacheResult) {
                                storeInCache(pr.offset, buffer);
                            }
                            return new ByteArraySegment(buffer);
                        }, this.executor),
                pr.completion);
        return pr.completion;
    }

    private void unregisterPendingRead(long offset) {
        PendingRead pr;
        synchronized (this.pendingReads) {
            pr = this.pendingReads.remove(offset);
        }
        if (pr != null && pr.count.get() > 1) {
            // Only do it for the interesting cases (don't spam the logs if there's no concurrency).
            log.debug("{}: Concurrent reads unregistered (Offset={}, Length={}, Count={}).", this.traceObjectId, pr.offset, pr.length, pr.count);
        }
    }

    private CompletableFuture<Long> writePages(List<BTreeIndex.WritePage> pages, Collection<Long> obsoleteOffsets,
                                               long truncateOffset, Duration timeout) {
        // The write offset is the offset of the first page to be written in the list.
        long writeOffset = pages.get(0).getOffset();

        // Collect the data to be written.
        val streams = new ArrayList<InputStream>();
        AtomicInteger length = new AtomicInteger();
        for (val p : pages) {
            // Validate that the given pages are indeed in the correct order.
            Preconditions.checkArgument(p.getOffset() == writeOffset + length.get(), "Unexpected page offset.");

            // Collect the pages (as InputStreams) and record their lengths.
            streams.add(p.getContents().getReader());
            length.addAndGet(p.getContents().getLength());
        }

        // Create the Attribute Segment in Storage (if needed), then write the new data to it and truncate if necessary.
        TimeoutTimer timer = new TimeoutTimer(timeout);
        return createAttributeSegmentIfNecessary(() -> writeToSegment(streams, writeOffset, length.get(), timer), timer.getRemaining())
                .thenApplyAsync(v -> {
                    Exceptions.checkNotClosed(this.closed.get(), this);

                    // Trigger an async truncation. There is no need to wait for it.
                    truncateAsync(truncateOffset, timer.getRemaining());

                    // Store data in cache and remove obsolete pages.
                    storeInCache(pages, obsoleteOffsets);

                    // Return the current length of the Segment Attribute Index.
                    return writeOffset + length.get();
                }, this.executor);
    }

    private CompletableFuture<Void> writeToSegment(List<InputStream> streams, long writeOffset, int length, TimeoutTimer timer) {
        // Stitch the collected Input Streams and write them to Storage.
        val toWrite = new SequenceInputStream(Collections.enumeration(streams));
        return this.storage.write(this.handle.get(), writeOffset, toWrite, length, timer.getRemaining());
    }

    /**
     * Initiates a segment truncation at the given offset, but does not wait for it. An exception listener is attached
     * which will log any errors.
     *
     * Truncation is only useful for compaction so it is not a big deal if it doesn't complete or fails. A subsequent
     * attempt (via another update) will include this one as well.
     */
    private void truncateAsync(long truncateOffset, Duration timeout) {
        if (this.storage.supportsTruncation() && truncateOffset >= 0) {
            log.debug("{}: Truncating attribute segment at offset {}.", this.traceObjectId, truncateOffset);
            Futures.exceptionListener(this.storage.truncate(this.handle.get(), truncateOffset, timeout),
                    ex -> log.warn("{}: Error while performing async truncation at offset {}.", this.traceObjectId, truncateOffset, ex));
        } else {
            log.debug("{}: Not truncating attribute segment. SupportsTruncation = {}, TruncateOffset = {}.",
                    this.traceObjectId, this.storage.supportsTruncation(), truncateOffset);
        }
    }

    private byte[] getFromCache(long offset, int length) {
        synchronized (this.cacheEntries) {
            CacheEntry entry = this.cacheEntries.getOrDefault(offset, null);
            if (entry != null) {
                BufferView data = this.cacheStorage.get(entry.getCacheAddress());
                if (data != null && data.getLength() == length) {
                    // We only deem a cache entry valid if it exists and has the expected length; otherwise it's best
                    // if we treat it as a cache miss and re-read it from Storage.
                    entry.setGeneration(this.currentCacheGeneration);
                    // TODO: we do need a copy since we are making changes to this thing and we shouldn't modify the cache directly.
                    return data.getCopy();
                }
            }
        }

        return null;
    }

    private void storeInCache(long offset, byte[] data) {
        synchronized (this.cacheEntries) {
            Exceptions.checkNotClosed(this.closed.get(), this);
            storeInCache(offset, new ByteArraySegment(data));
        }
    }

    private void storeInCache(List<BTreeIndex.WritePage> toAdd, Collection<Long> obsoleteOffsets) {
        synchronized (this.cacheEntries) {
            Exceptions.checkNotClosed(this.closed.get(), this);

            // Remove obsolete pages.
            obsoleteOffsets.stream()
                    .map(this.cacheEntries::get)
                    .filter(Objects::nonNull)
                    .forEach(this::removeFromCache);

            // Add new ones.
            for (val p : toAdd) {
                if (p.isCache()) {
                    storeInCache(p.getOffset(), p.getContents());
                }
            }
        }
    }

    @GuardedBy("cacheEntries")
    private void storeInCache(long entryOffset, ByteArraySegment data) {
        CacheEntry entry = this.cacheEntries.getOrDefault(entryOffset, null);
        if (entry != null && entry.getSize() == data.getLength()) {
            // Already cached.
            return;
        }

        // All our cache entries are considered "non-essential" since they can always be reloaded from Storage. Do not
        // try to insert/update them into the cache if such traffic is discouraged.
        if (this.cacheDisabled) {
            log.debug("{}: Cache update skipped for offset {}, length {}. Non-essential cache disabled.",
                    this.traceObjectId, entryOffset, data.getLength());
            return;
        }

        // Insert/Update the cache. There is a chance this won't go through, so be able to handle that. It's not the end
        // of the world if we can't update the cache (as we always have the data in the attribute segment). If we are
        // having trouble, log the error and move on.
        int entryAddress = entry == null ? CacheEntry.NO_ADDRESS : entry.getCacheAddress();
        try {
            if (entry != null && entry.isStored()) {
                // Entry exists. Replace it.
                entryAddress = this.cacheStorage.replace(entry.getCacheAddress(), data);
            } else {
                // Entry does not exist. Insert it.
                entryAddress = this.cacheStorage.insert(data);
            }
        } catch (CacheFullException cfe) {
            log.warn("{}: Cache update failed for offset {}, length {} (existing entry={}). Cache full.",
                    this.traceObjectId, entryOffset, data.getLength(), entry);
        } catch (Throwable ex) {
            log.error("{}: Cache update failed for offset {}, length {} (existing entry={}).",
                    this.traceObjectId, entryOffset, data.getLength(), entry, ex);
        }

        // Create a new entry wrapper.
        entry = new CacheEntry(entryOffset, data.getLength(), this.currentCacheGeneration, entryAddress);
        if (entry.isStored()) {
            this.cacheEntries.put(entryOffset, entry);
        } else {
            // If we were unable to store this entry, remove it from the index.
            this.cacheEntries.remove(entry.getOffset());
        }
    }

    private void removeFromCache(Collection<CacheEntry> entries) {
        synchronized (this.cacheEntries) {
            entries.forEach(this::removeFromCache);
        }
    }

    @GuardedBy("cacheEntries")
    private void removeFromCache(CacheEntry e) {
        if (e.isStored()) {
            this.cacheStorage.delete(e.getCacheAddress());
        }
        this.cacheEntries.remove(e.getOffset());
    }

    private void ensureInitialized() {
        Exceptions.checkNotClosed(this.closed.get(), this);
        Preconditions.checkState(this.index.isInitialized(), "SegmentAttributeIndex is not initialized.");
    }

    @SneakyThrows
    private <T> T handleIndexOperationException(Throwable ex) {
        // BTreeIndex throws IllegalDataFormatException (since DataCorruptionException does not exist at that level),
        // so we must convert that so that upstream layers can take appropriate action.
        ex = Exceptions.unwrap(ex);
        if (ex instanceof IllegalDataFormatException) {
            throw new DataCorruptionException("BTreeIndex operation failed. Index corrupted.", ex);
        }

        throw ex;
    }

    //endregion

    //region CacheEntry

    /**
     * An entry in the Cache to which one or more Attributes are mapped.
     */
    @AllArgsConstructor
    private static class CacheEntry {
        static final int NO_ADDRESS = -1;
        /**
         * Id of the entry. This is used to lookup cached data in the Cache.
         */
        @Getter
        private final long offset;
        @Getter
        private final int size;
        @GuardedBy("this")
        private int generation;
        /**
         * The {@link CacheStorage} address for this Cache Entry's data.
         */
        @Getter
        private final int cacheAddress;

        /**
         * Gets a value representing the current Generation of this Cache Entry.
         */
        synchronized int getGeneration() {
            return this.generation;
        }

        /**
         * Gets a value representing whether this Cache Entry does have any data stored in the {@link CacheStorage}.
         */
        synchronized boolean isStored() {
            return this.cacheAddress >= 0;
        }

        /**
         * Sets the current Generation of this Cache Entry.
         *
         * @param value The current Generation to set.
         */
        synchronized void setGeneration(int value) {
            this.generation = value;
        }

        @Override
        public String toString() {
            return String.format("Offset = %s, Length = %s", this.offset, this.size);
        }
    }

    //endregion

    //region AttributeIteratorImpl

    /**
     * Converts a Page Entry Iterator into an Attribute Iterator.
     */
    private class AttributeIteratorImpl implements AttributeIterator {
        private final CreatePageEntryIterator getPageEntryIterator;
        private final AtomicReference<AsyncIterator<List<PageEntry>>> pageEntryIterator;
        private final AtomicReference<AttributeId> lastProcessedId;
        private final AtomicBoolean firstInvocation;

        AttributeIteratorImpl(AttributeId firstId, CreatePageEntryIterator getPageEntryIterator) {
            this.getPageEntryIterator = getPageEntryIterator;
            this.pageEntryIterator = new AtomicReference<>();
            this.lastProcessedId = new AtomicReference<>(firstId);
            this.firstInvocation = new AtomicBoolean(true);
            reinitialize();
        }

        @Override
        public CompletableFuture<List<Map.Entry<AttributeId, Long>>> getNext() {
            return READ_RETRY
                    .runAsync(this::getNextPageEntries, executor)
                    .thenApply(pageEntries -> {
                        if (pageEntries == null) {
                            // We are done.
                            return null;
                        }

                        val result = pageEntries.stream()
                                                .map(e -> Maps.immutableEntry(keySerializer.deserialize(e.getKey()), deserializeValue(e.getValue())))
                                                .collect(Collectors.toList());
                        if (result.size() > 0) {
                            // Update the last Attribute Id and also indicate that we have processed at least one iteration.
                            this.lastProcessedId.set(result.get(result.size() - 1).getKey());
                            this.firstInvocation.set(false);
                        }

                        return result;
                    })
                    .exceptionally(SegmentAttributeBTreeIndex.this::handleIndexOperationException);
        }

        private CompletableFuture<List<PageEntry>> getNextPageEntries() {
            Exceptions.checkNotClosed(SegmentAttributeBTreeIndex.this.closed.get(), SegmentAttributeBTreeIndex.this);
            return this.pageEntryIterator
                    .get().getNext()
                    .exceptionally(ex -> {
                        // Reinitialize the iterator, then rethrow the exception so we may try again.
                        reinitialize();
                        throw new CompletionException(ex);
                    });
        }

        private void reinitialize() {
            // If this is the first invocation then we need to treat the lastProcessedId as "inclusive" in the iterator, since it
            // was the first value we wanted our iterator to begin at. For any other cases, we need to treat it as exclusive,
            // since it stores the last id we have ever returned, so we want to begin with the following one.
            Exceptions.checkNotClosed(SegmentAttributeBTreeIndex.this.closed.get(), SegmentAttributeBTreeIndex.this);
            this.pageEntryIterator.set(this.getPageEntryIterator.apply(this.lastProcessedId.get(), this.firstInvocation.get()));
        }
    }

    @FunctionalInterface
    private interface CreatePageEntryIterator {
        AsyncIterator<List<PageEntry>> apply(AttributeId firstId, boolean firstIdInclusive);
    }

    @RequiredArgsConstructor
    private static class PendingRead {
        final long offset;
        final int length;
        final AtomicInteger count = new AtomicInteger(1);
        final CompletableFuture<ByteArraySegment> completion = new CompletableFuture<>();
    }

    //endregion

    //region Key Serializers

    /**
     * Serializer for Keys.
     */
    private static abstract class KeySerializer {
        /**
         * The expected Key Length for this serializer.
         */
        abstract int getKeyLength();

        /**
         * Serializes the given {@link AttributeId} to a {@link ByteArraySegment} of length {@link #getKeyLength()}.
         */
        abstract ByteArraySegment serialize(AttributeId attributeId);

        /**
         * Deserializes a {@link ByteArraySegment} of length {@link #getKeyLength()} into an {@link AttributeId}.
         */
        abstract AttributeId deserialize(ByteArraySegment serializedId);

        protected void checkKeyLength(int length) {
            Preconditions.checkArgument(length == getKeyLength(), "Expected Attribute Id length %s, given %s.", getKeyLength(), length);
        }
    }

    @RequiredArgsConstructor
    @Getter
    private static class BufferKeySerializer extends KeySerializer {
        private final int keyLength;

        @Override
        ByteArraySegment serialize(AttributeId attributeId) {
            checkKeyLength(attributeId.byteCount());
            return attributeId.toBuffer();
        }

        @Override
        AttributeId deserialize(ByteArraySegment serializedId) {
            checkKeyLength(serializedId.getLength());
            return AttributeId.from(serializedId.getCopy());
        }
    }

    private static class UUIDKeySerializer extends KeySerializer {
        @Override
        int getKeyLength() {
            return DEFAULT_KEY_LENGTH;
        }

        @Override
        ByteArraySegment serialize(AttributeId attributeId) {
            // Keys are serialized using Unsigned Longs. This ensures that they will be stored in the Attribute Index in their
            // natural order (i.e., the same as the one done by AttributeId.compare()).
            checkKeyLength(attributeId.byteCount());
            ByteArraySegment result = new ByteArraySegment(new byte[DEFAULT_KEY_LENGTH]);
            result.setUnsignedLong(0, attributeId.getBitGroup(0));
            result.setUnsignedLong(Long.BYTES, attributeId.getBitGroup(1));
            return result;
        }

        @Override
        AttributeId deserialize(ByteArraySegment serializedId) {
            checkKeyLength(serializedId.getLength());
            long msb = serializedId.getUnsignedLong(0);
            long lsb = serializedId.getUnsignedLong(Long.BYTES);
            return AttributeId.uuid(msb, lsb);
        }
    }

    //endregion
}
