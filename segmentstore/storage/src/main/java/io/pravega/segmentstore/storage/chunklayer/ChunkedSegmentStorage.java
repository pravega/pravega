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

import com.google.common.annotations.Beta;
import com.google.common.base.Preconditions;
import io.pravega.common.Exceptions;
import io.pravega.common.LoggerHelpers;
import io.pravega.common.Timer;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.concurrent.MultiKeySequentialProcessor;
import io.pravega.common.util.ImmutableDate;
import io.pravega.segmentstore.contracts.SegmentProperties;
import io.pravega.segmentstore.contracts.StreamSegmentExistsException;
import io.pravega.segmentstore.contracts.StreamSegmentInformation;
import io.pravega.segmentstore.contracts.StreamSegmentNotExistsException;
import io.pravega.segmentstore.contracts.StreamSegmentSealedException;
import io.pravega.segmentstore.storage.SegmentHandle;
import io.pravega.segmentstore.storage.SegmentRollingPolicy;
import io.pravega.segmentstore.storage.Storage;
import io.pravega.segmentstore.storage.StorageFullException;
import io.pravega.segmentstore.storage.StorageNotPrimaryException;
import io.pravega.segmentstore.storage.StorageWrapper;
import io.pravega.segmentstore.storage.metadata.ChunkMetadata;
import io.pravega.segmentstore.storage.metadata.ChunkMetadataStore;
import io.pravega.segmentstore.storage.metadata.MetadataTransaction;
import io.pravega.segmentstore.storage.metadata.ReadIndexBlockMetadata;
import io.pravega.segmentstore.storage.metadata.SegmentMetadata;
import io.pravega.segmentstore.storage.metadata.StatusFlags;
import io.pravega.segmentstore.storage.metadata.StorageMetadataWritesFencedOutException;
import io.pravega.shared.NameUtils;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

import javax.annotation.concurrent.GuardedBy;
import java.io.InputStream;
import java.time.Duration;
import java.util.Arrays;
import java.util.ConcurrentModificationException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import static io.pravega.segmentstore.storage.chunklayer.ChunkStorageMetrics.SLTS_CREATE_COUNT;
import static io.pravega.segmentstore.storage.chunklayer.ChunkStorageMetrics.SLTS_CREATE_LATENCY;
import static io.pravega.segmentstore.storage.chunklayer.ChunkStorageMetrics.SLTS_DELETE_COUNT;
import static io.pravega.segmentstore.storage.chunklayer.ChunkStorageMetrics.SLTS_DELETE_LATENCY;
import static io.pravega.shared.MetricsNames.SLTS_STORAGE_USED_BYTES;
import static io.pravega.shared.MetricsNames.SLTS_STORAGE_USED_PERCENTAGE;
import static io.pravega.shared.MetricsNames.STORAGE_METADATA_NUM_CHUNKS;
import static io.pravega.shared.MetricsNames.STORAGE_METADATA_SIZE;
import static io.pravega.shared.NameUtils.INTERNAL_SCOPE_PREFIX;

/**
 * Implements storage for segments using {@link ChunkStorage} and {@link ChunkMetadataStore}.
 * The metadata about the segments is stored in metadataStore using two types of records {@link SegmentMetadata} and {@link ChunkMetadata}.
 * Any changes to layout must be made inside a {@link MetadataTransaction} which will atomically change the records upon
 * {@link MetadataTransaction#commit()}.
 * Detailed design is documented here https://github.com/pravega/pravega/wiki/PDP-34:-Simplified-Tier-2
 */
@Slf4j
@Beta
public class ChunkedSegmentStorage implements Storage, StatsReporter {

    /**
     * Configuration options for this ChunkedSegmentStorage instance.
     */
    @Getter
    private final ChunkedSegmentStorageConfig config;

    /**
     * Metadata store containing all storage data.
     * Initialized by segment container via {@link ChunkedSegmentStorage#bootstrap(SnapshotInfoStore, AbstractTaskQueueManager)} ()}.
     */
    @Getter
    private final ChunkMetadataStore metadataStore;

    /**
     * Underlying {@link ChunkStorage} to use to read and write data.
     */
    @Getter
    private final ChunkStorage chunkStorage;

    /**
     * Storage executor object.
     */
    @Getter
    private final Executor executor;

    /**
     * Tracks whether this instance is closed or not.
     */
    private final AtomicBoolean closed;

    /**
     * Current epoch of the {@link Storage} instance.
     * Initialized by segment container via {@link ChunkedSegmentStorage#initialize(long)}.
     */
    @Getter
    private volatile long epoch;

    /**
     * Id of the current Container.
     * Initialized by segment container via {@link ChunkedSegmentStorage#bootstrap(SnapshotInfoStore, AbstractTaskQueueManager)}.
     */
    @Getter
    private final int containerId;

    /**
     * {@link SystemJournal} that logs all changes to system segment layout so that they can be are used during system bootstrap.
     */
    @Getter
    private final SystemJournal systemJournal;

    /**
     * {@link ReadIndexCache} that has index of chunks by start offset
     */
    @Getter
    private final ReadIndexCache readIndexCache;

    /**
     * Prefix string to use for logging.
     */
    @Getter
    private String logPrefix = "";

    /**
     * Instance of {@link MultiKeySequentialProcessor}.
     */
    private final MultiKeySequentialProcessor<String> taskProcessor;

    @GuardedBy("activeSegments")
    private final HashSet<String> activeRequests = new HashSet<>();

    @Getter
    private final GarbageCollector garbageCollector;

    private final ScheduledFuture<?> reporter;
    private ScheduledFuture<?> storageChecker;

    private AbstractTaskQueueManager<GarbageCollector.TaskInfo> taskQueue;

    private final AtomicBoolean isStorageFull = new AtomicBoolean(false);
    private final AtomicLong storageUsed = new AtomicLong(0);

    /**
     * Creates a new instance of the ChunkedSegmentStorage class.
     *
     * @param containerId   container id.
     * @param chunkStorage  ChunkStorage instance.
     * @param metadataStore Metadata store.
     * @param executor      A {@link ScheduledExecutorService} for async operations.
     * @param config        Configuration options for this ChunkedSegmentStorage instance.
     */
    public ChunkedSegmentStorage(int containerId, ChunkStorage chunkStorage, ChunkMetadataStore metadataStore, ScheduledExecutorService executor, ChunkedSegmentStorageConfig config) {
        this.containerId = containerId;
        this.config = Preconditions.checkNotNull(config, "config");
        this.chunkStorage = Preconditions.checkNotNull(chunkStorage, "chunkStorage");
        this.metadataStore = Preconditions.checkNotNull(metadataStore, "metadataStore");
        this.executor = Preconditions.checkNotNull(executor, "executor");
        this.readIndexCache = new ReadIndexCache(config.getMaxIndexedSegments(),
                config.getMaxIndexedChunks());
        this.taskProcessor = new MultiKeySequentialProcessor<>(this.executor);
        this.garbageCollector = new GarbageCollector(containerId,
                chunkStorage,
                metadataStore,
                config,
                executor,
                System::currentTimeMillis,
                duration -> Futures.delayedFuture(duration, executor));

        this.systemJournal = new SystemJournal(containerId,
                chunkStorage,
                metadataStore,
                garbageCollector,
                config,
                executor);
        this.closed = new AtomicBoolean(false);
        this.reporter = executor.scheduleAtFixedRate(this::report, 1000, 1000, TimeUnit.MILLISECONDS);
        if (config.isSafeStorageSizeCheckEnabled()) {
            this.storageChecker = executor.scheduleAtFixedRate(this::updateStorageStats,
                    config.getSafeStorageSizeCheckFrequencyInSeconds(),
                    config.getSafeStorageSizeCheckFrequencyInSeconds(),
                    TimeUnit.SECONDS);
        }
    }

    /**
     * Initializes the ChunkedSegmentStorage and bootstrap the metadata about storage metadata segments by reading and processing the journal.
     *
     * @param snapshotInfoStore Store that saves {@link SnapshotInfo}.
     */
    public CompletableFuture<Void> bootstrap(SnapshotInfoStore snapshotInfoStore) {

        this.logPrefix = String.format("ChunkedSegmentStorage[%d]", containerId);
        // Now bootstrap
        return this.systemJournal.bootstrap(epoch, snapshotInfoStore);
    }

    /**
     * Concludes and finalizes the boostrap.
     *
     * @param taskQueue  Task queue to use for garbage collection.
     * @return
     */
    public CompletableFuture<Void> finishBootstrap(AbstractTaskQueueManager<GarbageCollector.TaskInfo> taskQueue) {
        this.taskQueue = taskQueue;
        return garbageCollector.initialize(taskQueue);
    }

    @Override
    public void initialize(long containerEpoch) {
        this.epoch = containerEpoch;
    }

    @Override
    public CompletableFuture<SegmentHandle> openWrite(String streamSegmentName) {
        checkInitialized();
        return executeSerialized(() -> {
            val traceId = LoggerHelpers.traceEnter(log, "openWrite", streamSegmentName);
            val timer = new Timer();
            Preconditions.checkNotNull(streamSegmentName, "streamSegmentName");
            log.debug("{} openWrite - started segment={}.", logPrefix, streamSegmentName);
            return tryWith(metadataStore.beginTransaction(false, streamSegmentName),
                    txn -> txn.get(streamSegmentName)
                            .thenComposeAsync(storageMetadata -> {
                                val segmentMetadata = (SegmentMetadata) storageMetadata;
                                checkSegmentExists(streamSegmentName, segmentMetadata);
                                segmentMetadata.checkInvariants();
                                // This segment was created by an older segment store. Need to start a fresh new chunk.
                                final CompletableFuture<Void> f;
                                if (segmentMetadata.getOwnerEpoch() < this.epoch) {
                                    log.debug("{} openWrite - Segment needs ownership change - segment={}.", logPrefix, segmentMetadata.getName());
                                    f = claimOwnership(txn, segmentMetadata);
                                } else {
                                    f = CompletableFuture.completedFuture(null);
                                }
                                return f.thenApplyAsync(v -> {
                                    // If created by newer instance then abort.
                                    checkOwnership(streamSegmentName, segmentMetadata);

                                    // This instance is the owner, return a handle.
                                    val retValue = SegmentStorageHandle.writeHandle(streamSegmentName);
                                    log.debug("{} openWrite - finished segment={} latency={}.", logPrefix, streamSegmentName, timer.getElapsedMillis());
                                    LoggerHelpers.traceLeave(log, "openWrite", traceId, retValue);
                                    return retValue;
                                }, executor);
                            }, executor),
                    executor)
                    .handleAsync( (v, ex) -> {
                        if (null != ex) {
                            log.debug("{} openWrite - exception segment={} latency={}.", logPrefix, streamSegmentName, timer.getElapsedMillis(), ex);
                            handleException(streamSegmentName, ex);
                        }
                        return v;
                    }, executor);
        }, streamSegmentName);
    }

    /**
     * Extracts and returns instance of {@link ChunkedSegmentStorage} for given {@link Storage} instance.
     * @param storage Storage to use.
     * @return Instance of {@link ChunkedSegmentStorage} or null.
     */
    public static ChunkedSegmentStorage getReference(Storage storage) {
        ChunkedSegmentStorage chunkedSegmentStorage = null;
        if (storage instanceof ChunkedSegmentStorage) {
            chunkedSegmentStorage = (ChunkedSegmentStorage) storage;
        }
        if (storage instanceof StorageWrapper) {
            val inner = ((StorageWrapper) storage).getInner();
            if (inner instanceof ChunkedSegmentStorage) {
                chunkedSegmentStorage = (ChunkedSegmentStorage) inner;
            }
        }
        return chunkedSegmentStorage;
    }

    /**
     * Checks ownership and adjusts the length of the segment if required.
     *
     * @param txn             Active {@link MetadataTransaction}.
     * @param segmentMetadata {@link SegmentMetadata} for the segment to change ownership for.
     *                        throws ChunkStorageException    In case of any chunk storage related errors.
     *                        throws StorageMetadataException In case of any chunk metadata store related errors.
     */
    private CompletableFuture<Void> claimOwnership(MetadataTransaction txn, SegmentMetadata segmentMetadata) {
        Preconditions.checkState(!segmentMetadata.isStorageSystemSegment(), "claimOwnership called on system segment %s", segmentMetadata);
        // Get the last chunk
        val lastChunkName = segmentMetadata.getLastChunk();
        final CompletableFuture<Boolean> f;
        val shouldReconcile = !segmentMetadata.isAtomicWrite()
                && shouldAppend()
                && null != lastChunkName;

        if (shouldReconcile) {
            f = txn.get(lastChunkName)
                    .thenComposeAsync(storageMetadata -> {
                        val lastChunk = (ChunkMetadata) storageMetadata;
                        Preconditions.checkState(null != lastChunk, "last chunk metadata must not be null.");
                        Preconditions.checkState(null != lastChunk.getName(), "Name of last chunk must not be null.");
                        log.debug("{} claimOwnership - current last chunk - segment={}, last chunk={}, Length={}.",
                                logPrefix,
                                segmentMetadata.getName(),
                                lastChunk.getName(),
                                lastChunk.getLength());
                        return chunkStorage.getInfo(lastChunkName)
                                .thenApplyAsync(chunkInfo -> {
                                    Preconditions.checkState(chunkInfo != null, "chunkInfo for last chunk must not be null.");
                                    Preconditions.checkState(lastChunk != null, "last chunk metadata must not be null.");
                                    // Adjust its length;
                                    if (chunkInfo.getLength() != lastChunk.getLength()) {
                                        Preconditions.checkState(chunkInfo.getLength() > lastChunk.getLength(),
                                                "Length of last chunk on LTS must be greater than what is in metadata. Chunk=%s length=%s",
                                                lastChunk, chunkInfo.getLength());
                                        // Whatever length you see right now is the final "sealed" length of the last chunk.
                                        val oldLength = segmentMetadata.getLength();
                                        lastChunk.setLength(chunkInfo.getLength());
                                        segmentMetadata.setLength(segmentMetadata.getLastChunkStartOffset() + lastChunk.getLength());
                                        if (!segmentMetadata.isStorageSystemSegment()) {
                                            addBlockIndexEntriesForChunk(txn, segmentMetadata.getName(),
                                                    lastChunk.getName(),
                                                    segmentMetadata.getLastChunkStartOffset(),
                                                    oldLength,
                                                    segmentMetadata.getLength());
                                        }
                                        txn.update(lastChunk);
                                        log.debug("{} claimOwnership - Length of last chunk adjusted - segment={}, last chunk={}, Length={}.",
                                                logPrefix,
                                                segmentMetadata.getName(),
                                                lastChunk.getName(),
                                                chunkInfo.getLength());
                                    }
                                    return true;
                                }, executor)
                                .exceptionally(e -> {
                                    val ex = Exceptions.unwrap(e);
                                    if (ex instanceof ChunkNotFoundException) {
                                        // This probably means that this instance is fenced out and newer instance truncated this segment.
                                        // Try a commit of unmodified data to fail fast.
                                        log.debug("{} claimOwnership - Last chunk was missing, failing fast - segment={}, last chunk={}.",
                                                logPrefix,
                                                segmentMetadata.getName(),
                                                lastChunk.getName());
                                        txn.update(segmentMetadata);
                                        return false;
                                    }
                                    throw new CompletionException(ex);
                                });
                    }, executor);
        } else {
            f = CompletableFuture.completedFuture(true);
        }

        return f.thenComposeAsync(shouldChange -> {
            // Claim ownership.
            // This is safe because the previous instance is definitely not an owner anymore. (even if this instance is no more owner)
            // If this instance is no more owner, then transaction commit will fail.So it is still safe.
            if (shouldChange) {
                segmentMetadata.setOwnerEpoch(this.epoch);
                segmentMetadata.setOwnershipChanged(true);
                segmentMetadata.setAtomicWrites(true);
            }
            // Update and commit
            // If This instance is fenced this update will fail.
            txn.update(segmentMetadata);
            return txn.commit();
        }, executor);
    }

    @Override
    public CompletableFuture<SegmentHandle> create(String streamSegmentName, SegmentRollingPolicy rollingPolicy, Duration timeout) {
        checkInitialized();
        return executeSerialized(() -> {
            val traceId = LoggerHelpers.traceEnter(log, "create", streamSegmentName, rollingPolicy);
            val timer = new Timer();
            log.debug("{} create - started segment={}, rollingPolicy={}.", logPrefix, streamSegmentName, rollingPolicy);
            return tryWith(metadataStore.beginTransaction(false, streamSegmentName), txn -> {
                // Retrieve metadata and make sure it does not exist.
                return txn.get(streamSegmentName)
                        .thenComposeAsync(storageMetadata -> {
                            val oldSegmentMetadata = (SegmentMetadata) storageMetadata;
                            if (null != oldSegmentMetadata) {
                                throw new CompletionException(new StreamSegmentExistsException(streamSegmentName));
                            }

                            // Create a new record.
                            val newSegmentMetadata = SegmentMetadata.builder()
                                    .name(streamSegmentName)
                                    .maxRollinglength(rollingPolicy.getMaxLength() == 0 ? SegmentRollingPolicy.NO_ROLLING.getMaxLength() : rollingPolicy.getMaxLength())
                                    .ownerEpoch(this.epoch)
                                    .build();

                            newSegmentMetadata.setActive(true);
                            newSegmentMetadata.setAtomicWrites(true);
                            txn.create(newSegmentMetadata);
                            // commit.
                            return txn.commit()
                                    .thenApplyAsync(v -> {
                                        val retValue = SegmentStorageHandle.writeHandle(streamSegmentName);
                                        Duration elapsed = timer.getElapsed();
                                        SLTS_CREATE_LATENCY.reportSuccessEvent(elapsed);
                                        SLTS_CREATE_COUNT.inc();
                                        log.debug("{} create - finished segment={}, rollingPolicy={}, latency={}.", logPrefix, streamSegmentName, rollingPolicy, elapsed.toMillis());
                                        LoggerHelpers.traceLeave(log, "create", traceId, retValue);
                                        return retValue;
                                    }, executor);
                        }, executor);
            }, executor)
            .handleAsync((v, e) -> {
                if (null != e) {
                    log.debug("{} create - exception segment={}, rollingPolicy={}, latency={}.", logPrefix, streamSegmentName, rollingPolicy, timer.getElapsedMillis(), e);
                    handleException(streamSegmentName, e);
                }
                return v;
            }, executor);
        }, streamSegmentName);
    }

    private void handleException(String streamSegmentName, Throwable e) {
        val ex = Exceptions.unwrap(e);
        if (ex instanceof StorageMetadataWritesFencedOutException) {
            throw new CompletionException(new StorageNotPrimaryException(streamSegmentName, ex));
        }
        if (ex instanceof ChunkStorageFullException) {
            throw new CompletionException(new StorageFullException(streamSegmentName, ex));
        }
        throw new CompletionException(ex);
    }

    @Override
    public CompletableFuture<Void> write(SegmentHandle handle, long offset, InputStream data, int length, Duration timeout) {
        checkInitialized();
        if (null == handle) {
            return CompletableFuture.failedFuture(new IllegalArgumentException("handle must not be null"));
        }
        if (isStorageFull() && !isSegmentInSystemScope(handle)) {
            return CompletableFuture.failedFuture(new StorageFullException(handle.getSegmentName()));
        }
        return executeSerialized(new WriteOperation(this, handle, offset, data, length), handle.getSegmentName());
    }

    /**
     * Gets whether given segment is a critical storage system segment.
     *
     * @param segmentMetadata Metadata for the segment.
     * @return True if this is a storage system segment.
     */
    boolean isStorageSystemSegment(SegmentMetadata segmentMetadata) {
        return null != systemJournal && segmentMetadata.isStorageSystemSegment();
    }

    @Override
    public CompletableFuture<Void> seal(SegmentHandle handle, Duration timeout) {
        checkInitialized();
        return executeSerialized(() -> {
            val traceId = LoggerHelpers.traceEnter(log, "seal", handle);
            Timer timer = new Timer();
            log.debug("{} seal - started segment={}.", logPrefix, handle.getSegmentName());
            Preconditions.checkNotNull(handle, "handle");
            String streamSegmentName = handle.getSegmentName();
            Preconditions.checkNotNull(streamSegmentName, "streamSegmentName");
            Preconditions.checkArgument(!handle.isReadOnly(), "handle must not be read only. Segment=%s", handle.getSegmentName());

            return tryWith(metadataStore.beginTransaction(false, handle.getSegmentName()), txn ->
                    txn.get(streamSegmentName)
                            .thenComposeAsync(storageMetadata -> {
                                val segmentMetadata = (SegmentMetadata) storageMetadata;
                                // Validate preconditions.
                                checkSegmentExists(streamSegmentName, segmentMetadata);
                                checkOwnership(streamSegmentName, segmentMetadata);

                                // seal if it is not already sealed.
                                if (!segmentMetadata.isSealed()) {
                                    segmentMetadata.setSealed(true);
                                    txn.update(segmentMetadata);
                                    return txn.commit();
                                } else {
                                    return CompletableFuture.completedFuture(null);
                                }
                            }, executor)
                            .thenRunAsync(() -> {
                                log.debug("{} seal - finished segment={} latency={}.", logPrefix, handle.getSegmentName(), timer.getElapsedMillis());
                                LoggerHelpers.traceLeave(log, "seal", traceId, handle);
                            }, executor), executor)
                    .exceptionally( ex -> {
                        log.warn("{} seal - exception segment={} latency={}.", logPrefix, handle.getSegmentName(), timer.getElapsedMillis(), ex);
                        handleException(streamSegmentName, ex);
                        return null;
                    });
        }, handle.getSegmentName());
    }

    @Override
    public CompletableFuture<Void> concat(SegmentHandle targetHandle, long offset, String sourceSegment, Duration timeout) {
        checkInitialized();
        if (null == targetHandle) {
            return CompletableFuture.failedFuture(new IllegalArgumentException("handle must not be null"));
        }
        if (null == sourceSegment) {
            return CompletableFuture.failedFuture(new IllegalArgumentException("sourceSegment must not be null"));
        }
        if (isStorageFull() && !isSegmentInSystemScope(targetHandle)) {
            return CompletableFuture.failedFuture(new StorageFullException(targetHandle.getSegmentName()));
        }

        return executeSerialized(new ConcatOperation(this, targetHandle, offset, sourceSegment), targetHandle.getSegmentName(), sourceSegment);
    }

    boolean shouldAppend() {
        return chunkStorage.supportsAppend() && config.isAppendEnabled();
    }

    /**
     * Defragments the list of chunks for a given segment.
     * It finds eligible consecutive chunks that can be merged together.
     * The sublist such eligible chunks is replaced with single new chunk record corresponding to new large chunk.
     * Conceptually this is like deleting nodes from middle of the list of chunks.
     *
     * @param txn             Active {@link MetadataTransaction}.
     * @param segmentMetadata {@link SegmentMetadata} for the segment to defrag.
     * @param startChunkName  Name of the first chunk to start defragmentation.
     * @param lastChunkName   Name of the last chunk before which to stop defragmentation. (last chunk is not concatenated).
     * @param chunksToDelete  List of chunks to which names of chunks to be deleted are added. It is the responsibility
     *                        of caller to garbage collect these chunks.
     * @param newReadIndexEntries List of new read index entries as a result of defrag.
     * @param defragOffset    Offest where defrag begins. It is start offset of the startChunk.
     *                        throws ChunkStorageException    In case of any chunk storage related errors.
     *                        throws StorageMetadataException In case of any chunk metadata store related errors.
     */
    public CompletableFuture<Void> defrag(MetadataTransaction txn, SegmentMetadata segmentMetadata,
                                           String startChunkName,
                                           String lastChunkName,
                                           List<String> chunksToDelete,
                                           List<ChunkNameOffsetPair> newReadIndexEntries,
                                           long defragOffset) {
        return new DefragmentOperation(this, txn, segmentMetadata, startChunkName, lastChunkName, chunksToDelete, newReadIndexEntries, defragOffset).call();
    }

    @Override
    public CompletableFuture<Void> delete(SegmentHandle handle, Duration timeout) {
        checkInitialized();
        if (null == handle) {
            return CompletableFuture.failedFuture(new IllegalArgumentException("handle must not be null"));
        }
        return executeSerialized(() -> {
            val traceId = LoggerHelpers.traceEnter(log, "delete", handle);
            log.debug("{} delete - started segment={}.", logPrefix, handle.getSegmentName());
            val timer = new Timer();
            val streamSegmentName = handle.getSegmentName();
            return tryWith(metadataStore.beginTransaction(false, streamSegmentName), txn -> txn.get(streamSegmentName)
                    .thenComposeAsync(storageMetadata -> {
                        val segmentMetadata = (SegmentMetadata) storageMetadata;
                        // Check preconditions
                        checkSegmentExists(streamSegmentName, segmentMetadata);
                        checkOwnership(streamSegmentName, segmentMetadata);

                        segmentMetadata.setActive(false);
                        txn.update(segmentMetadata);
                        // Collect garbage
                        return garbageCollector.addSegmentToGarbage(txn.getVersion(), streamSegmentName)
                                .thenComposeAsync(vv -> {
                                    // Commit metadata.
                                    return txn.commit()
                                            .thenRunAsync(() -> {
                                                // Update the read index.
                                                readIndexCache.remove(streamSegmentName);

                                                val elapsed = timer.getElapsed();
                                                SLTS_DELETE_LATENCY.reportSuccessEvent(elapsed);
                                                SLTS_DELETE_COUNT.inc();
                                                log.debug("{} delete - finished segment={}, latency={}.", logPrefix, handle.getSegmentName(), elapsed.toMillis());
                                                LoggerHelpers.traceLeave(log, "delete", traceId, handle);
                                            }, executor);
                                }, executor);
                    }, executor), executor)
                    .exceptionally( ex -> {
                        log.warn("{} delete - exception segment={}, latency={}.", logPrefix, handle.getSegmentName(), timer.getElapsedMillis(), ex);
                        handleException(streamSegmentName, ex);
                        return null;
                    });
        }, handle.getSegmentName());
    }

    @Override
    public CompletableFuture<Void> truncate(SegmentHandle handle, long offset, Duration timeout) {
        checkInitialized();
        if (null == handle) {
            return CompletableFuture.failedFuture(new IllegalArgumentException("handle must not be null"));
        }

        return executeSerialized(new TruncateOperation(this, handle, offset), handle.getSegmentName());
    }

    @Override
    public boolean supportsTruncation() {
        return true;
    }

    @Override
    public boolean supportsAtomicWrites() {
        // Regardless of the actual Storage implementation, ChunkedSegmentStorage guarantees that all calls to #write(...)
        // are atomic.
        return true;
    }

    /**
     * Lists all the segments stored on the storage device.
     * This method looks only at the segment metadata committed to the underlying {@link ChunkMetadataStore}.
     *
     * @return A CompletableFuture that, when completed, will contain an {@link Iterator} that can be used to enumerate and retrieve properties of all the segments.
     * If the operation failed, it will contain the cause of the failure.
     */
    @Override
    public CompletableFuture<Iterator<SegmentProperties>> listSegments() {
        return metadataStore.getAllEntries()
                .thenApplyAsync(storageMetadataStream ->
                        storageMetadataStream
                                .filter(metadata -> metadata instanceof SegmentMetadata && (((SegmentMetadata) metadata).isActive()))
                                .map(metadata -> {
                                    SegmentMetadata segmentMetadata = (SegmentMetadata) metadata;
                                    return (SegmentProperties) StreamSegmentInformation.builder()
                                                                    .name(segmentMetadata.getName())
                                                                    .sealed(segmentMetadata.isSealed())
                                                                    .length(segmentMetadata.getLength())
                                                                    .startOffset(segmentMetadata.getStartOffset())
                                                                    .lastModified(new ImmutableDate(segmentMetadata.getLastModified()))
                                                                    .build();
                                }).iterator(), executor);
    }

    @Override
    public CompletableFuture<SegmentHandle> openRead(String streamSegmentName) {
        checkInitialized();
        return executeParallel(() -> {
            val traceId = LoggerHelpers.traceEnter(log, "openRead", streamSegmentName);
            val timer = new Timer();
            // Validate preconditions and return handle.
            Preconditions.checkNotNull(streamSegmentName, "streamSegmentName");
            log.debug("{} openRead - started segment={}.", logPrefix, streamSegmentName);
            return tryWith(metadataStore.beginTransaction(false, streamSegmentName), txn ->
                            txn.get(streamSegmentName).thenComposeAsync(storageMetadata -> {
                                val segmentMetadata = (SegmentMetadata) storageMetadata;
                                checkSegmentExists(streamSegmentName, segmentMetadata);
                                segmentMetadata.checkInvariants();
                                // This segment was created by an older segment store. Then claim ownership and adjust length.
                                final CompletableFuture<Void> f;
                                if (segmentMetadata.getOwnerEpoch() < this.epoch) {
                                    log.debug("{} openRead - Segment needs ownership change. segment={}.", logPrefix, segmentMetadata.getName());
                                    // In case of a fail-over, length recorded in metadata will be lagging behind its actual length in the storage.
                                    // This can happen with lazy commits that were still not committed at the time of fail-over.
                                    f = executeSerialized(() ->
                                        claimOwnership(txn, segmentMetadata), streamSegmentName);
                                } else {
                                    f = CompletableFuture.completedFuture(null);
                                }
                                return f.thenApplyAsync(v -> {
                                    val retValue = SegmentStorageHandle.readHandle(streamSegmentName);
                                    log.debug("{} openRead - finished segment={} latency={}.", logPrefix, streamSegmentName, timer.getElapsedMillis());
                                    LoggerHelpers.traceLeave(log, "openRead", traceId, retValue);
                                    return retValue;
                                }, executor);
                            }, executor),
                    executor)
                    .handleAsync( (v, ex) -> {
                        if (null != ex) {
                            log.debug("{} openRead - exception segment={} latency={}.", logPrefix, streamSegmentName, timer.getElapsedMillis(), ex);
                            handleException(streamSegmentName, ex);
                        }
                        return v;
                    }, executor);
        }, streamSegmentName);
    }

    @Override
    public CompletableFuture<Integer> read(SegmentHandle handle, long offset, byte[] buffer, int bufferOffset, int length, Duration timeout) {
        checkInitialized();
        return executeParallel(new ReadOperation(this, handle, offset, buffer, bufferOffset, length), handle.getSegmentName());
    }

    @Override
    public CompletableFuture<SegmentProperties> getStreamSegmentInfo(String streamSegmentName, Duration timeout) {
        checkInitialized();
        return executeParallel(() -> {
            val traceId = LoggerHelpers.traceEnter(log, "getStreamSegmentInfo", streamSegmentName);
            val timer = new Timer();
            Preconditions.checkNotNull(streamSegmentName, "streamSegmentName");
            log.debug("{} getStreamSegmentInfo - started segment={}.", logPrefix, streamSegmentName);
            return tryWith(metadataStore.beginTransaction(true, streamSegmentName), txn ->
                    txn.get(streamSegmentName)
                            .thenApplyAsync(storageMetadata -> {
                                SegmentMetadata segmentMetadata = (SegmentMetadata) storageMetadata;
                                checkSegmentExists(streamSegmentName, segmentMetadata);
                                segmentMetadata.checkInvariants();

                                val retValue = StreamSegmentInformation.builder()
                                        .name(streamSegmentName)
                                        .sealed(segmentMetadata.isSealed())
                                        .length(segmentMetadata.getLength())
                                        .startOffset(segmentMetadata.getStartOffset())
                                        .lastModified(new ImmutableDate(segmentMetadata.getLastModified()))
                                        .build();
                                log.debug("{} getStreamSegmentInfo - finished segment={} latency={}.", logPrefix, streamSegmentName, timer.getElapsedMillis());
                                LoggerHelpers.traceLeave(log, "getStreamSegmentInfo", traceId, retValue);
                                return retValue;
                            }, executor),
                    executor)
                    .handleAsync( (v, ex) -> {
                        if (null != ex) {
                            log.debug("{} getStreamSegmentInfo - exception segment={}.", logPrefix, streamSegmentName, ex);
                            handleException(streamSegmentName, ex);
                        }
                        return v;
                    }, executor);
        }, streamSegmentName);
    }

    @Override
    public CompletableFuture<Boolean> exists(String streamSegmentName, Duration timeout) {
        checkInitialized();
        return executeParallel(() -> {
            val traceId = LoggerHelpers.traceEnter(log, "exists", streamSegmentName);
            Preconditions.checkNotNull(streamSegmentName, "streamSegmentName");
            return tryWith(metadataStore.beginTransaction(true, streamSegmentName),
                    txn -> txn.get(streamSegmentName)
                            .thenApplyAsync(storageMetadata -> {
                                SegmentMetadata segmentMetadata = (SegmentMetadata) storageMetadata;
                                val retValue = segmentMetadata != null && segmentMetadata.isActive();
                                LoggerHelpers.traceLeave(log, "exists", traceId, retValue);
                                return retValue;
                            }, executor),
                    executor);
        }, streamSegmentName);
    }

    @Override
    public void report() {
        garbageCollector.report();
        metadataStore.report();
        chunkStorage.report();
        readIndexCache.report();
        // Report storage size.
        ChunkStorageMetrics.DYNAMIC_LOGGER.reportGaugeValue(SLTS_STORAGE_USED_BYTES, storageUsed.get());
        ChunkStorageMetrics.DYNAMIC_LOGGER.reportGaugeValue(SLTS_STORAGE_USED_PERCENTAGE, 100.0 * storageUsed.get() / config.getMaxSafeStorageSize());
    }

    /**
     * Updates storage stats.
     * @return A CompletableFuture that, when completed, will indicate the operation succeeded. If the operation failed,
     *      it will contain the cause of the failure.
     */
    CompletableFuture<Void> updateStorageStats() {
        return chunkStorage.getUsedSpace()
                .thenAcceptAsync(used -> {
                    storageUsed.set(used);
                    boolean isFull = used >= config.getMaxSafeStorageSize();
                    if (isFull) {
                        if (!isStorageFull.get()) {
                            log.warn("{} STORAGE FULL. ENTERING READ ONLY MODE. Any non-critical writes will be rejected.", logPrefix);
                        }
                        log.warn("{} STORAGE FULL - used={} total={}.", logPrefix, used, config.getMaxSafeStorageSize());
                    } else {
                        if (isStorageFull.get()) {
                            log.info("{} STORAGE AVAILABLE. LEAVING READ ONLY MODE. Restoring normal writes", logPrefix);
                        }
                    }
                    isStorageFull.set(isFull);
                }, executor)
                .exceptionally( ex ->     {
                    log.warn("{} updateStorageStats.", logPrefix, ex);
                    return null;
                });
    }

    /**
     * Whether this instance is running under the safe mode or not.
     * @return True if safe mode, False otherwise.
     */
    boolean isSafeMode() {
        return isStorageFull.get();
    }

    @Override
    public void close() {
        close("metadataStore", this.metadataStore);
        close("garbageCollector", this.garbageCollector);
        // taskQueue is per instance so safe to close this here.
        close("taskQueue", this.taskQueue);

        // Do not forget to close ChunkStorage.
        close("chunkStorage", this.chunkStorage);

        this.reporter.cancel(true);
        if (null != this.storageChecker) {
            this.storageChecker.cancel(true);
        }

        this.closed.set(true);
    }

    private void close(String message, AutoCloseable toClose) {
        try {
            log.debug("{} Closing {}", logPrefix, message);
            if (null != toClose) {
                toClose.close();
            }
            log.info("{} Closed {}", logPrefix, message);
        } catch (Exception e) {
            log.error("{} Error while closing {}", logPrefix, message, e);
        }
    }

    /**
     * Adds block index entries for given chunk.
     */
    void addBlockIndexEntriesForChunk(MetadataTransaction txn, String segmentName, String chunkName, long chunkStartOffset, long fromOffset, long toOffset) {
        Preconditions.checkState(chunkStartOffset <= fromOffset,
                "chunkStartOffset must be less than or equal to fromOffset. Segment=%s Chunk=%s chunkStartOffset=%s fromOffset=%s",
                segmentName, chunkName, chunkStartOffset, fromOffset);
        Preconditions.checkState(fromOffset <= toOffset, "fromOffset must be less than or equal to toOffset. Segment=%s Chunk=%s toOffset=%s fromOffset=%s",
                segmentName, chunkName, toOffset, fromOffset);
        val blockSize = config.getIndexBlockSize();
        val startBlock = fromOffset / blockSize;
        // For each block start that falls on this chunk, add block index entry.
        for (long blockStartOffset = startBlock * blockSize; blockStartOffset < toOffset; blockStartOffset += blockSize) {
            if (blockStartOffset >= chunkStartOffset) {
                val blockEntry = ReadIndexBlockMetadata.builder()
                        .name(NameUtils.getSegmentReadIndexBlockName(segmentName, blockStartOffset))
                        .startOffset(chunkStartOffset)
                        .chunkName(chunkName)
                        .status(StatusFlags.ACTIVE)
                        .build();
                txn.create(blockEntry);
                log.debug("{} adding new block index entry segment={}, entry={}.", logPrefix, segmentName, blockEntry);
            }
        }
    }

    /**
     * Delete block index entries for given chunk.
     */
    void deleteBlockIndexEntriesForChunk(MetadataTransaction txn, String segmentName, long startOffset, long endOffset) {
        this.garbageCollector.deleteBlockIndexEntriesForChunk(txn, segmentName, startOffset, endOffset);
    }

    /**
     * Report size related metrics for the given system segment.
     * @param segmentMetadata Segment metadata.
     */
    void reportMetricsForSystemSegment(SegmentMetadata segmentMetadata) {
        val name = segmentMetadata.getName().substring(segmentMetadata.getName().lastIndexOf('/') + 1).replace('$', '_');
        ChunkStorageMetrics.DYNAMIC_LOGGER.reportGaugeValue(STORAGE_METADATA_SIZE + name, segmentMetadata.getLength() - segmentMetadata.getStartOffset());
        ChunkStorageMetrics.DYNAMIC_LOGGER.reportGaugeValue(STORAGE_METADATA_NUM_CHUNKS + name, segmentMetadata.getChunkCount());
    }


    /**
     * Executes the given Callable asynchronously and returns a CompletableFuture that will be completed with the result.
     * The operations are serialized on the segmentNames provided.
     *
     * @param operation    The Callable to execute.
     * @param <R>       Return type of the operation.
     * @param segmentNames The names of the Segments involved in this operation (for sequencing purposes).
     * @return A CompletableFuture that, when completed, will contain the result of the operation.
     * If the operation failed, it will contain the cause of the failure.
     * */
    <R> CompletableFuture<R> executeSerialized(Callable<CompletableFuture<R>> operation, String... segmentNames) {
        Exceptions.checkNotClosed(this.closed.get(), this);
        if (segmentNames.length == 1 && this.systemJournal.isStorageSystemSegment(segmentNames[0])) {
            // To maintain consistency of snapshot, all operations on any of the storage system segments are linearized
            // on the entire group.
            val segments = this.systemJournal.getSystemSegments();
            return this.taskProcessor.add(Arrays.asList(segments), () -> executeExclusive(operation, segments));
        } else {
            return this.taskProcessor.add(Arrays.asList(segmentNames), () -> executeExclusive(operation, segmentNames));
        }
    }

    /**
     * Executes the given Callable asynchronously and exclusively.
     * It returns a CompletableFuture that will be completed with the result.
     * The operations are not allowed to be concurrent.
     *
     * @param operation    The Callable to execute.
     * @param <R>       Return type of the operation.
     * @param segmentNames The names of the Segments involved in this operation (for sequencing purposes).
     * @return A CompletableFuture that, when completed, will contain the result of the operation.
     * If the operation failed, it will contain the cause of the failure.
     * */
    private <R> CompletableFuture<R> executeExclusive(Callable<CompletableFuture<R>> operation, String... segmentNames) {
        val shouldRelease = new AtomicBoolean(false);
        acquire(segmentNames);
        shouldRelease.set(true);
        return CompletableFuture.completedFuture(null).thenComposeAsync(v -> {
            Exceptions.checkNotClosed(this.closed.get(), this);
            try {
                return operation.call();
            } catch (CompletionException e) {
                throw new CompletionException(Exceptions.unwrap(e));
            } catch (Exception e) {
                throw new CompletionException(e);
            }
        }, this.executor)
        .whenCompleteAsync((v, e) -> {
            if (shouldRelease.get()) {
                release(segmentNames);
            }
        }, this.executor);
    }

    /**
     * Adds the given segments to the exclusion list.
     * If concurrent requests are detected then throws {@link ConcurrentModificationException}.
     *
     * @param segmentNames The names of the Segments involved in this operation (for sequencing purposes).
     */
    private void acquire(String... segmentNames) {
        synchronized (activeRequests) {
            for (String segmentName : segmentNames) {
                if (activeRequests.contains(segmentName)) {
                    log.error("{} Concurrent modifications for Segment={}", logPrefix, segmentName);
                    throw new ConcurrentModificationException(String.format("Concurrent modifications not allowed. Segment=%s", segmentName));
                }
            }
            // Now that we have validated, mark all keys as "locked".
            for (String segmentName : segmentNames) {
                activeRequests.add(segmentName);
            }
        }
    }

    /**
     * Removes the given segments from exclusion list.
     *
     * @param segmentNames The names of the Segments involved in this operation (for sequencing purposes).
     */
    private void release(String... segmentNames) {
        synchronized (activeRequests) {
            for (String segmentName : segmentNames) {
                activeRequests.remove(segmentName);
            }
        }
    }

    /**
     * Executes the given Callable asynchronously and concurrently.
     * It returns a CompletableFuture that will be completed with the result.
     * The operations are assumed to be independent of other operations.
     *
     * @param operation    The Callable to execute.
     * @param <R>       Return type of the operation.
     * @param segmentNames The names of the Segments involved in this operation (for sequencing purposes).
     * @return A CompletableFuture that, when completed, will contain the result of the operation.
     * If the operation failed, it will contain the cause of the failure.
     * */
    <R> CompletableFuture<R> executeParallel(Callable<CompletableFuture<R>> operation, String... segmentNames) {
        return CompletableFuture.completedFuture(null).thenComposeAsync(v -> {
            Exceptions.checkNotClosed(this.closed.get(), this);
            try {
                return operation.call();
            } catch (CompletionException e) {
                throw new CompletionException(Exceptions.unwrap(e));
            } catch (Exception e) {
                throw new CompletionException(e);
            }
        }, this.executor);
    }

    static <T extends AutoCloseable, R> CompletableFuture<R> tryWith(T closeable, Function<T, CompletableFuture<R>> function, Executor executor) {
        return function.apply(closeable)
                    .whenCompleteAsync((v, ex) -> {
                        try {
                            closeable.close();
                        } catch (Exception e) {
                            throw new CompletionException(e);
                        }
                    }, executor);
    }

    final void checkSegmentExists(String streamSegmentName, SegmentMetadata segmentMetadata) {
        if (null == segmentMetadata || !segmentMetadata.isActive()) {
            throw new CompletionException(new StreamSegmentNotExistsException(streamSegmentName));
        }
    }

    final void checkOwnership(String streamSegmentName, SegmentMetadata segmentMetadata) {
        if (segmentMetadata.getOwnerEpoch() > this.epoch) {
            throw new CompletionException(new StorageNotPrimaryException(streamSegmentName));
        }
    }

    final void checkNotSealed(String streamSegmentName, SegmentMetadata segmentMetadata) {
        if (segmentMetadata.isSealed()) {
            throw new CompletionException(new StreamSegmentSealedException(streamSegmentName));
        }
    }

    boolean isStorageFull() {
        return config.isSafeStorageSizeCheckEnabled() && isStorageFull.get();
    }

    boolean isSegmentInSystemScope(SegmentHandle handle) {
        return handle.getSegmentName().startsWith(INTERNAL_SCOPE_PREFIX);
    }

    private void checkInitialized() {
        Preconditions.checkState(0 != this.epoch, "epoch must not be zero");
        Preconditions.checkState(!closed.get(), "ChunkedSegmentStorage instance must not be closed");
    }

    String getNewChunkName(String segmentName, long offset) {
        return NameUtils.getSegmentChunkName(segmentName, getEpoch(), offset);
    }
}
