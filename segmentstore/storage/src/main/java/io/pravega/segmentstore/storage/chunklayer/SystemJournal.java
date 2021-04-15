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

import com.google.common.base.Preconditions;
import io.pravega.common.Exceptions;
import io.pravega.common.ObjectBuilder;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.concurrent.MultiKeySequentialProcessor;
import io.pravega.common.io.serialization.RevisionDataInput;
import io.pravega.common.io.serialization.RevisionDataOutput;
import io.pravega.common.io.serialization.VersionedSerializer;
import io.pravega.common.util.ByteArraySegment;
import io.pravega.segmentstore.storage.metadata.ChunkMetadata;
import io.pravega.segmentstore.storage.metadata.ChunkMetadataStore;
import io.pravega.segmentstore.storage.metadata.MetadataTransaction;
import io.pravega.segmentstore.storage.metadata.SegmentMetadata;
import io.pravega.shared.NameUtils;
import java.io.ByteArrayInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

import static com.google.common.base.Strings.emptyToNull;
import static com.google.common.base.Strings.nullToEmpty;

/**
 * This class implements system journaling functionality for critical storage system segments which is useful for bootstrap after failover.
 * It records any layout changes to storage system segments.
 * Storage system segments are the segments that the storage subsystem uses to store all metadata.
 * This creates a circular dependency while reading or writing the data about these segments from the metadata segments.
 * System journal is a mechanism to break this circular dependency by having independent log of all layout changes to system segments.
 * During bootstrap all the system journal files are read and processed to re-create the state of the storage system segments.
 * Currently only two actions are considered viz. Addition of new chunks {@link SystemJournal.ChunkAddedRecord} and truncation of segments
 * {@link SystemJournal.TruncationRecord}.
 * In addition to these two records, log also contains system snapshot records {@link SystemJournal.SystemSnapshotRecord} which contains the state
 * of each storage system segments ({@link SystemJournal.SegmentSnapshotRecord}) after replaying all available logs at the time of snapshots.
 * These snapshot records help avoid replaying entire log evey time. Each container instance records snapshot immediately after bootstrap.
 * To avoid data corruption, each instance writes to its own distinct log file/object.
 * The bootstrap algorithm also correctly ignores invalid log entries written by running instance which is no longer owner of the given container.
 * To prevent applying partial changes resulting from unexpected crash, the log records are written as {@link SystemJournal.SystemJournalRecordBatch}.
 * In such cases either a full batch is read and applied completely or no records in the batch are applied.
 */
@Slf4j
public class SystemJournal {
    /**
     * The key name to use when linearizing commits of {@link SystemJournal.SystemJournalRecordBatch}.
     */
    private static final String LOCK_KEY_NAME = "SingleThreadedLock";

    /**
     * Serializer for {@link SystemJournal.SystemJournalRecordBatch}.
     */
    private static final SystemJournalRecordBatch.SystemJournalRecordBatchSerializer BATCH_SERIALIZER = new SystemJournalRecordBatch.SystemJournalRecordBatchSerializer();

    /**
     * Serializer for {@link SystemJournal.SystemSnapshotRecord}.
     */
    private static final SystemSnapshotRecord.Serializer SYSTEM_SNAPSHOT_SERIALIZER = new SystemSnapshotRecord.Serializer();

    @Getter
    private final ChunkStorage chunkStorage;

    @Getter
    private final ChunkMetadataStore metadataStore;

    /**
     * Epoch of the current instance.
     */
    @Getter
    private volatile long epoch;

    /**
     * Container id of the owner container.
     */
    @Getter
    private final int containerId;

    /**
     * Index of current journal file.
     */
    @Getter
    final private AtomicInteger currentFileIndex = new AtomicInteger();

    /**
     * Index of current snapshot.
     */
    final private AtomicLong currentSnapshotIndex = new AtomicLong();

    /**
     * Indicates whether new chunk is required.
     */
    final private AtomicBoolean newChunkRequired = new AtomicBoolean();

    /**
     * Last successful snapshot.
     */
    final private AtomicReference<SystemSnapshotRecord> lastSavedSystemSnapshot = new AtomicReference<>();

    /**
     * Id of the last saved snapshot.
     */
    final private AtomicLong lastSavedSystemSnapshotId = new AtomicLong();

    /**
     * Most recently saved {@link SnapshotInfo} instance.
     */
    final private AtomicReference<SnapshotInfo> lastSavedSnapshotInfo = new AtomicReference<>();

    /**
     * Time when last snapshot was saved.
     */
    final private AtomicLong lastSavedSnapshotTime = new AtomicLong();

    /**
     * The current attempt for writing system snapshot.
     */
    final private AtomicInteger recordsSinceSnapshot = new AtomicInteger();

    /**
     * SnapshotInfoStore .
     */
    private volatile SnapshotInfoStore snapshotInfoStore;

    /**
     * String prefix for all system segments.
     */
    @Getter
    private final String systemSegmentsPrefix;

    /**
     * System segments to track.
     */
    @Getter
    private final String[] systemSegments;

    /**
     * Offset at which next log will be written.
     */
    final private AtomicLong systemJournalOffset = new AtomicLong();

    /**
     * Handle to current journal file.
     */
    final private AtomicReference<ChunkHandle> currentHandle = new AtomicReference<>();

    /**
     * Configuration {@link ChunkedSegmentStorageConfig} for the {@link ChunkedSegmentStorage}.
     */
    @Getter
    private final ChunkedSegmentStorageConfig config;

    private final GarbageCollector garbageCollector;

    private final Supplier<Long> currentTimeSupplier;

    private final AtomicBoolean reentryGuard = new AtomicBoolean();

    private final Executor executor;

    /**
     * Instance of {@link MultiKeySequentialProcessor} used for linearizing commits.
     */
    private final MultiKeySequentialProcessor<String> taskProcessor;

    /**
     * Constructs an instance of {@link SystemJournal}.
     *
     * @param containerId         Container id of the owner container.
     * @param chunkStorage        ChunkStorage instance to use for writing all logs.
     * @param metadataStore       ChunkMetadataStore for owner container.
     * @param garbageCollector    GarbageCollection instance.
     * @param currentTimeSupplier Function that supplies current time in milliseconds.
     * @param config              Configuration options for this ChunkedSegmentStorage instance.
     * @param executor            Executor to use.
     */
    public SystemJournal(int containerId, ChunkStorage chunkStorage, ChunkMetadataStore metadataStore,
                         GarbageCollector garbageCollector,
                         Supplier<Long> currentTimeSupplier,
                         ChunkedSegmentStorageConfig config,
                         Executor executor) {
        this.chunkStorage = Preconditions.checkNotNull(chunkStorage, "chunkStorage");
        this.metadataStore = Preconditions.checkNotNull(metadataStore, "metadataStore");
        this.config = Preconditions.checkNotNull(config, "config");
        this.garbageCollector = Preconditions.checkNotNull(garbageCollector, "garbageCollector");
        this.containerId = containerId;
        this.systemSegments = getChunkStorageSystemSegments(containerId);
        this.systemSegmentsPrefix = NameUtils.INTERNAL_SCOPE_NAME;
        this.currentTimeSupplier = Preconditions.checkNotNull(currentTimeSupplier, "currentTimeSupplier");
        this.executor = Preconditions.checkNotNull(executor, "executor");
        this.taskProcessor = new MultiKeySequentialProcessor<>(this.executor);
    }

    /**
     * Constructs an instance of {@link SystemJournal}.
     *
     * @param containerId      Container id of the owner container.
     * @param chunkStorage     ChunkStorage instance to use for writing all logs.
     * @param metadataStore    ChunkMetadataStore for owner container.
     * @param garbageCollector GarbageCollection instance.
     * @param config           Configuration options for this ChunkedSegmentStorage instance.
     * @param executor         Executor to use.
     */
    public SystemJournal(int containerId, ChunkStorage chunkStorage, ChunkMetadataStore metadataStore,
                         GarbageCollector garbageCollector,
                         ChunkedSegmentStorageConfig config,
                         Executor executor) {
        this(containerId, chunkStorage, metadataStore, garbageCollector, System::currentTimeMillis, config, executor);
    }

    /**
     * Initializes this instance.
     *
     * @throws Exception Exception if any.
     */
    public void initialize() throws Exception {
        if (chunkStorage.supportsAppend()) {
            chunkStorage.create(getSystemJournalChunkName()).get();
        }
    }

    /**
     * Bootstrap the metadata about storage metadata segments by reading and processing the journal.
     *
     * @param epoch             Epoch of the current container instance.
     * @param snapshotInfoStore {@link SnapshotInfoStore} that stores {@link SnapshotInfo}.
     * @throws Exception Exception in case of any error.
     */
    public CompletableFuture<Void> bootstrap(long epoch, SnapshotInfoStore snapshotInfoStore) throws Exception {
        this.epoch = epoch;
        this.snapshotInfoStore = Preconditions.checkNotNull(snapshotInfoStore, "snapshotInfoStore");
        Preconditions.checkState(!reentryGuard.getAndSet(true), "bootstrap called multiple times.");

        // Start a transaction
        val txn = metadataStore.beginTransaction(false, getSystemSegments());

        // Keep track of offsets at which chunks were added to the system segments.
        val chunkStartOffsets = new HashMap<String, Long>();

        // Keep track of offsets at which system segments were truncated.
        // We don't need to apply each truncate operation, only need to apply the final truncate offset.
        val finalTruncateOffsets = new HashMap<String, Long>();
        val finalFirstChunkStartsAtOffsets = new HashMap<String, Long>();

        // Keep track of already processed records.
        val visitedRecords = new HashSet<SystemJournalRecord>();
        val snapshotSaved = new AtomicBoolean();

        // Step 1: Create metadata records for system segments from latest snapshot.
        return findLatestSnapshot()
                .thenComposeAsync(snapshot ->
                                applySystemSnapshotRecord(txn, chunkStartOffsets, snapshot),
                        executor)
                .thenComposeAsync(latestSnapshot -> {
                    // Step 2: For each epoch, find the corresponding system journal files, process them and apply operations recorded.
                    return applySystemLogOperations(txn, latestSnapshot, chunkStartOffsets, finalTruncateOffsets, finalFirstChunkStartsAtOffsets, visitedRecords);
                }, executor)
                .thenComposeAsync(v -> {
                    // Step 3: Adjust the length of the last chunk.
                    if (config.isLazyCommitEnabled()) {
                        return adjustLastChunkLengths(txn);
                    } else {
                        return CompletableFuture.completedFuture(null);
                    }
                }, executor)
                .thenComposeAsync(v -> {
                    // Step 4: Apply the truncate offsets.
                    return applyFinalTruncateOffsets(txn, finalTruncateOffsets, finalFirstChunkStartsAtOffsets);
                }, executor)
                .thenComposeAsync(v -> {
                    // Step 5: Validate and save a snapshot.
                    return validateAndSaveSnapshot(txn, true, config.isSelfCheckEnabled())
                            .thenComposeAsync(saved -> {
                                if (saved) {
                                    snapshotSaved.set(true);
                                    return writeSnapshotInfo(lastSavedSystemSnapshotId.get());
                                } else {
                                    return CompletableFuture.completedFuture(null);
                                }
                            }, executor);
                }, executor)
                .thenAcceptAsync(v -> {
                    // Step 6: Check invariants.
                    Preconditions.checkState(currentFileIndex.get() == 0, "currentFileIndex must be zero");
                    Preconditions.checkState(systemJournalOffset.get() == 0, "systemJournalOffset must be zero");
                    Preconditions.checkState(newChunkRequired.get(), "newChunkRequired must be true");
                    if (snapshotSaved.get()) {
                        Preconditions.checkState(lastSavedSystemSnapshot.get() != null, "lastSavedSystemSnapshot must be initialized");
                    }
                }, executor)
                .thenComposeAsync(v -> {
                    // Step 7: Finally commit all data.
                    return txn.commit(true, true);
                }, executor)
                .whenCompleteAsync((v, e) -> txn.close(), executor);
    }

    /**
     * Checks if snapshot file exists for given snapshotId.
     */
    private CompletableFuture<Void> checkSnapshotFileExists(long snapshotId) {
        if (getConfig().isSelfCheckEnabled()) {
            val snapshotFileName = NameUtils.getSystemJournalSnapshotFileName(containerId, epoch, snapshotId);
            return chunkStorage.exists(snapshotFileName)
                    .thenAcceptAsync(exists -> Preconditions.checkState(exists, "Snapshot file must exist"), executor);
        } else {
            return CompletableFuture.completedFuture(null);
        }
    }

    /**
     * Commits a given system log record to the underlying log chunk.
     *
     * @param record Record to persist.
     * @return A CompletableFuture that, when completed, will indicate that the operation completed.
     * If the operation failed, it will be completed with the appropriate exception. Notable Exceptions:
     * {@link ChunkStorageException} In case of I/O related exceptions.
     */
    public CompletableFuture<Void> commitRecord(SystemJournalRecord record) {
        Preconditions.checkArgument(null != record, "record must not be null");
        return commitRecords(Collections.singletonList(record));
    }

    /**
     * Commits a given list of system log records to the underlying log chunk.
     *
     * @param records List of records to log to.
     * @return A CompletableFuture that, when completed, will indicate that the operation completed.
     * If the operation failed, it will be completed with the appropriate exception. Notable Exceptions:
     * {@link ChunkStorageException} In case of I/O related exceptions.
     */
    public CompletableFuture<Void> commitRecords(Collection<SystemJournalRecord> records) {
        Preconditions.checkArgument(null != records, "records must not be null");
        Preconditions.checkArgument(records.size() > 0, "records must not be empty");

        return executeSerialized(() -> generateSnapshotIfRequired()
                .thenComposeAsync(v -> writeSnapshotInfoIfRequired(), executor)
                .thenComposeAsync(v -> writeRecordBatch(records), executor));
    }

    /**
     * Writes a single batch of {@link SystemJournalRecord}
     */
    private CompletableFuture<Void> writeRecordBatch(Collection<SystemJournalRecord> records) {
        val batch = SystemJournalRecordBatch.builder().systemJournalRecords(records).build();
        ByteArraySegment bytes;
        try {
            bytes = BATCH_SERIALIZER.serialize(batch);
        } catch (IOException e) {
            return CompletableFuture.failedFuture(new ChunkStorageException(getSystemJournalChunkName(), "Unable to serialize", e));
        }
        // Persist
        // Repeat until not successful.
        val attempt = new AtomicInteger();
        val done = new AtomicBoolean();
        return Futures.loop(
                () -> !done.get() && attempt.get() < config.getMaxJournalWriteAttempts(),
                () -> writeToJournal(bytes)
                        .thenAcceptAsync(v -> {
                            log.debug("SystemJournal[{}] Logging system log records - file={}, batch={}.",
                                    containerId, currentHandle.get().getChunkName(), batch);
                            recordsSinceSnapshot.incrementAndGet();
                            done.set(true);
                        }, executor)
                        .handleAsync((v, e) -> {
                            attempt.incrementAndGet();
                            if (e != null) {
                                val ex = Exceptions.unwrap(e);
                                // Throw if retries exhausted.
                                if (attempt.get() >= config.getMaxJournalWriteAttempts()) {
                                    throw new CompletionException(ex);
                                }
                                // In case of partial write during previous failure, this time we'll get InvalidOffsetException.
                                // In that case we start a new journal file and retry.
                                if (ex instanceof InvalidOffsetException) {
                                    return null;
                                }
                                if (ex instanceof ChunkStorageException) {
                                    return null;
                                }
                                // Unknown Error
                                throw new CompletionException(ex);
                            } else {
                                // No exception just return the value.
                                return v;
                            }
                        }, executor)
                        .thenAcceptAsync(v -> {
                            // Add a new log file if required.
                            if (!chunkStorage.supportsAppend() || !config.isAppendEnabled() || !done.get()) {
                                newChunkRequired.set(true);
                            }
                        }, executor),
                executor);
    }

    /**
     * Generate a snapshot if required.
     */
    private CompletableFuture<Void> generateSnapshotIfRequired() {
        // Generate a snapshot when threshold for either time or number batches is reached.
        if (recordsSinceSnapshot.get() > config.getMaxJournalUpdatesPerSnapshot() ||
                currentTimeSupplier.get() - lastSavedSnapshotTime.get() > config.getJournalSnapshotInfoUpdateFrequency().toMillis()) {
            // Write a snapshot.
            val txn = metadataStore.beginTransaction(true, getSystemSegments());
            return validateAndSaveSnapshot(txn, true, config.isSelfCheckEnabled())
                    .thenAcceptAsync(saved -> {
                        txn.close();
                        if (saved) {
                            recordsSinceSnapshot.set(0);
                            // Always start a new journal after snapshot
                            newChunkRequired.set(true);
                        }
                    }, executor)
                    .exceptionally(e -> {
                        log.error("SystemJournal[{}] Error while creating snapshot", containerId, e);
                        return null;
                    });
        } else {
            return CompletableFuture.completedFuture(null);
        }
    }

    /**
     * Write snapshot info if required.
     */
    private CompletableFuture<Void> writeSnapshotInfoIfRequired() {
        // Save if we have generated newer snapshot since last time we saved.
        if (lastSavedSystemSnapshot.get() != null && lastSavedSnapshotInfo.get() != null
                && lastSavedSnapshotInfo.get().getSnapshotId() < lastSavedSystemSnapshotId.get()) {
            return writeSnapshotInfo(lastSavedSystemSnapshotId.get());
        } else {
            return CompletableFuture.completedFuture(null);
        }
    }

    /**
     * Saves the {@link SnapshotInfo} for given snapshotId in {@link SnapshotInfoStore}.
     */
    private CompletableFuture<Void> writeSnapshotInfo(long snapshotId) {
        return checkSnapshotFileExists(snapshotId)
                .thenComposeAsync(v -> {
                    val info = SnapshotInfo.builder()
                            .snapshotId(snapshotId)
                            .epoch(epoch)
                            .build();
                    return snapshotInfoStore.writeSnapshotInfo(info)
                            .thenAcceptAsync(v1 -> {
                                lastSavedSnapshotInfo.set(info);
                                lastSavedSnapshotTime.set(currentTimeSupplier.get());
                            }, executor)
                            .exceptionally(e -> {
                                log.error("Unable to persist snapshot info.{}", currentSnapshotIndex, e);
                                return null;
                            });
                }, executor);
    }

    /**
     * Find and apply latest snapshot.
     */
    private CompletableFuture<SystemSnapshotRecord> findLatestSnapshot() {
        // Step 1: Read snapshot info.
        return snapshotInfoStore.readSnapshotInfo()
                .thenComposeAsync(persisted -> {
                    if (null != persisted) {
                        // Step 2: Validate.
                        val snapshotFileName = NameUtils.getSystemJournalSnapshotFileName(containerId, persisted.getEpoch(), persisted.getSnapshotId());
                        CompletableFuture<Void> validationFuture;
                        if (getConfig().isSelfCheckEnabled()) {
                            validationFuture = chunkStorage.exists(snapshotFileName)
                                    .thenAcceptAsync(exists -> Preconditions.checkState(exists, "File pointed by SnapshotInfo must exist"), executor);
                        } else {
                            validationFuture = CompletableFuture.completedFuture(null);
                        }

                        // Step 3: Read contents.
                        return validationFuture
                                .thenComposeAsync(v ->
                                    getContents(snapshotFileName)
                                        .thenApplyAsync(snapshotContents -> {
                                            // Step 4: Deserialize and return.
                                            try {
                                                val systemSnapshot = SYSTEM_SNAPSHOT_SERIALIZER.deserialize(snapshotContents);
                                                log.info("SystemJournal[{}] Done finding snapshots. Snapshot found and parsed. {}", containerId, persisted);
                                                return systemSnapshot;
                                            } catch (Exception e) {
                                                val ex = Exceptions.unwrap(e);
                                                if (ex instanceof EOFException) {
                                                    log.warn("SystemJournal[{}] Incomplete snapshot found, skipping {}.", containerId, persisted, e);
                                                } else if (ex instanceof ChunkNotFoundException) {
                                                    log.warn("SystemJournal[{}] Missing snapshot, skipping {}.", containerId, persisted, e);
                                                } else {
                                                    log.error("SystemJournal[{}] Error with snapshot, skipping {}.", containerId, persisted, e);
                                                    throw new CompletionException(e);
                                                }
                                            }
                                            return null;
                                        }, executor),
                                executor);
                    } else {
                        return CompletableFuture.completedFuture(null);
                    }
                }, executor);
    }

    /**
     * Recreates in-memory state from the given snapshot record.
     */
    private CompletableFuture<SystemSnapshotRecord> applySystemSnapshotRecord(MetadataTransaction txn, HashMap<String, Long> chunkStartOffsets, SystemSnapshotRecord systemSnapshot) {
        if (null != systemSnapshot) {
            log.debug("SystemJournal[{}] Processing system log snapshot {}.", containerId, systemSnapshot);
            // Initialize the segments and their chunks.
            for (SegmentSnapshotRecord segmentSnapshot : systemSnapshot.segmentSnapshotRecords) {
                // Update segment data.
                segmentSnapshot.segmentMetadata.setActive(true)
                        .setOwnershipChanged(true)
                        .setStorageSystemSegment(true);
                segmentSnapshot.segmentMetadata.setOwnerEpoch(epoch);

                // Add segment data.
                txn.create(segmentSnapshot.segmentMetadata);

                // make sure that the record is marked pinned.
                txn.markPinned(segmentSnapshot.segmentMetadata);

                // Add chunk metadata and keep track of start offsets for each chunk.
                long offset = segmentSnapshot.segmentMetadata.getFirstChunkStartOffset();
                for (ChunkMetadata metadata : segmentSnapshot.chunkMetadataCollection) {
                    txn.create(metadata);

                    // make sure that the record is marked pinned.
                    txn.markPinned(metadata);

                    chunkStartOffsets.put(metadata.getName(), offset);
                    offset += metadata.getLength();
                }
            }
        } else {
            // Initialize with default values.
            for (String systemSegment : systemSegments) {
                SegmentMetadata segmentMetadata = SegmentMetadata.builder()
                        .name(systemSegment)
                        .ownerEpoch(epoch)
                        .maxRollinglength(config.getDefaultRollingPolicy().getMaxLength())
                        .build();
                segmentMetadata.setActive(true)
                        .setOwnershipChanged(true)
                        .setStorageSystemSegment(true);
                segmentMetadata.checkInvariants();
                txn.create(segmentMetadata);
                txn.markPinned(segmentMetadata);
            }
        }
        log.debug("SystemJournal[{}] Done applying snapshots.", containerId);

        // Validate
        return checkInvariants(systemSnapshot)
                .thenComposeAsync(v ->
                        validateSystemSnapshotExistsInTxn(txn, systemSnapshot), executor)
                .thenApplyAsync(v -> systemSnapshot, executor);
    }

    /**
     * Validate that all information in given snapshot exists in transaction.
     */
    private CompletableFuture<Void> validateSystemSnapshotExistsInTxn(MetadataTransaction txn, SystemSnapshotRecord systemSnapshot) {
        if (null == systemSnapshot) {
            return CompletableFuture.completedFuture(null);
        } else {
            // For each segment in snapshot
            return Futures.loop(
                    systemSnapshot.getSegmentSnapshotRecords(),
                    segmentSnapshot ->
                        txn.get(segmentSnapshot.segmentMetadata.getKey())
                            .thenApplyAsync(metadata -> {
                                Preconditions.checkState(null != metadata);
                                return metadata;
                            }, executor)
                            .thenComposeAsync(v ->
                                // For each chunk in the segment
                                Futures.loop(
                                        segmentSnapshot.getChunkMetadataCollection(),
                                        m -> txn.get(m.getKey())
                                                .thenApplyAsync(metadata -> {
                                                    Preconditions.checkState(null != metadata);
                                                    return true;
                                                }, executor),
                                        executor),
                            executor)
                            .thenComposeAsync(vv -> validateSegmentRecordInTxn(txn, segmentSnapshot.segmentMetadata.getKey()), executor)
                            .thenApplyAsync(v -> true, executor),
                    executor);
        }
    }

    /**
     * Validate that all information for given segment in given snapshot exists in transaction.
     */
    private CompletableFuture<Void> validateSegmentRecordInTxn(MetadataTransaction txn, String segmentName) {
        return txn.get(segmentName)
                .thenComposeAsync(m -> {
                    val segmentMetadata = (SegmentMetadata) m;
                    Preconditions.checkState(null != segmentMetadata);
                    val chunkName = new AtomicReference<>(segmentMetadata.getFirstChunk());
                    return Futures.loop(
                            () -> chunkName.get() != null,
                            () -> txn.get(chunkName.get())
                                    .thenApplyAsync(mm -> {
                                        val chunkMetadata = (ChunkMetadata) mm;
                                        Preconditions.checkState(null != chunkMetadata);
                                        chunkName.set(chunkMetadata.getNextChunk());
                                        return null;
                                    }, executor),
                            executor);
                }, executor);
    }

    /**
     * Check invariants for given {@link SystemSnapshotRecord}.
     */
    private CompletableFuture<Void> checkInvariants(SystemSnapshotRecord systemSnapshot) {
        if (null != systemSnapshot) {
            for (val segmentSnapshot : systemSnapshot.getSegmentSnapshotRecords()) {
                segmentSnapshot.segmentMetadata.checkInvariants();
                Preconditions.checkState(segmentSnapshot.segmentMetadata.isStorageSystemSegment());
                Preconditions.checkState(segmentSnapshot.segmentMetadata.getChunkCount() == segmentSnapshot.chunkMetadataCollection.size());
                if (segmentSnapshot.chunkMetadataCollection.size() == 0) {
                    Preconditions.checkState(segmentSnapshot.segmentMetadata.getFirstChunk() == null);
                    Preconditions.checkState(segmentSnapshot.segmentMetadata.getLastChunk() == null);
                } else if (segmentSnapshot.chunkMetadataCollection.size() == 1) {
                    Preconditions.checkState(segmentSnapshot.segmentMetadata.getFirstChunk() != null);
                    Preconditions.checkState(segmentSnapshot.segmentMetadata.getFirstChunk().equals(segmentSnapshot.segmentMetadata.getLastChunk()));
                } else {
                    Preconditions.checkState(segmentSnapshot.segmentMetadata.getFirstChunk() != null);
                    Preconditions.checkState(segmentSnapshot.segmentMetadata.getLastChunk() != null);
                    Preconditions.checkState(!segmentSnapshot.segmentMetadata.getFirstChunk().equals(segmentSnapshot.segmentMetadata.getLastChunk()));
                }
                ChunkMetadata previous = null;
                for (val metadata : segmentSnapshot.getChunkMetadataCollection()) {
                    if (previous != null) {
                        Preconditions.checkState(previous.getNextChunk().equals(metadata.getName()));
                    }
                    previous = metadata;
                }
            }
        }
        return CompletableFuture.completedFuture(null);
    }

    /**
     * Read contents from file.
     */
    private CompletableFuture<byte[]> getContents(String snapshotFile) {
        val isReadDone = new AtomicBoolean();
        val attempt = new AtomicInteger();
        val lastException = new AtomicReference<Throwable>();
        val retValue = new AtomicReference<byte[]>();
        // Try config.getMaxJournalReadAttempts() times.
        return Futures.loop(
                () -> attempt.get() < config.getMaxJournalReadAttempts() && !isReadDone.get(),
                () -> chunkStorage.getInfo(snapshotFile)
                        .thenComposeAsync(info -> {
                            val h = ChunkHandle.readHandle(snapshotFile);
                            // Allocate buffer to read into.
                            retValue.set(new byte[Math.toIntExact(info.getLength())]);

                            val fromOffset = new AtomicLong();
                            val remaining = new AtomicInteger(retValue.get().length);

                            // Continue until there is still data remaining to be read.
                            return Futures.loop(
                                        () -> remaining.get() > 0,
                                        () -> chunkStorage.read(h, fromOffset.get(), remaining.get(), retValue.get(), Math.toIntExact(fromOffset.get())),
                                        bytesRead -> {
                                            remaining.addAndGet(-bytesRead);
                                            fromOffset.addAndGet(bytesRead);
                                        },
                                        executor)
                                    .thenAcceptAsync(v -> isReadDone.set(true), executor);

                        }, executor)
                        .handleAsync((v, e) -> {
                            attempt.incrementAndGet();
                            if (e != null) {
                                // record the exception
                                lastException.set(e);
                                log.warn("SystemJournal[{}] Error while reading journal {}.", containerId, snapshotFile, lastException);
                                return null;
                            } else {
                                // no exception, return the value.
                                return v;
                            }
                        }, executor),
                executor)
                .handleAsync((v, e) -> {
                    // If read is not done and we have exception then throw.
                    if (!isReadDone.get() && lastException.get() != null) {
                        throw new CompletionException(lastException.get());
                    }
                    return v;
                }, executor)
                .thenApplyAsync(v -> retValue.get(), executor);
    }

    /**
     * Process all systemLog entries to recreate the state of metadata storage system segments.
     */
    private CompletableFuture<Void> applySystemLogOperations(MetadataTransaction txn,
                                                             SystemSnapshotRecord systemSnapshotRecord,
                                                             HashMap<String, Long> chunkStartOffsets,
                                                             HashMap<String, Long> finalTruncateOffsets,
                                                             HashMap<String, Long> finalFirstChunkStartsAtOffsets,
                                                             HashSet<SystemJournalRecord> visitedRecords) {

        val epochToStartScanning = new AtomicLong();
        val fileIndexToRecover = new AtomicInteger(1);
        // Starting with journal file after last snapshot,
        if (null != systemSnapshotRecord) {
            epochToStartScanning.set(systemSnapshotRecord.epoch);
            fileIndexToRecover.set(systemSnapshotRecord.fileIndex + 1);
        }

        // Linearly read and apply all the journal files after snapshot.
        val epochToRecover = new AtomicLong(epochToStartScanning.get());
        return Futures.loop(
                () -> epochToRecover.get() < epoch,
                () -> {
                    // Start scan with file index 1 if epoch is later than snapshot.
                    if (epochToRecover.get() > epochToStartScanning.get()) {
                        fileIndexToRecover.set(1);
                    }

                    // Process one file at a time.
                    val isFileDone = new AtomicBoolean();
                    return Futures.loop(
                            () -> !isFileDone.get(),
                            () -> {
                                val systemLogName = getSystemJournalChunkName(containerId, epochToRecover.get(), fileIndexToRecover.get());
                                return chunkStorage.exists(systemLogName)
                                        .thenComposeAsync(exists -> {
                                            if (!exists) {
                                                // File does not exist. We have reached end of our scanning.
                                                isFileDone.set(true);
                                                return CompletableFuture.completedFuture(null);
                                            } else {
                                                // Read contents.
                                                return getContents(systemLogName)
                                                        .thenComposeAsync(contents -> {
                                                            val input = new ByteArrayInputStream(contents);

                                                            // Apply record batches from the file.
                                                            // Loop is exited with eventual EOFException.
                                                            val isBatchDone = new AtomicBoolean();
                                                            return Futures.loop(
                                                                    () -> !isBatchDone.get(),
                                                                    () -> {
                                                                        try {
                                                                            val batch = BATCH_SERIALIZER.deserialize(input);

                                                                            if (null != batch.getSystemJournalRecords()) {
                                                                                return Futures.loop(
                                                                                            batch.getSystemJournalRecords(),
                                                                                            record -> applyRecord(txn, chunkStartOffsets, finalTruncateOffsets, finalFirstChunkStartsAtOffsets, visitedRecords, record)
                                                                                                        .thenApply( r -> true),
                                                                                            executor);

                                                                            }
                                                                        } catch (EOFException e) {
                                                                            log.debug("SystemJournal[{}] Done processing file {}.", containerId, systemLogName);
                                                                            isBatchDone.set(true);
                                                                        } catch (Exception e) {
                                                                            log.error("SystemJournal[{}] Error file {}.", containerId, systemLogName, e);
                                                                            throw new CompletionException(e);
                                                                        }
                                                                        return CompletableFuture.completedFuture(null);
                                                                    },
                                                                    executor
                                                            ).thenAcceptAsync(v -> {
                                                                // Move to next file.
                                                                fileIndexToRecover.incrementAndGet();
                                                            }, executor);
                                                        }, executor);
                                            }
                                        }, executor);
                            },
                            executor);
                },
                v -> epochToRecover.incrementAndGet(),
                executor);
    }

    /**
     * Apply given {@link SystemJournalRecord}
     */
    private CompletableFuture<Void> applyRecord(MetadataTransaction txn,
                                                HashMap<String, Long> chunkStartOffsets,
                                                HashMap<String, Long> finalTruncateOffsets,
                                                HashMap<String, Long> finalFirstChunkStartsAtOffsets,
                                                HashSet<SystemJournalRecord> visitedRecords, SystemJournalRecord record) {
        log.debug("SystemJournal[{}] Processing system log record ={}.", epoch, record);
        if (visitedRecords.contains(record)) {
            return CompletableFuture.completedFuture(null);
        }
        visitedRecords.add(record);

        // ChunkAddedRecord.
        if (record instanceof ChunkAddedRecord) {
            val chunkAddedRecord = (ChunkAddedRecord) record;
            return applyChunkAddition(txn, chunkStartOffsets,
                    chunkAddedRecord.getSegmentName(),
                    nullToEmpty(chunkAddedRecord.getOldChunkName()),
                    chunkAddedRecord.getNewChunkName(),
                    chunkAddedRecord.getOffset());
        }

        // TruncationRecord.
        if (record instanceof TruncationRecord) {
            val truncationRecord = (TruncationRecord) record;
            finalTruncateOffsets.put(truncationRecord.getSegmentName(), truncationRecord.getOffset());
            finalFirstChunkStartsAtOffsets.put(truncationRecord.getSegmentName(), truncationRecord.getStartOffset());
            return CompletableFuture.completedFuture(null);
        }

        // Unknown record.
        return CompletableFuture.failedFuture(new IllegalStateException(String.format("Unknown record type encountered. record = %s", record)));
    }

    /**
     * Adjusts the lengths of last chunks for each segment.
     */
    private CompletableFuture<Void> adjustLastChunkLengths(MetadataTransaction txn) {
        val futures = new ArrayList<CompletableFuture<Void>>();
        for (String systemSegment : systemSegments) {
            val f = txn.get(systemSegment)
                    .thenComposeAsync(m -> {
                        SegmentMetadata segmentMetadata = (SegmentMetadata) m;
                        segmentMetadata.checkInvariants();
                        CompletableFuture<Void> ff;
                        // Update length of last chunk in metadata to what we actually find on LTS.
                        if (null != segmentMetadata.getLastChunk()) {
                            ff = chunkStorage.getInfo(segmentMetadata.getLastChunk())
                                    .thenComposeAsync(chunkInfo -> {
                                        long length = chunkInfo.getLength();
                                        return txn.get(segmentMetadata.getLastChunk())
                                                .thenAcceptAsync(mm -> {
                                                    ChunkMetadata lastChunk = (ChunkMetadata) mm;
                                                    Preconditions.checkState(null != lastChunk, "lastChunk must not be null. Segment=%s", segmentMetadata);
                                                    lastChunk.setLength(length);
                                                    txn.update(lastChunk);
                                                    segmentMetadata.setLength(segmentMetadata.getLastChunkStartOffset() + length);
                                                }, executor);
                                    }, executor);
                        } else {
                            ff = CompletableFuture.completedFuture(null);
                        }
                        return ff.thenApplyAsync(v -> {
                            Preconditions.checkState(segmentMetadata.isOwnershipChanged(), "ownershipChanged must be true. Segment=%s", segmentMetadata);
                            segmentMetadata.checkInvariants();

                            return segmentMetadata;
                        });
                    }, executor)
                    .thenAcceptAsync(segmentMetadata -> {
                        txn.update(segmentMetadata);
                    }, executor);
            futures.add(f);
        }
        return Futures.allOf(futures);
    }

    /**
     * Apply last effective truncate offsets.
     */
    private CompletableFuture<Void> applyFinalTruncateOffsets(MetadataTransaction txn, HashMap<String, Long> finalTruncateOffsets, HashMap<String, Long> finalFirstChunkStartsAtOffsets) {
        val futures = new ArrayList<CompletableFuture<Void>>();
        for (String systemSegment : systemSegments) {
            if (finalTruncateOffsets.containsKey(systemSegment)) {
                val truncateAt = finalTruncateOffsets.get(systemSegment);
                val firstChunkStartsAt = finalFirstChunkStartsAtOffsets.get(systemSegment);
                futures.add(applyTruncate(txn, systemSegment, truncateAt, firstChunkStartsAt));
            }
        }
        return Futures.allOf(futures);
    }

    /**
     * Apply chunk addition.
     */
    private CompletableFuture<Void> applyChunkAddition(MetadataTransaction txn, HashMap<String, Long> chunkStartOffsets, String segmentName, String oldChunkName, String newChunkName, long offset) {
        Preconditions.checkState(null != oldChunkName, "oldChunkName must not be null");
        Preconditions.checkState(null != newChunkName && !newChunkName.isEmpty(), "newChunkName must not be null or empty");

        return txn.get(segmentName)
                .thenComposeAsync(m -> {
                    val segmentMetadata = (SegmentMetadata) m;
                    segmentMetadata.checkInvariants();
                    validateSegmentRecordInTxn(txn, segmentName);
                    // set length.
                    segmentMetadata.setLength(offset);

                    val newChunkMetadata = ChunkMetadata.builder()
                            .name(newChunkName)
                            .build();
                    newChunkMetadata.setActive(true);
                    txn.create(newChunkMetadata);
                    txn.markPinned(newChunkMetadata);

                    chunkStartOffsets.put(newChunkName, offset);
                    CompletableFuture<Void> f;
                    // Set first and last pointers.
                    if (!oldChunkName.isEmpty()) {
                        Preconditions.checkState(txn.getData().containsKey(oldChunkName), "Txn must contain old key", oldChunkName);
                        f = txn.get(oldChunkName)
                                .thenComposeAsync(mm -> {
                                    val oldChunk = (ChunkMetadata) mm;
                                    Preconditions.checkState(null != oldChunk, "oldChunk must not be null. oldChunkName=%s", oldChunkName);

                                    // In case the old segment store was still writing some zombie chunks when ownership changed
                                    // then new offset may invalidate tail part of chunk list.
                                    // Note that chunk with oldChunkName is still valid, it is the chunks after this that become invalid.
                                    val toDelete = new AtomicReference<String>(oldChunk.getNextChunk());

                                    return Futures.loop(
                                            () -> toDelete.get() != null,
                                            () -> txn.get(toDelete.get())
                                                    .thenAcceptAsync(mmm -> {
                                                        val chunkToDelete = (ChunkMetadata) mmm;
                                                        txn.delete(toDelete.get());
                                                        toDelete.set(chunkToDelete.getNextChunk());
                                                        segmentMetadata.setChunkCount(segmentMetadata.getChunkCount() - 1);
                                                    }, executor),
                                            executor)
                                            .thenAcceptAsync(v -> {
                                                // Set next chunk
                                                oldChunk.setNextChunk(newChunkName);

                                                // Set length
                                                long oldLength = chunkStartOffsets.get(oldChunkName);
                                                oldChunk.setLength(offset - oldLength);

                                                txn.update(oldChunk);
                                            }, executor);
                                }, executor);
                    } else {
                        segmentMetadata.setFirstChunk(newChunkName);
                        segmentMetadata.setStartOffset(offset);
                        f = CompletableFuture.completedFuture(null);
                    }
                    return f.thenComposeAsync(v -> {
                        segmentMetadata.setLastChunk(newChunkName);
                        segmentMetadata.setLastChunkStartOffset(offset);
                        segmentMetadata.setChunkCount(segmentMetadata.getChunkCount() + 1);
                        segmentMetadata.checkInvariants();
                        // Save the segment metadata.
                        txn.update(segmentMetadata);
                        if (config.isSelfCheckEnabled()) {
                            return validateSegmentRecordInTxn(txn, segmentName);
                        } else {
                            return CompletableFuture.completedFuture(null);
                        }
                    }, executor);
                }, executor);
    }

    /**
     * Apply truncate action to the segment metadata.
     */
    private CompletableFuture<Void> applyTruncate(MetadataTransaction txn, String segmentName, long truncateAt, long firstChunkStartsAt) {
        return txn.get(segmentName)
                .thenComposeAsync(metadata -> {
                    SegmentMetadata segmentMetadata = (SegmentMetadata) metadata;
                    segmentMetadata.checkInvariants();
                    val currentChunkName = new AtomicReference<String>(segmentMetadata.getFirstChunk());
                    val currentMetadata = new AtomicReference<ChunkMetadata>();
                    val startOffset = new AtomicLong(segmentMetadata.getFirstChunkStartOffset());
                    val shouldBreak = new AtomicBoolean();
                    return Futures.loop(
                            () -> null != currentChunkName.get() && !shouldBreak.get(),
                            () -> txn.get(currentChunkName.get())
                                    .thenAcceptAsync(m -> {
                                        currentMetadata.set((ChunkMetadata) m);
                                        // If for given chunk start <= truncateAt < end  then we have found the chunk that will be the first chunk.
                                        if ((startOffset.get() <= truncateAt) && (startOffset.get() + currentMetadata.get().getLength() > truncateAt)) {
                                            shouldBreak.set(true);
                                        } else {
                                            startOffset.addAndGet(currentMetadata.get().getLength());
                                            // move to next chunk
                                            currentChunkName.set(currentMetadata.get().getNextChunk());
                                            txn.delete(currentMetadata.get().getName());
                                            segmentMetadata.setChunkCount(segmentMetadata.getChunkCount() - 1);
                                        }
                                    }, executor),
                            executor)
                            .thenAcceptAsync(v -> {
                                Preconditions.checkState(firstChunkStartsAt == startOffset.get(),
                                        "firstChunkStartsAt (%s) must be equal to startOffset (%s)", firstChunkStartsAt, startOffset);
                                segmentMetadata.setFirstChunk(currentChunkName.get());
                                if (null == currentChunkName.get()) {
                                    segmentMetadata.setLastChunk(null);
                                    segmentMetadata.setLastChunkStartOffset(firstChunkStartsAt);
                                }
                                segmentMetadata.setStartOffset(truncateAt);
                                segmentMetadata.setFirstChunkStartOffset(firstChunkStartsAt);
                                segmentMetadata.checkInvariants();
                            }, executor);
                }, executor);
    }

    CompletableFuture<Boolean> validateAndSaveSnapshot(MetadataTransaction txn,
                                                       boolean validateSegment,
                                                       boolean validateChunks) {
        return createSystemSnapshotRecord(txn, validateSegment, validateChunks)
                .thenComposeAsync(this::writeSystemSnapshotRecord, executor);

    }

    private CompletableFuture<SystemSnapshotRecord> createSystemSnapshotRecord(MetadataTransaction txn,
                                                                               boolean validateSegment,
                                                                               boolean validateChunks) {
        val systemSnapshot = SystemSnapshotRecord.builder()
                .epoch(epoch)
                .fileIndex(currentFileIndex.get())
                .segmentSnapshotRecords(new ArrayList<>())
                .build();

        val futures = Collections.synchronizedList(new ArrayList<CompletableFuture<Void>>());
        for (String systemSegment : systemSegments) {
            // Find segment metadata.
            val future = txn.get(systemSegment)
                    .thenComposeAsync(metadata -> {
                        val segmentMetadata = (SegmentMetadata) metadata;
                        segmentMetadata.checkInvariants();

                        val segmentSnapshot = SegmentSnapshotRecord.builder()
                                .segmentMetadata(segmentMetadata)
                                .chunkMetadataCollection(new ArrayList<>())
                                .build();

                        // Enumerate all chunks.
                        val currentChunkName = new AtomicReference<>(segmentMetadata.getFirstChunk());
                        val dataSize = new AtomicLong();
                        val chunkCount = new AtomicLong();

                        // For each chunk
                        return Futures.loop(
                                () -> null != currentChunkName.get(),
                                () -> txn.get(currentChunkName.get())
                                        .thenComposeAsync(m -> {
                                            val currentChunkMetadata = (ChunkMetadata) m;
                                            CompletableFuture<Void> f;
                                            Preconditions.checkState(null != currentChunkMetadata, "currentChunkMetadata must not be null");
                                            if (validateChunks) {
                                                f = chunkStorage.getInfo(currentChunkName.get())
                                                        .thenAcceptAsync(chunkInfo ->
                                                                        Preconditions.checkState(chunkInfo.getLength() >= currentChunkMetadata.getLength(),
                                                                                "Wrong chunk length chunkInfo=%d, currentMetadata=%d.",
                                                                                chunkInfo.getLength(), currentChunkMetadata.getLength()),
                                                                executor);
                                            } else {
                                                f = CompletableFuture.completedFuture(null);
                                            }
                                            return f.thenAcceptAsync(v -> {
                                                chunkCount.getAndIncrement();
                                                dataSize.addAndGet(currentChunkMetadata.getLength());
                                                segmentSnapshot.chunkMetadataCollection.add(currentChunkMetadata);
                                                // move to next chunk
                                                currentChunkName.set(currentChunkMetadata.getNextChunk());
                                            }, executor);
                                        }, executor),
                                executor)
                                .thenAcceptAsync(v -> {
                                    // Validate
                                    if (validateSegment) {
                                        Preconditions.checkState(chunkCount.get() == segmentMetadata.getChunkCount(), "Wrong chunk count. Segment=%s", segmentMetadata);
                                        Preconditions.checkState(dataSize.get() == segmentMetadata.getLength() - segmentMetadata.getFirstChunkStartOffset(),
                                                "Data size does not match dataSize (%s). Segment=%s", dataSize.get(), segmentMetadata);
                                    }

                                    // Add to the system snapshot.
                                    synchronized (systemSnapshot) {
                                        systemSnapshot.segmentSnapshotRecords.add(segmentSnapshot);
                                    }
                                }, executor);
                    }, executor);
            futures.add(future);
        }
        return Futures.allOf(futures)
                .thenApplyAsync(v -> systemSnapshot, executor);
    }

    /**
     *
     * @param systemSnapshot
     * @return
     */
    private CompletableFuture<Boolean> writeSystemSnapshotRecord(SystemSnapshotRecord systemSnapshot) {
        // Write snapshot
        ByteArraySegment bytes;
        try {
            bytes = SYSTEM_SNAPSHOT_SERIALIZER.serialize(systemSnapshot);
        } catch (IOException e) {
            log.error("SystemJournal[{}] Error while creating snapshot {}", containerId, e);
            return CompletableFuture.completedFuture(false);
        }
        val isWritten = new AtomicBoolean();
        val attempt = new AtomicInteger();
        val lastException = new AtomicReference<Throwable>();
        return Futures.loop(
                () -> attempt.get() < config.getMaxJournalWriteAttempts() && !isWritten.get(),
                () -> {
                    currentSnapshotIndex.incrementAndGet();
                    val snapshotFile = NameUtils.getSystemJournalSnapshotFileName(containerId, epoch, currentSnapshotIndex.get());
                    return chunkStorage.createWithContent(snapshotFile,
                            bytes.getLength(),
                            new ByteArrayInputStream(bytes.array(), bytes.arrayOffset(), bytes.getLength()))
                            .thenComposeAsync(v -> getContents(snapshotFile)
                                            .thenAcceptAsync(contents -> {
                                                try {
                                                    val snapshotReadback = SYSTEM_SNAPSHOT_SERIALIZER.deserialize(contents);
                                                    if (config.isSelfCheckEnabled()) {
                                                        checkInvariants(snapshotReadback);
                                                    }
                                                    Preconditions.checkState(systemSnapshot.equals(snapshotReadback), "Records do not match %s != %s", snapshotReadback, systemSnapshot);
                                                    // Record as successful.
                                                    lastSavedSystemSnapshot.set(systemSnapshot);
                                                    lastSavedSystemSnapshotId.set(currentSnapshotIndex.get());
                                                    isWritten.set(true);
                                                } catch (Exception e1) {
                                                    throw new CompletionException(Exceptions.unwrap(e1));
                                                }
                                            }, executor),
                                    executor)
                            .handleAsync((v, e) -> {
                                // Start new journal.
                                newChunkRequired.set(true);
                                attempt.incrementAndGet();
                                if (e != null) {
                                    lastException.set(Exceptions.unwrap(e));
                                    return null;
                                } else {
                                    return v;
                                }
                            }, executor);
                },
                executor)
                .thenApplyAsync(v -> isWritten.get(), executor)
                .whenCompleteAsync((v, e) -> {
                    if (!isWritten.get() && null != lastException.get()) {
                        throw new CompletionException(lastException.get());
                    }
                }, executor);
    }

    /**
     * Writes given ByteArraySegment to journal.
     *
     * @param bytes Bytes to write.
     */
    private CompletableFuture<Void> writeToJournal(ByteArraySegment bytes) {
        if (newChunkRequired.get()) {
            currentFileIndex.incrementAndGet();
            systemJournalOffset.set(0);
            return chunkStorage.createWithContent(getSystemJournalChunkName(containerId, epoch, currentFileIndex.get()), bytes.getLength(),
                    new ByteArrayInputStream(bytes.array(), bytes.arrayOffset(), bytes.getLength()))
                    .thenAcceptAsync(h -> {
                        currentHandle.set(h);
                        systemJournalOffset.addAndGet(bytes.getLength());
                        newChunkRequired.set(false);
                    }, executor);
        } else {
            Preconditions.checkState(chunkStorage.supportsAppend() && config.isAppendEnabled(), "Append mode not enabled or chunk storage does not support appends.");
            return chunkStorage.write(currentHandle.get(), systemJournalOffset.get(), bytes.getLength(),
                    new ByteArrayInputStream(bytes.array(), bytes.arrayOffset(), bytes.getLength()))
                    .thenAcceptAsync(bytesWritten -> {
                        Preconditions.checkState(bytesWritten == bytes.getLength(),
                                "Bytes written do not match expected length. Actual=%d, expected=%d", bytesWritten, bytes.getLength());
                        systemJournalOffset.addAndGet(bytesWritten);
                    }, executor);
        }
    }

    /**
     * Indicates whether given segment is a system segment.
     *
     * @param segmentName Name of the segment to check.
     * @return True if given segment is a system segment.
     */
    public boolean isStorageSystemSegment(String segmentName) {
        if (segmentName.startsWith(systemSegmentsPrefix)) {
            for (String systemSegment : systemSegments) {
                if (segmentName.equals(systemSegment)) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Gets the names of the critical storage segments.
     *
     * @param containerId Container if of the owner container.
     * @return Array of names of the critical storage segments.
     */
    public static String[] getChunkStorageSystemSegments(int containerId) {
        return new String[]{
                NameUtils.getStorageMetadataSegmentName(containerId),
                NameUtils.getAttributeSegmentName(NameUtils.getStorageMetadataSegmentName(containerId)),
                NameUtils.getMetadataSegmentName(containerId),
                NameUtils.getAttributeSegmentName(NameUtils.getMetadataSegmentName(containerId))
        };
    }

    private String getSystemJournalChunkName() {
        return getSystemJournalChunkName(containerId, epoch, currentFileIndex.get());
    }

    private String getSystemJournalChunkName(int containerId, long epoch, long currentFileIndex) {
        return NameUtils.getSystemJournalFileName(containerId, epoch, currentFileIndex);
    }

    private <R> CompletableFuture<R> executeSerialized(Callable<CompletableFuture<R>> operation) {
        return this.taskProcessor.add(Collections.singletonList(LOCK_KEY_NAME), () -> executeExclusive(operation));
    }

    private <R> CompletableFuture<R> executeExclusive(Callable<CompletableFuture<R>> operation) {
        return CompletableFuture.completedFuture(null).thenComposeAsync(v -> {
            try {
                return operation.call();
            } catch (CompletionException e) {
                throw new CompletionException(Exceptions.unwrap(e));
            } catch (Exception e) {
                throw new CompletionException(e);
            }
        }, this.executor);
    }

    /**
     * Represents a system journal record.
     */
    @Data
    public static class SystemJournalRecord {
        /**
         * Serializer that implements {@link VersionedSerializer}.
         */
        public static class SystemJournalRecordSerializer extends VersionedSerializer.MultiType<SystemJournalRecord> {
            /**
             * Declare all supported serializers of subtypes.
             *
             * @param builder A MultiType.Builder that can be used to declare serializers.
             */
            @Override
            protected void declareSerializers(Builder builder) {
                // Unused values (Do not repurpose!):
                // - 0: Unsupported Serializer.
                builder.serializer(ChunkAddedRecord.class, 1, new ChunkAddedRecord.Serializer())
                        .serializer(TruncationRecord.class, 2, new TruncationRecord.Serializer())
                        .serializer(SystemSnapshotRecord.class, 3, new SystemSnapshotRecord.Serializer())
                        .serializer(SegmentSnapshotRecord.class, 4, new SegmentSnapshotRecord.Serializer());
            }
        }
    }

    /**
     * Represents a system journal record.
     */
    @Builder(toBuilder = true)
    @Data
    @EqualsAndHashCode
    static class SystemJournalRecordBatch {
        @NonNull
        private final Collection<SystemJournalRecord> systemJournalRecords;

        /**
         * Builder that implements {@link ObjectBuilder}.
         */
        public static class SystemJournalRecordBatchBuilder implements ObjectBuilder<SystemJournalRecordBatch> {
        }

        /**
         * Serializer that implements {@link VersionedSerializer}.
         */
        public static class SystemJournalRecordBatchSerializer
                extends VersionedSerializer.WithBuilder<SystemJournalRecordBatch, SystemJournalRecordBatchBuilder> {
            private static final SystemJournalRecord.SystemJournalRecordSerializer SERIALIZER = new SystemJournalRecord.SystemJournalRecordSerializer();
            private static final RevisionDataOutput.ElementSerializer<SystemJournalRecord> ELEMENT_SERIALIZER = (dataOutput, element) -> SERIALIZER.serialize(dataOutput, element);
            private static final RevisionDataInput.ElementDeserializer<SystemJournalRecord> ELEMENT_DESERIALIZER = dataInput -> SERIALIZER.deserialize(dataInput.getBaseStream());

            @Override
            protected SystemJournalRecordBatchBuilder newBuilder() {
                return builder();
            }

            @Override
            protected byte getWriteVersion() {
                return 0;
            }

            @Override
            protected void declareVersions() {
                version(0).revision(0, this::write00, this::read00);
            }

            private void read00(RevisionDataInput input, SystemJournalRecordBatchBuilder b) throws IOException {
                b.systemJournalRecords(input.readCollection(ELEMENT_DESERIALIZER));
            }

            private void write00(SystemJournalRecordBatch object, RevisionDataOutput output) throws IOException {
                output.writeCollection(object.systemJournalRecords, ELEMENT_SERIALIZER);
            }
        }
    }

    /**
     * Journal record for chunk addition.
     */
    @Builder(toBuilder = true)
    @Data
    @EqualsAndHashCode(callSuper = true)
    static class ChunkAddedRecord extends SystemJournalRecord {
        /**
         * Name of the segment.
         */
        @NonNull
        private final String segmentName;

        /**
         * Offset at which first byte in chunk starts.
         */
        private final long offset;

        /**
         * Name of the old last chunk.
         */
        private final String oldChunkName;

        /**
         * Name of the new chunk.
         */
        @NonNull
        private final String newChunkName;

        /**
         * Builder that implements {@link ObjectBuilder}.
         */
        public static class ChunkAddedRecordBuilder implements ObjectBuilder<ChunkAddedRecord> {
        }

        /**
         * Serializer that implements {@link VersionedSerializer}.
         */
        public static class Serializer extends VersionedSerializer.WithBuilder<ChunkAddedRecord, ChunkAddedRecordBuilder> {
            @Override
            protected ChunkAddedRecordBuilder newBuilder() {
                return ChunkAddedRecord.builder();
            }

            @Override
            protected byte getWriteVersion() {
                return 0;
            }

            @Override
            protected void declareVersions() {
                version(0).revision(0, this::write00, this::read00);
            }

            private void write00(ChunkAddedRecord object, RevisionDataOutput output) throws IOException {
                output.writeUTF(object.segmentName);
                output.writeUTF(nullToEmpty(object.newChunkName));
                output.writeUTF(nullToEmpty(object.oldChunkName));
                output.writeCompactLong(object.offset);
            }

            private void read00(RevisionDataInput input, ChunkAddedRecordBuilder b) throws IOException {
                b.segmentName(input.readUTF());
                b.newChunkName(emptyToNull(input.readUTF()));
                b.oldChunkName(emptyToNull(input.readUTF()));
                b.offset(input.readCompactLong());
            }
        }
    }

    /**
     * Journal record for segment truncation.
     */
    @Builder(toBuilder = true)
    @Data
    @EqualsAndHashCode(callSuper = true)
    static class TruncationRecord extends SystemJournalRecord {
        /**
         * Name of the segment.
         */
        @NonNull
        private final String segmentName;

        /**
         * Offset at which chunk is truncated.
         */
        private final long offset;

        /**
         * Name of the new first chunk.
         */
        @NonNull
        private final String firstChunkName;

        /**
         * Offset inside the first chunk where data starts.
         */
        private final long startOffset;

        /**
         * Builder that implements {@link ObjectBuilder}.
         */
        public static class TruncationRecordBuilder implements ObjectBuilder<TruncationRecord> {
        }

        /**
         * Serializer that implements {@link VersionedSerializer}.
         */
        public static class Serializer extends VersionedSerializer.WithBuilder<TruncationRecord, TruncationRecord.TruncationRecordBuilder> {
            @Override
            protected TruncationRecord.TruncationRecordBuilder newBuilder() {
                return TruncationRecord.builder();
            }

            @Override
            protected byte getWriteVersion() {
                return 0;
            }

            @Override
            protected void declareVersions() {
                version(0).revision(0, this::write00, this::read00);
            }

            private void write00(TruncationRecord object, RevisionDataOutput output) throws IOException {
                output.writeUTF(object.segmentName);
                output.writeCompactLong(object.offset);
                output.writeUTF(object.firstChunkName);
                output.writeCompactLong(object.startOffset);
            }

            private void read00(RevisionDataInput input, TruncationRecord.TruncationRecordBuilder b) throws IOException {
                b.segmentName(input.readUTF());
                b.offset(input.readCompactLong());
                b.firstChunkName(input.readUTF());
                b.startOffset(input.readCompactLong());
            }
        }
    }

    /**
     * Journal record for segment snapshot.
     */
    @Builder(toBuilder = true)
    @Data
    @EqualsAndHashCode(callSuper = true)
    static class SegmentSnapshotRecord extends SystemJournalRecord {
        /**
         * Data about the segment.
         */
        @NonNull
        private final SegmentMetadata segmentMetadata;

        @NonNull
        private final Collection<ChunkMetadata> chunkMetadataCollection;

        /**
         * Builder that implements {@link ObjectBuilder}.
         */
        public static class SegmentSnapshotRecordBuilder implements ObjectBuilder<SegmentSnapshotRecord> {
        }

        /**
         * Serializer that implements {@link VersionedSerializer}.
         */
        public static class Serializer extends VersionedSerializer.WithBuilder<SegmentSnapshotRecord, SegmentSnapshotRecord.SegmentSnapshotRecordBuilder> {

            private static final SegmentMetadata.StorageMetadataSerializer SEGMENT_METADATA_SERIALIZER = new SegmentMetadata.StorageMetadataSerializer();
            private static final ChunkMetadata.StorageMetadataSerializer CHUNK_METADATA_SERIALIZER = new ChunkMetadata.StorageMetadataSerializer();
            private static final RevisionDataOutput.ElementSerializer<ChunkMetadata> ELEMENT_SERIALIZER = (dataOutput, element) -> CHUNK_METADATA_SERIALIZER.serialize(dataOutput, element);
            private static final RevisionDataInput.ElementDeserializer<ChunkMetadata> ELEMENT_DESERIALIZER = dataInput -> (ChunkMetadata) CHUNK_METADATA_SERIALIZER.deserialize(dataInput.getBaseStream());

            @Override
            protected SegmentSnapshotRecord.SegmentSnapshotRecordBuilder newBuilder() {
                return SegmentSnapshotRecord.builder();
            }

            @Override
            protected byte getWriteVersion() {
                return 0;
            }

            @Override
            protected void declareVersions() {
                version(0).revision(0, this::write00, this::read00);
            }

            private void write00(SegmentSnapshotRecord object, RevisionDataOutput output) throws IOException {
                SEGMENT_METADATA_SERIALIZER.serialize(output, object.segmentMetadata);
                output.writeCollection(object.chunkMetadataCollection, ELEMENT_SERIALIZER);
            }

            private void read00(RevisionDataInput input, SegmentSnapshotRecord.SegmentSnapshotRecordBuilder b) throws IOException {
                b.segmentMetadata((SegmentMetadata) SEGMENT_METADATA_SERIALIZER.deserialize(input.getBaseStream()));
                b.chunkMetadataCollection(input.readCollection(ELEMENT_DESERIALIZER));
            }
        }
    }

    /**
     * Journal record for segment snapshot.
     */
    @Builder(toBuilder = true)
    @Data
    @EqualsAndHashCode(callSuper = true)
    static class SystemSnapshotRecord extends SystemJournalRecord {
        /**
         * Epoch of the snapshot
         */
        private final long epoch;

        /**
         * Epoch of the snapshot
         */
        private final int fileIndex;

        /**
         * Snapshot of the individual segments.
         */
        @NonNull
        private final Collection<SegmentSnapshotRecord> segmentSnapshotRecords;

        /**
         * Builder that implements {@link ObjectBuilder}.
         */
        public static class SystemSnapshotRecordBuilder implements ObjectBuilder<SystemSnapshotRecord> {
        }

        /**
         * Serializer that implements {@link VersionedSerializer}.
         */
        public static class Serializer extends VersionedSerializer.WithBuilder<SystemSnapshotRecord, SystemSnapshotRecord.SystemSnapshotRecordBuilder> {
            private static final SegmentSnapshotRecord.Serializer CHUNK_METADATA_SERIALIZER = new SegmentSnapshotRecord.Serializer();
            private static final RevisionDataOutput.ElementSerializer<SegmentSnapshotRecord> ELEMENT_SERIALIZER = (dataOutput, element) -> CHUNK_METADATA_SERIALIZER.serialize(dataOutput, element);
            private static final RevisionDataInput.ElementDeserializer<SegmentSnapshotRecord> ELEMENT_DESERIALIZER = dataInput -> (SegmentSnapshotRecord) CHUNK_METADATA_SERIALIZER.deserialize(dataInput.getBaseStream());

            @Override
            protected SystemSnapshotRecord.SystemSnapshotRecordBuilder newBuilder() {
                return SystemSnapshotRecord.builder();
            }

            @Override
            protected byte getWriteVersion() {
                return 0;
            }

            @Override
            protected void declareVersions() {
                version(0).revision(0, this::write00, this::read00);
            }

            private void write00(SystemSnapshotRecord object, RevisionDataOutput output) throws IOException {
                output.writeCompactLong(object.epoch);
                output.writeCompactInt(object.fileIndex);
                output.writeCollection(object.segmentSnapshotRecords, ELEMENT_SERIALIZER);
            }

            private void read00(RevisionDataInput input, SystemSnapshotRecord.SystemSnapshotRecordBuilder b) throws IOException {
                b.epoch(input.readCompactLong());
                b.fileIndex(input.readCompactInt());
                b.segmentSnapshotRecords(input.readCollection(ELEMENT_DESERIALIZER));
            }
        }
    }

}
