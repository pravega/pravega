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
import io.pravega.common.Timer;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.concurrent.MultiKeySequentialProcessor;
import io.pravega.common.io.serialization.RevisionDataInput;
import io.pravega.common.io.serialization.RevisionDataOutput;
import io.pravega.common.io.serialization.VersionedSerializer;
import io.pravega.common.util.ByteArraySegment;
import io.pravega.segmentstore.storage.StorageNotPrimaryException;
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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;
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
import lombok.RequiredArgsConstructor;
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

    /**
     * Serializer for {@link SnapshotInfo}.
     */
    private static final SnapshotInfo.Serializer SNAPSHOT_INFO_SERIALIZER = new SnapshotInfo.Serializer();

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
    final private AtomicBoolean newChunkRequired = new AtomicBoolean(true);

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
     * List of chunks (journals & snapshots) to delete after snapshot.
     */
    final private List<String> pendingGarbageChunks = new Vector<>();

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
        this.systemSegmentsPrefix = NameUtils.INTERNAL_CONTAINER_PREFIX;
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
     * Holds state for boot strap operation.
     */
    @RequiredArgsConstructor
    private static class BootstrapState {
        /**
         * Keep track of offsets at which chunks were added to the system segments.
         */
        final private Map<String, Long> chunkStartOffsets = Collections.synchronizedMap(new HashMap<>());

        /**
         * Keep track of offsets at which system segments were truncated.
         * We don't need to apply each truncate operation, only need to apply the final truncate offset.
         */
        final private Map<String, Long> finalTruncateOffsets = Collections.synchronizedMap(new HashMap<>());

        /**
         * Final first chunk start offsets for all segments.
         */
        final private Map<String, Long> finalFirstChunkStartsAtOffsets = Collections.synchronizedMap(new HashMap<>());

        /**
         * Keep track of already processed records.
         */
        final private Set<SystemJournalRecord> visitedRecords = Collections.synchronizedSet(new HashSet<>());

        /**
         * Number of journals processed.
         */
        final private AtomicInteger filesProcessedCount = new AtomicInteger();

        /**
         * Number of records processed.
         */
        final private AtomicInteger recordsProcessedCount = new AtomicInteger();
    }

    /**
     * Bootstrap the metadata about storage metadata segments by reading and processing the journal.
     *
     * @param epoch             Epoch of the current container instance.
     * @param snapshotInfoStore {@link SnapshotInfoStore} that stores {@link SnapshotInfo}.
     */
    public CompletableFuture<Void> bootstrap(long epoch, SnapshotInfoStore snapshotInfoStore) {
        this.epoch = epoch;
        this.snapshotInfoStore = Preconditions.checkNotNull(snapshotInfoStore, "snapshotInfoStore");
        Preconditions.checkState(!reentryGuard.getAndSet(true), "bootstrap called multiple times.");

        log.info("SystemJournal[{}] BOOT started.", containerId);
        Timer t = new Timer();

        // Start a transaction
        val txn = metadataStore.beginTransaction(false, getSystemSegments());

        val state = new BootstrapState();

        // Step 1: Create metadata records for system segments from latest snapshot.
        return findLatestSnapshot()
                .thenComposeAsync(snapshot ->
                   applySystemSnapshotRecord(txn, state, snapshot),
                executor)
                .thenComposeAsync(latestSnapshot -> {
                    // Step 2: For each epoch, find the corresponding system journal files, process them and apply operations recorded.
                    return applySystemLogOperations(txn, state, latestSnapshot);
                }, executor)
                .thenComposeAsync(v -> {
                    // Step 3: Adjust the length of the last chunk.
                    return adjustLastChunkLengths(txn);
                }, executor)
                .thenComposeAsync(v -> {
                    // Step 4: Apply the truncate offsets.
                    return applyFinalTruncateOffsets(txn, state);
                }, executor)
                .thenComposeAsync(v -> {
                    // Step 5: Check invariants. These should never fail.
                    if (config.isSelfCheckEnabled()) {
                        Preconditions.checkState(currentFileIndex.get() == 0, "currentFileIndex must be zero");
                        Preconditions.checkState(systemJournalOffset.get() == 0, "systemJournalOffset must be zero");
                        Preconditions.checkState(newChunkRequired.get(), "newChunkRequired must be true");
                    }
                    // Step 6: Create a snapshot record and validate it. Save it in journals.
                    return createSystemSnapshotRecord(txn, true, config.isSelfCheckEnabled())
                            .thenComposeAsync(systemSnapshotRecord -> writeRecordBatch(Collections.singletonList(systemSnapshotRecord)), executor)
                            .thenRunAsync(() -> newChunkRequired.set(true), executor);
                }, executor)
                .thenComposeAsync(v -> {
                    // Step 7: Finally commit all data.
                    return txn.commit(true, true);
                }, executor)
                .whenCompleteAsync((v, e) -> {
                    txn.close();
                    if (e == null) {
                        log.info("SystemJournal[{}] BOOT complete - applied {} records in {} journals. Total time = {} ms.",
                                containerId,
                                state.recordsProcessedCount.get(),
                                state.filesProcessedCount.get(),
                                t.getElapsedMillis());
                    } else {
                        log.error("SystemJournal[{}] BOOT failed. Total time = {} ms.", containerId, t.getElapsedMillis(), e);
                    }
                }, executor);
    }

    /**
     * Checks if snapshot file exists for given snapshotId.
     */
    private CompletableFuture<Void> checkSnapshotFileExists(long snapshotId) {
        if (getConfig().isSelfCheckEnabled()) {
            val snapshotFileName = NameUtils.getSystemJournalSnapshotFileName(containerId, epoch, snapshotId);
            return chunkStorage.exists(snapshotFileName)
                    .thenAcceptAsync(exists -> Preconditions.checkState(exists, "Snapshot chunk must exist"), executor);
        }
        return CompletableFuture.completedFuture(null);
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
                            log.trace("SystemJournal[{}] Logging system log records - journal={}, batch={}.",
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
                                log.warn("SystemJournal[{}] Error while writing journal {}. Attempt#{}", containerId,
                                        getSystemJournalChunkName(containerId, epoch, currentFileIndex.get()), attempt.get(), e);

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
        // Generate a snapshot if no snapshot was saved before or when threshold for either time or number batches is reached.
        boolean shouldGenerate = true;
        if (lastSavedSystemSnapshot.get() == null) {
            log.debug("SystemJournal[{}] Generating first snapshot.", containerId);
        } else if (recordsSinceSnapshot.get() > config.getMaxJournalUpdatesPerSnapshot()) {
            log.debug("SystemJournal[{}] Generating snapshot based on update threshold. {} updates since last snapshot.", containerId, recordsSinceSnapshot.get());
        } else if (currentTimeSupplier.get() - lastSavedSnapshotTime.get() > config.getJournalSnapshotInfoUpdateFrequency().toMillis()) {
            log.debug("SystemJournal[{}] Generating snapshot based on time threshold. current time={} last saved ={}.",
                    containerId, currentTimeSupplier.get(), lastSavedSnapshotTime.get());
        } else {
            shouldGenerate = false;
        }
        if (shouldGenerate) {
            // Write a snapshot.
            val txn = metadataStore.beginTransaction(true, getSystemSegments());
            return validateAndSaveSnapshot(txn, true, config.isSelfCheckEnabled())
                    .thenAcceptAsync(saved -> {
                        txn.close();
                        if (saved) {
                            lastSavedSnapshotTime.set(currentTimeSupplier.get());
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
        if (lastSavedSystemSnapshot.get() != null) {
            boolean shouldSave = true;
            if (lastSavedSnapshotInfo.get() == null) {
                log.debug("SystemJournal[{}] Saving first snapshot info new={}.", containerId, lastSavedSystemSnapshotId.get());
            } else if (lastSavedSnapshotInfo.get().getSnapshotId() < lastSavedSystemSnapshotId.get()) {
                log.debug("SystemJournal[{}] Saving new snapshot info new={} old={}.", containerId,
                        lastSavedSystemSnapshotId.get(), lastSavedSnapshotInfo.get().getSnapshotId());
            } else {
                shouldSave = false;
            }
            if (shouldSave) {
                return writeSnapshotInfo(lastSavedSystemSnapshotId.get());
            }
        }

        return CompletableFuture.completedFuture(null);
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
                            .thenComposeAsync(v1 -> writeSnapshotInfoToFile(info), executor)
                            .thenAcceptAsync(v2 -> {
                                val oldSnapshotInfo = lastSavedSnapshotInfo.get();
                                log.info("SystemJournal[{}] Snapshot info saved.{}", containerId, info);
                                lastSavedSnapshotInfo.set(info);
                                if (null != oldSnapshotInfo) {
                                    val oldSnapshotFile = NameUtils.getSystemJournalSnapshotFileName(containerId, epoch, oldSnapshotInfo.getSnapshotId());
                                    pendingGarbageChunks.add(oldSnapshotFile);
                                }
                                garbageCollector.addChunksToGarbage(-1, pendingGarbageChunks);
                                pendingGarbageChunks.clear();
                            }, executor)
                            .exceptionally(e -> {
                                log.error("SystemJournal[{}] Unable to persist snapshot info.{}", containerId, currentSnapshotIndex, e);
                                return null;
                            });
                }, executor);
    }

    /**
     * Writes SnapshotInfo to a well known file.
     */
    private CompletableFuture<Void> writeSnapshotInfoToFile(SnapshotInfo info) {
        val snapshotInfoFileName = NameUtils.getSystemJournalSnapshotInfoFileName(containerId);
        try {
            val isDone = new AtomicBoolean(false);
            val attempts = new AtomicInteger();
            val bytes = SNAPSHOT_INFO_SERIALIZER.serialize(info);

            return Futures.loop(
                    () -> !isDone.get(),
                    () -> {
                        return chunkStorage.exists(snapshotInfoFileName)
                                .thenComposeAsync( exists -> {
                                    if (exists) {
                                        // Read back and make sure higher epoch has not already written it
                                        return readSnapshotInfoFromFile()
                                                .thenComposeAsync( existingSnapshot -> {
                                                    if (existingSnapshot.getEpoch() > epoch) {
                                                        return CompletableFuture.failedFuture(
                                                            new StorageNotPrimaryException(String.format(
                                                                "SystemJournal[{}] Unexpected snapshot. Expected = {} actual = {}",
                                                                info, existingSnapshot)));
                                                    } else {
                                                        return chunkStorage.delete(ChunkHandle.writeHandle(snapshotInfoFileName));
                                                    }
                                                }, executor);
                                    } else {
                                        return CompletableFuture.completedFuture(null);
                                    }
                                }, executor)
                                .thenComposeAsync(v -> chunkStorage.createWithContent(snapshotInfoFileName,
                                                bytes.getLength(),
                                                new ByteArrayInputStream(bytes.array(), bytes.arrayOffset(), bytes.getLength())),
                                        executor)
                                .thenComposeAsync(v -> {
                                    log.debug("SystemJournal[{}] Snapshot info saved to LTS. File {}. info = {}", containerId, snapshotInfoFileName, info);
                                    return readSnapshotInfoFromFile();
                                    }, executor)
                                .thenApplyAsync( readBackInfo -> {
                                    if (readBackInfo.getEpoch() > epoch) {
                                        throw new CompletionException(new StorageNotPrimaryException(String.format(
                                            "SystemJournal[{}] Unexpected snapshot. Expected = {} actual = {}",
                                            info, readBackInfo)));
                                    }
                                    if (!info.equals(readBackInfo)) {
                                        throw new CompletionException(
                                                new IllegalStateException(String.format("SystemJournal[{}] Unexpected snapshot. Expected = {} actual = {}", info, readBackInfo)));
                                    }
                                    return null;
                                }, executor)
                                .handleAsync((v, e) -> {
                                    if (null != e) {
                                        val ex = Exceptions.unwrap(e);
                                        if (ex instanceof StorageNotPrimaryException) {
                                            log.warn("SystemJournal[{}] Error while saving snapshot info to LTS {}. info = {}", containerId, snapshotInfoFileName, info, e);
                                            throw new CompletionException(e);
                                        }
                                        if (attempts.incrementAndGet() > config.getMaxJournalWriteAttempts()) {
                                            log.warn("SystemJournal[{}] Error while saving snapshot info to LTS. File = {}. info = {}", containerId, snapshotInfoFileName, info, e);
                                            throw new CompletionException(e);
                                        }
                                        log.warn("SystemJournal[{}] Error while saving snapshot info to LTS. File = {}. info = {}", containerId, snapshotInfoFileName, info, e);
                                    } else {
                                        // No exception we are done
                                        log.info("SystemJournal[{}] Snapshot info saved successfully. info = {}", containerId, info);
                                        isDone.set(true);
                                    }
                                    return null;
                                }, executor);
                    }, executor);

        } catch (IOException e) {
            return CompletableFuture.failedFuture(new ChunkStorageException(snapshotInfoFileName, "Unable to serialize", e));
        }
    }

    /**
     * Find and apply latest snapshot.
     */
    private CompletableFuture<SystemSnapshotRecord> findLatestSnapshot() {
        // Step 1: Read snapshot info.
        return readSnapshotInfo()
            .thenComposeAsync(snapshotInfo -> {
                if (null != snapshotInfo) {
                    val snapshotFileName = NameUtils.getSystemJournalSnapshotFileName(containerId, snapshotInfo.getEpoch(), snapshotInfo.getSnapshotId());
                    log.debug("SystemJournal[{}] Snapshot info read. {} pointing to {}", containerId, snapshotInfo, snapshotFileName);

                    // Step 2: Validate and Read contents.
                    return getContents(snapshotFileName, false)
                            // Step 3: Deserialize and return.
                            .thenApplyAsync(contents -> readSnapshotRecord(snapshotInfo, contents), executor)
                            .exceptionally(e -> {
                                throw new CompletionException(new IllegalStateException(
                                        String.format("Chunk pointed by SnapshotInfo could not be read. chunk name = %s", snapshotFileName),
                                        Exceptions.unwrap(e)));
                            });
                } else {
                    log.info("SystemJournal[{}] No Snapshot info available. This is ok if this is new installation", containerId);
                    return CompletableFuture.completedFuture(null);
                }
            }, executor);
    }

    /**
     * Read snapshot info.
     */
    private CompletableFuture<SnapshotInfo> readSnapshotInfo() {
        log.info("SystemJournal[{}] reading snapshot info from store.", containerId);
        return snapshotInfoStore.readSnapshotInfo()
            .thenComposeAsync(snapshotInfoFromStore -> {
                if (config.isSelfCheckForSnapshotEnabled() || null == snapshotInfoFromStore) {
                    log.info("SystemJournal[{}] Read Snapshot info from store. Info = {}", containerId, snapshotInfoFromStore);
                    return readSnapshotInfoFromFile()
                            .thenComposeAsync( snapshotInfoFromFile -> {
                                if ( null == snapshotInfoFromFile ) {
                                    log.info("SystemJournal[{}] No Snapshot info available from storage. This is ok if this is new installation", containerId);
                                    return CompletableFuture.completedFuture(snapshotInfoFromStore);
                                } else {
                                    if (snapshotInfoFromStore != null) {
                                        if (!snapshotInfoFromStore.equals(snapshotInfoFromFile)) {
                                            log.warn("SystemJournal[{}] Snapshot info from store should match one from file. File = {}, Store = {}",
                                                     containerId, snapshotInfoFromFile, snapshotInfoFromStore);
                                        } else {
                                            log.debug("SystemJournal[{}] readSnapshotInfo. File = {}, Store = {}", containerId, snapshotInfoFromFile, snapshotInfoFromStore);
                                        }
                                    } else {
                                        log.info("SystemJournal[{}] Using Snapshot info available from LTS instead of store. This is ok if this first boot after migration",
                                                containerId);
                                    }
                                    return CompletableFuture.completedFuture(snapshotInfoFromFile);
                                }

                            }, executor);
                } else {
                    return CompletableFuture.completedFuture(snapshotInfoFromStore);
                }
            }, executor);
    }

    /**
     * Read snapshot info from file.
     */
    private CompletableFuture<SnapshotInfo> readSnapshotInfoFromFile() {
        val snapshotInfoFileName = NameUtils.getSystemJournalSnapshotInfoFileName(containerId);
        return getContents(snapshotInfoFileName, false)
                .exceptionally(e -> {
                    val ex = Exceptions.unwrap(e);
                    if (ex instanceof ChunkNotFoundException) {
                        log.info(String.format("Chunk containing SnapshotInfo does not exist. This is ok if this is new installation. Chunk name = %s", snapshotInfoFileName));
                    } else {
                        log.warn(String.format("Chunk containing SnapshotInfo could not be read. Chunk name = %s", snapshotInfoFileName), ex);
                    }
                    return null;
                })
                .thenApplyAsync(contents -> {
                    if (contents != null) {
                        log.debug("SystemJournal[{}] reading snapshot info from LTS file {}", containerId, snapshotInfoFileName);
                        try {
                            val snapshotInfo = SNAPSHOT_INFO_SERIALIZER.deserialize(contents);
                            return snapshotInfo;
                        } catch (Exception e) {
                            log.error("SystemJournal[{}] Error while reading snapshot info {}", containerId, e);
                        }
                    }
                    return null;
                }, executor);
    }

    /**
     * Read {@link SystemSnapshotRecord} from given byte array.
     */
    private SystemSnapshotRecord readSnapshotRecord(SnapshotInfo snapshotInfo, byte[] snapshotContents) {
        try {
            val systemSnapshot = SYSTEM_SNAPSHOT_SERIALIZER.deserialize(snapshotContents);
            log.info("SystemJournal[{}] Done finding snapshots. Snapshot found and parsed. {}", containerId, snapshotInfo);
            return systemSnapshot;
        } catch (Exception e) {
            val ex = Exceptions.unwrap(e);
            if (ex instanceof EOFException) {
                log.error("SystemJournal[{}] Incomplete snapshot found, skipping {}.", containerId, snapshotInfo, e);
                throw new CompletionException(e);
            } else if (ex instanceof ChunkNotFoundException) {
                log.warn("SystemJournal[{}] Missing snapshot, skipping {}.", containerId, snapshotInfo, e);
            } else {
                log.error("SystemJournal[{}] Error with snapshot, skipping {}.", containerId, snapshotInfo, e);
                throw new CompletionException(e);
            }
        }
        return null;
    }

    /**
     * Recreates in-memory state from the given snapshot record.
     */
    private CompletableFuture<SystemSnapshotRecord> applySystemSnapshotRecord(MetadataTransaction txn,
                                                                              BootstrapState state,
                                                                              SystemSnapshotRecord systemSnapshot) {

        //Now apply
        if (null != systemSnapshot) {
            // validate
            systemSnapshot.checkInvariants();
            // reset all state so far
            txn.getData().clear();
            state.finalTruncateOffsets.clear();
            state.finalFirstChunkStartsAtOffsets.clear();
            state.chunkStartOffsets.clear();

            log.debug("SystemJournal[{}] Applying snapshot that includes journals up to epoch={} journal index={}", containerId,
                    systemSnapshot.epoch, systemSnapshot.fileIndex);
            log.trace("SystemJournal[{}] Processing system log snapshot {}.", containerId, systemSnapshot);
            // Initialize the segments and their chunks.
            for (val segmentSnapshot : systemSnapshot.segmentSnapshotRecords) {
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
                for (val metadata : segmentSnapshot.chunkMetadataCollection) {
                    txn.create(metadata);

                    // make sure that the record is marked pinned.
                    txn.markPinned(metadata);
                    state.chunkStartOffsets.put(metadata.getName(), offset);
                    offset += metadata.getLength();
                }
            }
        } else {
            log.debug("SystemJournal[{}] No previous snapshot present.", containerId);
            // Initialize with default values.
            for (val systemSegment : systemSegments) {
                val segmentMetadata = SegmentMetadata.builder()
                        .name(systemSegment)
                        .ownerEpoch(epoch)
                        .maxRollinglength(config.getStorageMetadataRollingPolicy().getMaxLength())
                        .build();
                segmentMetadata.setActive(true)
                        .setOwnershipChanged(true)
                        .setStorageSystemSegment(true);
                segmentMetadata.checkInvariants();
                txn.create(segmentMetadata);
                txn.markPinned(segmentMetadata);
            }
        }

        // Validate
        return validateSystemSnapshotExistsInTxn(txn, systemSnapshot)
                .thenApplyAsync(v -> {
                    log.debug("SystemJournal[{}] Done applying snapshots.", containerId);
                    return systemSnapshot;
                }, executor);
    }

    /**
     * Validate that all information in given snapshot exists in transaction.
     */
    private CompletableFuture<Void> validateSystemSnapshotExistsInTxn(MetadataTransaction txn, SystemSnapshotRecord systemSnapshot) {
        if (null == systemSnapshot) {
            return CompletableFuture.completedFuture(null);
        }
        val iterator = systemSnapshot.getSegmentSnapshotRecords().iterator();
        // For each segment in snapshot
        return Futures.loop(
                () -> iterator.hasNext(),
                () -> {
                        val segmentSnapshot = iterator.next();
                        return txn.get(segmentSnapshot.segmentMetadata.getKey())
                                .thenComposeAsync(m -> validateChunksInSegmentSnapshot(txn, segmentSnapshot), executor)
                                .thenComposeAsync(vv -> validateSegment(txn, segmentSnapshot.segmentMetadata.getKey()), executor);

                    }, executor);
    }

    private CompletableFuture<Void> validateChunksInSegmentSnapshot(MetadataTransaction txn, SegmentSnapshotRecord segmentSnapshot) {
        val iterator = segmentSnapshot.getChunkMetadataCollection().iterator();
        // For each chunk in the segment
        return Futures.loop(
                () -> iterator.hasNext(),
                () -> {
                    val m = iterator.next();
                    return txn.get(m.getKey())
                            .thenAcceptAsync(mm -> Preconditions.checkState(null != mm, "Chunk metadata must not be null."), executor);
                }, executor);
    }

    /**
     * Validate that all information for given segment in given snapshot exists in transaction.
     */
    private CompletableFuture<Void> validateSegment(MetadataTransaction txn, String segmentName) {
        return txn.get(segmentName)
                .thenComposeAsync(m -> {
                    val segmentMetadata = (SegmentMetadata) m;
                    Preconditions.checkState(null != segmentMetadata, "Segment metadata must not be null.");
                    val chunkName = new AtomicReference<>(segmentMetadata.getFirstChunk());
                    return Futures.loop(
                            () -> chunkName.get() != null,
                            () -> txn.get(chunkName.get())
                                    .thenAcceptAsync(mm -> {
                                        Preconditions.checkState(null != mm, "Chunk metadata must not be null.");
                                        val chunkMetadata = (ChunkMetadata) mm;
                                        chunkName.set(chunkMetadata.getNextChunk());
                                    }, executor),
                            executor);
                }, executor);
    }

    /**
     * Read contents from file.
     */
    private CompletableFuture<byte[]> getContents(String chunkPath, boolean suppressExceptionWarning) {
        return chunkStorage.getInfo(chunkPath)
                .thenComposeAsync(info -> getContents(chunkPath, info.getLength(), suppressExceptionWarning), executor);
    }

    /**
     * Read contents from file.
     */
    private CompletableFuture<byte[]> getContents(String chunkPath, long length, boolean suppressExceptionWarning) {
        val isReadDone = new AtomicBoolean();
        val shouldBreak = new AtomicBoolean();
        val attempt = new AtomicInteger();
        val lastException = new AtomicReference<Throwable>();
        val retValue = new AtomicReference<byte[]>();
        // Try config.getMaxJournalReadAttempts() times.
        return Futures.loop(
                () -> attempt.get() < config.getMaxJournalReadAttempts() && !isReadDone.get() && !shouldBreak.get(),
                () -> readFully(chunkPath, retValue, length)
                        .handleAsync((v, e) -> {
                            attempt.incrementAndGet();
                            if (e != null) {
                                // record the exception
                                lastException.set(e);
                                val ex = Exceptions.unwrap(e);
                                boolean shouldLog = true;
                                if (!shouldRetry(ex)) {
                                    shouldBreak.set(true);
                                    shouldLog = !suppressExceptionWarning;
                                }
                                if (shouldLog) {
                                    log.warn("SystemJournal[{}] Error while reading journal {}. Attempt#{}", containerId, chunkPath, attempt.get(), lastException.get());
                                }
                                return null;
                            } else {
                                // no exception, we are done reading. Return the value.
                                isReadDone.set(true);
                                return v;
                            }
                        }, executor),
                executor)
                .handleAsync((v, e) -> {
                    // If read is not done and we have exception then throw.
                    if (shouldBreak.get() || (!isReadDone.get() && lastException.get() != null)) {
                        throw new CompletionException(lastException.get());
                    }
                    return v;
                }, executor)
                .thenApplyAsync(v -> retValue.get(), executor);
    }

    /**
     * Returns whether operation should be retried after given exception.
     */
    private boolean shouldRetry(Throwable ex) {
        // Skip retry if we know chunk does not exist.
        return !(ex instanceof ChunkNotFoundException);
    }

    /**
     * Read given chunk in its entirety.
     */
    private CompletableFuture<Void> readFully(String chunkPath, AtomicReference<byte[]> retValue, long length) {
        val h = ChunkHandle.readHandle(chunkPath);
        // Allocate buffer to read into.
        retValue.set(new byte[Math.toIntExact(length)]);
        if (length == 0) {
            log.warn("SystemJournal[{}] journal {} is empty.", containerId, chunkPath);
            return CompletableFuture.completedFuture(null);
        }

        val fromOffset = new AtomicLong();
        val remaining = new AtomicInteger(retValue.get().length);

        // Continue until there is still data remaining to be read.
        return Futures.loop(
                () -> remaining.get() > 0,
                () -> chunkStorage.read(h, fromOffset.get(), remaining.get(), retValue.get(), Math.toIntExact(fromOffset.get())),
                bytesRead -> {
                    Preconditions.checkState(0 != bytesRead, "bytesRead must not be 0");
                    remaining.addAndGet(-bytesRead);
                    fromOffset.addAndGet(bytesRead);
                },
                executor);
    }

    /**
     * Process all systemLog entries to recreate the state of metadata storage system segments.
     */
    private CompletableFuture<Void> applySystemLogOperations(MetadataTransaction txn,
                                                             BootstrapState state,
                                                             SystemSnapshotRecord systemSnapshotRecord) {

        val epochToStartScanning = new AtomicLong();
        val fileIndexToRecover = new AtomicInteger(1);
        val journalsProcessed = new Vector<String>();
        // Starting with journal file after last snapshot,
        if (null != systemSnapshotRecord) {
            epochToStartScanning.set(systemSnapshotRecord.epoch);
            fileIndexToRecover.set(systemSnapshotRecord.fileIndex + 1);
        }
        log.debug("SystemJournal[{}] Applying journal operations. Starting at epoch={}  journal index={}", containerId,
                epochToStartScanning.get(), fileIndexToRecover.get());
        // Linearly read and apply all the journal files after snapshot.
        val epochToRecover = new AtomicLong(epochToStartScanning.get());
        return Futures.loop(
                () -> epochToRecover.get() < epoch,
                () -> {
                    // Start scan with file index 1 if epoch is later than snapshot.
                    if (epochToRecover.get() > epochToStartScanning.get()) {
                        fileIndexToRecover.set(1);
                    }

                    // Process one journal at a time.
                    val scanAhead = new AtomicInteger();
                    val isScanDone = new AtomicBoolean();
                    return Futures.loop(
                            () -> !isScanDone.get(),
                            () -> {
                                val systemLogName = getSystemJournalChunkName(containerId, epochToRecover.get(), fileIndexToRecover.get());
                                return getContents(systemLogName, true)
                                        .thenApplyAsync(contents -> {
                                            // We successfully read the contents.
                                            journalsProcessed.add(systemLogName);
                                            // Reset scan ahead counter.
                                            scanAhead.set(0);
                                            return contents;
                                        }, executor)
                                        // Apply record batches from the file.
                                        .thenComposeAsync(contents ->  processJournalContents(txn, state, systemLogName, new ByteArrayInputStream(contents)), executor)
                                        .handleAsync((v, e) -> {
                                            if (null != e) {
                                                val ex = Exceptions.unwrap(e);
                                                if (ex instanceof ChunkNotFoundException) {
                                                    // Journal chunk does not exist.
                                                    log.debug("SystemJournal[{}] Journal does not exist for epoch={}. Last journal index={}",
                                                            containerId, epochToRecover.get(), fileIndexToRecover.get());

                                                    // Check whether we have reached end of our scanning (including scan ahead).
                                                    if (scanAhead.incrementAndGet() > config.getMaxJournalWriteAttempts()) {
                                                        isScanDone.set(true);
                                                        log.debug("SystemJournal[{}] Done applying journal operations for epoch={}. Last journal index={}",
                                                                containerId, epochToRecover.get(), fileIndexToRecover.get());
                                                        return null;
                                                    }
                                                } else {
                                                    throw new CompletionException(e);
                                                }
                                            }

                                            // Move to next journal.
                                            fileIndexToRecover.incrementAndGet();
                                            state.filesProcessedCount.incrementAndGet();
                                            return v;
                                        }, executor);
                            },
                            executor);
                },
                v -> epochToRecover.incrementAndGet(),
                executor)
                .thenRunAsync(() -> pendingGarbageChunks.addAll(journalsProcessed), executor);
    }

    private CompletableFuture<Void> processJournalContents(MetadataTransaction txn, BootstrapState state, String systemLogName, ByteArrayInputStream input) {
        // Loop is exited with eventual EOFException.
        val isBatchDone = new AtomicBoolean();
        log.debug("SystemJournal[{}] Processing journal {}.", containerId, systemLogName);
        return Futures.loop(
                () -> !isBatchDone.get(),
                () -> {
                    try {
                        val batch = BATCH_SERIALIZER.deserialize(input);
                        val iterator = batch.getSystemJournalRecords().iterator();
                        return Futures.loop(
                                () -> iterator.hasNext(),
                                () -> {
                                    val record = iterator.next();
                                    return applyRecord(txn, state, record);
                                },
                                executor);
                    } catch (EOFException e) {
                        log.debug("SystemJournal[{}] Done processing journal {}.", containerId, systemLogName);
                        isBatchDone.set(true);
                    } catch (Exception e) {
                        log.error("SystemJournal[{}] Error while processing journal {}.", containerId, systemLogName, e);
                        throw new CompletionException(e);
                    }
                    return CompletableFuture.completedFuture(null);
                },
                executor
        );
    }

    /**
     * Apply given {@link SystemJournalRecord}
     */
    private CompletableFuture<Void> applyRecord(MetadataTransaction txn,
                                                BootstrapState state,
                                                SystemJournalRecord record) {
        log.debug("SystemJournal[{}] Processing system log record ={}.", containerId, record);
        if (state.visitedRecords.contains(record)) {
            log.debug("SystemJournal[{}] Duplicate record ={}.", containerId, record);
            return CompletableFuture.completedFuture(null);
        }
        state.visitedRecords.add(record);
        state.recordsProcessedCount.incrementAndGet();
        final CompletableFuture<Void> retValue;
        // ChunkAddedRecord.
        if (record instanceof ChunkAddedRecord) {
            val chunkAddedRecord = (ChunkAddedRecord) record;
            retValue = applyChunkAddition(txn, state.chunkStartOffsets,
                    chunkAddedRecord.getSegmentName(),
                    nullToEmpty(chunkAddedRecord.getOldChunkName()),
                    chunkAddedRecord.getNewChunkName(),
                    chunkAddedRecord.getOffset());
        } else if (record instanceof AppendRecord) {
            val appendRecord = (AppendRecord) record;
            retValue = applyAppend(txn, appendRecord.getSegmentName(),
                    appendRecord.getChunkName(),
                    appendRecord.getOffset(),
                    appendRecord.getLength());
        } else if (record instanceof TruncationRecord) {
            // TruncationRecord.
            val truncationRecord = (TruncationRecord) record;
            state.finalTruncateOffsets.put(truncationRecord.getSegmentName(), truncationRecord.getOffset());
            state.finalFirstChunkStartsAtOffsets.put(truncationRecord.getSegmentName(), truncationRecord.getStartOffset());
            retValue = CompletableFuture.completedFuture(null);
        } else if (record instanceof SystemSnapshotRecord) {
            val snapshotRecord = (SystemSnapshotRecord) record;
            retValue = Futures.toVoid(applySystemSnapshotRecord(txn, state, snapshotRecord));
        } else {
            // Unknown record.
            retValue = CompletableFuture.failedFuture(new IllegalStateException(String.format("Unknown record type encountered. record = %s", record)));
        }
        return retValue;
    }

    /**
     * Adjusts the lengths of last chunks for each segment.
     */
    private CompletableFuture<Void> adjustLastChunkLengths(MetadataTransaction txn) {
        val futures = new ArrayList<CompletableFuture<Void>>();
        for (val systemSegment : systemSegments) {
            val f = txn.get(systemSegment)
                    .thenComposeAsync(m -> {
                        val segmentMetadata = (SegmentMetadata) m;
                        segmentMetadata.checkInvariants();
                        final CompletableFuture<Void> ff;
                        // Update length of last chunk in metadata to what we actually find on LTS.
                        if (!segmentMetadata.isAtomicWrite() && null != segmentMetadata.getLastChunk()) {
                            ff = chunkStorage.getInfo(segmentMetadata.getLastChunk())
                                    .thenComposeAsync(chunkInfo -> {
                                        val length = chunkInfo.getLength();
                                        return txn.get(segmentMetadata.getLastChunk())
                                                .thenAcceptAsync(mm -> {
                                                    val lastChunk = (ChunkMetadata) mm;
                                                    Preconditions.checkState(null != lastChunk, "lastChunk must not be null. Segment=%s", segmentMetadata);
                                                    Preconditions.checkState(chunkInfo.getLength() >= lastChunk.getLength(),
                                                            "Length of last chunk on LTS should not be less than what is in metadata. Chunk=%s length=%s",
                                                            lastChunk, chunkInfo.getLength());
                                                    if (length > lastChunk.getLength()) {
                                                        lastChunk.setLength(length);
                                                        txn.update(lastChunk);
                                                        val newLength = segmentMetadata.getLastChunkStartOffset() + length;
                                                        segmentMetadata.setLength(newLength);
                                                        log.debug("SystemJournal[{}] Adjusting length of last chunk segment. segment={}, length={} chunk={}, chunk length={}",
                                                                containerId, segmentMetadata.getName(), length, lastChunk.getName(), newLength);
                                                    }

                                                }, executor);
                                    }, executor);
                        } else {
                            ff = CompletableFuture.completedFuture(null);
                        }
                        return ff.thenApplyAsync(v -> {
                            // Always set as atomic write
                            segmentMetadata.setAtomicWrites(true);
                            Preconditions.checkState(segmentMetadata.isOwnershipChanged(), "ownershipChanged must be true. Segment=%s", segmentMetadata);
                            segmentMetadata.checkInvariants();
                            return segmentMetadata;
                        }, executor);
                    }, executor)
                    .thenAcceptAsync(segmentMetadata -> txn.update(segmentMetadata), executor);
            futures.add(f);
        }
        return Futures.allOf(futures);
    }

    /**
     * Apply last effective truncate offsets.
     */
    private CompletableFuture<Void> applyFinalTruncateOffsets(MetadataTransaction txn,
                                                              BootstrapState state) {
        val futures = new ArrayList<CompletableFuture<Void>>();
        for (val systemSegment : systemSegments) {
            if (state.finalTruncateOffsets.containsKey(systemSegment)) {
                val truncateAt = state.finalTruncateOffsets.get(systemSegment);
                val firstChunkStartsAt = state.finalFirstChunkStartsAtOffsets.get(systemSegment);
                futures.add(applyTruncate(txn, systemSegment, truncateAt, firstChunkStartsAt));
            }
        }
        return Futures.allOf(futures);
    }

    /**
     * Apply chunk addition.
     */
    private CompletableFuture<Void> applyChunkAddition(MetadataTransaction txn, Map<String, Long> chunkStartOffsets, String segmentName, String oldChunkName, String newChunkName, long offset) {
        Preconditions.checkState(null != oldChunkName, "oldChunkName must not be null");
        Preconditions.checkState(null != newChunkName && !newChunkName.isEmpty(), "newChunkName must not be null or empty");

        return validateSegment(txn, segmentName)
                .thenComposeAsync(v -> txn.get(segmentName), executor)
                .thenComposeAsync(m -> {
                    val segmentMetadata = (SegmentMetadata) m;
                    segmentMetadata.checkInvariants();
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
                                    val toDelete = new AtomicReference<>(oldChunk.getNextChunk());

                                    return Futures.loop(
                                            () -> toDelete.get() != null,
                                            () -> txn.get(toDelete.get())
                                                    .thenAcceptAsync(mmm -> {
                                                        val chunkToDelete = (ChunkMetadata) mmm;
                                                        txn.delete(toDelete.get());
                                                        segmentMetadata.setChunkCount(segmentMetadata.getChunkCount() - 1);
                                                        // move to next chunk in list of now zombie chunks
                                                        toDelete.set(chunkToDelete.getNextChunk());
                                                    }, executor),
                                            executor)
                                            .thenAcceptAsync(v -> {
                                                // Set next chunk
                                                oldChunk.setNextChunk(newChunkName);

                                                // Set length
                                                val oldLength = chunkStartOffsets.get(oldChunkName);
                                                oldChunk.setLength(offset - oldLength);

                                                txn.update(oldChunk);
                                            }, executor);
                                }, executor);
                    } else {
                        segmentMetadata.setFirstChunk(newChunkName);
                        segmentMetadata.setStartOffset(offset);
                        Preconditions.checkState(segmentMetadata.getChunkCount() == 0, "Chunk count must be 0. %s", segmentMetadata);
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
                            return validateSegment(txn, segmentName);
                        } else {
                            return CompletableFuture.completedFuture(null);
                        }
                    }, executor);
                }, executor);
    }

    /**
     * Apply chunk append.
     */
    private CompletableFuture<Void> applyAppend(MetadataTransaction txn, String segmentName, String chunkName, long offset, long length) {
        Preconditions.checkState(null != chunkName && !chunkName.isEmpty(), "chunkName must not be null or empty");
        Preconditions.checkState(offset >= 0, "offset must be non-negative");
        Preconditions.checkState(length > 0, "length must be positive");

        return validateSegment(txn, segmentName)
                .thenComposeAsync(v -> txn.get(segmentName), executor)
                .thenComposeAsync(m -> {
                    val segmentMetadata = (SegmentMetadata) m;
                    segmentMetadata.checkInvariants();
                    // set length.
                    return txn.get(segmentMetadata.getLastChunk())
                            .thenAcceptAsync( metadata -> {
                                Preconditions.checkState(metadata != null, "metadata must not be null");
                                ChunkMetadata lastChunkMetadata = (ChunkMetadata) metadata;
                                Preconditions.checkState(lastChunkMetadata.getName().equals(chunkName), "Invalid chunk name expected= %s actual=%s in segment=%s",
                                        lastChunkMetadata.getName(), chunkName, segmentMetadata);
                                Preconditions.checkState(lastChunkMetadata.getLength() == offset, "Invalid offset expected= %s actual=%s in segment=%s",
                                        lastChunkMetadata.getLength(), offset, segmentMetadata);
                                segmentMetadata.setLength(segmentMetadata.getLength() + length);
                                lastChunkMetadata.setLength(lastChunkMetadata.getLength() + length);
                                segmentMetadata.setAtomicWrites(true);
                                txn.update(segmentMetadata);
                                txn.update(lastChunkMetadata);
                                log.debug("SystemJournal[{}] Appending to last chunk. segment={}, segment length={} chunk={}, chunk length={}",
                                        containerId, segmentMetadata.getName(), segmentMetadata.getLength(), lastChunkMetadata.getName(), lastChunkMetadata.getLength());

                            }, executor);

                }, executor);
    }

    /**
     * Apply truncate action to the segment metadata.
     */
    private CompletableFuture<Void> applyTruncate(MetadataTransaction txn, String segmentName, long truncateAt, long firstChunkStartsAt) {
        return txn.get(segmentName)
                .thenComposeAsync(metadata -> {
                    val segmentMetadata = (SegmentMetadata) metadata;
                    segmentMetadata.checkInvariants();
                    val currentChunkName = new AtomicReference<>(segmentMetadata.getFirstChunk());
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
                .segmentSnapshotRecords(new Vector<>())
                .build();

        val futures = new ArrayList<CompletableFuture<Void>>();
        for (val systemSegment : systemSegments) {
            // Find segment metadata.
            val future = txn.get(systemSegment)
                    .thenComposeAsync(metadata -> {
                        val segmentMetadata = (SegmentMetadata) metadata;
                        segmentMetadata.checkInvariants();

                        val segmentSnapshot = SegmentSnapshotRecord.builder()
                                .segmentMetadata(segmentMetadata)
                                .chunkMetadataCollection(new Vector<>())
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
                                    systemSnapshot.segmentSnapshotRecords.add(segmentSnapshot);
                                }, executor);
                    }, executor);
            futures.add(future);
        }
        return Futures.allOf(futures)
                .thenApplyAsync(vv -> {
                    systemSnapshot.checkInvariants();
                    return systemSnapshot;
                }, executor);
    }

    /**
     * Writes {@link SystemSnapshotRecord}.
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
                            .thenComposeAsync(v -> getContents(snapshotFile, bytes.getLength() - bytes.arrayOffset(), false)
                                            .thenAcceptAsync(contents -> {
                                                try {
                                                    val snapshotReadback = SYSTEM_SNAPSHOT_SERIALIZER.deserialize(contents);
                                                    if (config.isSelfCheckEnabled()) {
                                                        snapshotReadback.checkInvariants();
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
                                    // Add failed file as garbage.
                                    pendingGarbageChunks.add(snapshotFile);
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
                        pendingGarbageChunks.add(h.getChunkName());
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
                        .serializer(SegmentSnapshotRecord.class, 4, new SegmentSnapshotRecord.Serializer())
                        .serializer(AppendRecord.class, 5, new AppendRecord.Serializer());
            }
        }
    }

    /**
     * Represents a system journal record.
     */
    @Builder(toBuilder = true)
    @Data
    @EqualsAndHashCode
    public static class SystemJournalRecordBatch {
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
            private static final RevisionDataOutput.ElementSerializer<SystemJournalRecord> ELEMENT_SERIALIZER = SERIALIZER::serialize;
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
                b.systemJournalRecords(input.readCollection(ELEMENT_DESERIALIZER, () -> new Vector<>()));
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
     * Journal record for segment append.
     */
    @Builder(toBuilder = true)
    @Data
    @EqualsAndHashCode(callSuper = true)
    static class AppendRecord extends SystemJournalRecord {
        /**
         * Name of the segment.
         */
        @NonNull
        private final String segmentName;

        /**
         * Offset at which chunk is appended.
         */
        private final long offset;

        /**
         * Size of append.
         */
        private final long length;

        /**
         * Name of the chunk.
         */
        @NonNull
        private final String chunkName;

        /**
         * Builder that implements {@link ObjectBuilder}.
         */
        public static class AppendRecordBuilder implements ObjectBuilder<AppendRecord> {
        }

        /**
         * Serializer that implements {@link VersionedSerializer}.
         */
        public static class Serializer extends VersionedSerializer.WithBuilder<AppendRecord, AppendRecord.AppendRecordBuilder> {
            @Override
            protected AppendRecord.AppendRecordBuilder newBuilder() {
                return AppendRecord.builder();
            }

            @Override
            protected byte getWriteVersion() {
                return 0;
            }

            @Override
            protected void declareVersions() {
                version(0).revision(0, this::write00, this::read00);
            }

            private void write00(AppendRecord object, RevisionDataOutput output) throws IOException {
                output.writeUTF(object.segmentName);
                output.writeUTF(object.chunkName);
                output.writeCompactLong(object.offset);
                output.writeCompactLong(object.length);
            }

            private void read00(RevisionDataInput input, AppendRecord.AppendRecordBuilder b) throws IOException {
                b.segmentName(input.readUTF());
                b.chunkName(input.readUTF());
                b.offset(input.readCompactLong());
                b.length(input.readCompactLong());
            }
        }
    }

    /**
     * Journal record for segment snapshot.
     */
    @Builder(toBuilder = true)
    @Data
    @EqualsAndHashCode(callSuper = true)
    public static class SegmentSnapshotRecord extends SystemJournalRecord {
        /**
         * Data about the segment.
         */
        @NonNull
        private final SegmentMetadata segmentMetadata;

        @NonNull
        private final Collection<ChunkMetadata> chunkMetadataCollection;

        /**
         * Check invariants.
         */
        public void checkInvariants() {
            segmentMetadata.checkInvariants();
            Preconditions.checkState(segmentMetadata.isStorageSystemSegment(),
                    "Segment must be storage segment. Segment snapshot= %s", this);
            Preconditions.checkState(segmentMetadata.getChunkCount() == chunkMetadataCollection.size(),
                    "Chunk count must match. Segment snapshot= %s", this);

            long dataSize = 0;
            ChunkMetadata previous = null;
            ChunkMetadata firstChunk = null;
            for (val metadata : getChunkMetadataCollection()) {
                dataSize += metadata.getLength();
                if (previous != null) {
                    Preconditions.checkState(previous.getNextChunk().equals(metadata.getName()),
                            "In correct link . chunk %s must point to chunk %s. Segment snapshot= %s",
                            previous.getName(), metadata.getName(), this);
                } else {
                    firstChunk = metadata;
                }
                previous = metadata;
            }
            Preconditions.checkState(dataSize == segmentMetadata.getLength() - segmentMetadata.getFirstChunkStartOffset(),
                    "Data size does not match dataSize (%s). Segment=%s", dataSize, segmentMetadata);

            if (chunkMetadataCollection.size() > 0) {
                Preconditions.checkState(segmentMetadata.getFirstChunk().equals(firstChunk.getName()),
                        "First chunk name is wrong. Segment snapshot= %s", this);
                Preconditions.checkState(segmentMetadata.getLastChunk().equals(previous.getName()),
                        "Last chunk name is wrong. Segment snapshot= %s", this);
                Preconditions.checkState(previous.getNextChunk() == null,
                        "Invalid last chunk Segment snapshot= %s", this);
                Preconditions.checkState(segmentMetadata.getLength() == segmentMetadata.getLastChunkStartOffset() + previous.getLength(),
                        "Last chunk start offset is wrong. snapshot= %s", this);
            }
        }

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
            private static final RevisionDataOutput.ElementSerializer<ChunkMetadata> ELEMENT_SERIALIZER = CHUNK_METADATA_SERIALIZER::serialize;
            private static final RevisionDataInput.ElementDeserializer<ChunkMetadata> ELEMENT_DESERIALIZER = dataInput -> {
                return (ChunkMetadata) CHUNK_METADATA_SERIALIZER.deserialize(dataInput.getBaseStream());
            };

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
                b.chunkMetadataCollection(input.readCollection(ELEMENT_DESERIALIZER, () -> new Vector<>()));
            }
        }
    }

    /**
     * Journal record for segment snapshot.
     */
    @Builder(toBuilder = true)
    @Data
    @EqualsAndHashCode(callSuper = true)
    public static class SystemSnapshotRecord extends SystemJournalRecord {
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
         * Check invariants.
         */
        public void checkInvariants() {
            for (val segmentSnapshot : getSegmentSnapshotRecords()) {
                segmentSnapshot.checkInvariants();
            }
        }

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
            private static final RevisionDataOutput.ElementSerializer<SegmentSnapshotRecord> ELEMENT_SERIALIZER = CHUNK_METADATA_SERIALIZER::serialize;
            private static final RevisionDataInput.ElementDeserializer<SegmentSnapshotRecord> ELEMENT_DESERIALIZER = dataInput -> CHUNK_METADATA_SERIALIZER.deserialize(dataInput.getBaseStream());

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
                b.segmentSnapshotRecords(input.readCollection(ELEMENT_DESERIALIZER, () -> new Vector<>()));
            }
        }
    }

}
