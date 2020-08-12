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

import com.google.common.annotations.Beta;
import com.google.common.base.Preconditions;
import io.pravega.common.Exceptions;
import io.pravega.common.LoggerHelpers;
import io.pravega.common.Timer;
import io.pravega.common.io.BoundedInputStream;
import io.pravega.common.util.ImmutableDate;
import io.pravega.segmentstore.contracts.BadOffsetException;
import io.pravega.segmentstore.contracts.SegmentProperties;
import io.pravega.segmentstore.contracts.StreamSegmentExistsException;
import io.pravega.segmentstore.contracts.StreamSegmentInformation;
import io.pravega.segmentstore.contracts.StreamSegmentNotExistsException;
import io.pravega.segmentstore.contracts.StreamSegmentSealedException;
import io.pravega.segmentstore.contracts.StreamSegmentTruncatedException;
import io.pravega.segmentstore.storage.SegmentHandle;
import io.pravega.segmentstore.storage.SegmentRollingPolicy;
import io.pravega.segmentstore.storage.Storage;
import io.pravega.segmentstore.storage.StorageNotPrimaryException;
import io.pravega.segmentstore.storage.metadata.ChunkMetadata;
import io.pravega.segmentstore.storage.metadata.ChunkMetadataStore;
import io.pravega.segmentstore.storage.metadata.MetadataTransaction;
import io.pravega.segmentstore.storage.metadata.SegmentMetadata;
import io.pravega.segmentstore.storage.metadata.StorageMetadataAlreadyExistsException;
import io.pravega.segmentstore.storage.metadata.StorageMetadataException;
import io.pravega.segmentstore.storage.metadata.StorageMetadataWritesFencedOutException;
import io.pravega.shared.NameUtils;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/**
 * Implements storage for segments using {@link ChunkStorage} and {@link ChunkMetadataStore}.
 * The metadata about the segments is stored in metadataStore using two types of records {@link SegmentMetadata} and {@link ChunkMetadata}.
 * Any changes to layout must be made inside a {@link MetadataTransaction} which will atomically change the records upon
 * {@link MetadataTransaction#commit()}.
 * Detailed design is documented here https://github.com/pravega/pravega/wiki/PDP-34:-Simplified-Tier-2
 */
@Slf4j
@Beta
public class ChunkedSegmentStorage implements Storage {
    /**
     * Configuration options for this ChunkedSegmentStorage instance.
     */
    @Getter
    private final ChunkedSegmentStorageConfig config;

    /**
     * Metadata store containing all storage data.
     * Initialized by segment container via {@link ChunkedSegmentStorage#bootstrap(int, ChunkMetadataStore)}.
     */
    @Getter
    private ChunkMetadataStore metadataStore;

    /**
     * Underlying {@link ChunkStorage} to use to read and write data.
     */
    @Getter
    private final ChunkStorage chunkStorage;

    /**
     * Storage executor object.
     */
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
    private long epoch;

    /**
     * Id of the current Container.
     * Initialized by segment container via {@link ChunkedSegmentStorage#bootstrap(int, ChunkMetadataStore)}.
     */
    @Getter
    private int containerId;

    /**
     * {@link SystemJournal} that logs all changes to system segment layout so that they can be are used during system bootstrap.
     */
    @Getter
    private SystemJournal systemJournal;

    /**
     * {@link ReadIndexCache} that has index of chunks by start offset
     */
    private final ReadIndexCache readIndexCache;

    /**
     * List of garbage chunks.
     */
    private final List<String> garbageChunks = new ArrayList<String>();

    /**
     * Prefix string to use for logging.
     */
    private String logPrefix;

    /**
     * Creates a new instance of the {@link ChunkedSegmentStorage} class.
     *
     * @param chunkStorage ChunkStorage instance.
     * @param executor     An Executor for async operations.
     * @param config       Configuration options for this ChunkedSegmentStorage instance.
     */
    public ChunkedSegmentStorage(ChunkStorage chunkStorage, Executor executor, ChunkedSegmentStorageConfig config) {
        this.config = Preconditions.checkNotNull(config, "config");
        this.chunkStorage = Preconditions.checkNotNull(chunkStorage, "chunkStorage");
        this.executor = Preconditions.checkNotNull(executor, "executor");
        this.readIndexCache = new ReadIndexCache(config.getMaxIndexedSegments(),
                config.getMaxIndexedChunksPerSegment(),
                config.getMaxIndexedChunks());
        this.closed = new AtomicBoolean(false);
    }

    /**
     * Creates a new instance of the ChunkedSegmentStorage class.
     *
     * @param chunkStorage  ChunkStorage instance.
     * @param metadataStore Metadata store.
     * @param executor      An Executor for async operations.
     * @param config        Configuration options for this ChunkedSegmentStorage instance.
     */
    public ChunkedSegmentStorage(ChunkStorage chunkStorage, ChunkMetadataStore metadataStore, Executor executor, ChunkedSegmentStorageConfig config) {
        this.config = Preconditions.checkNotNull(config, "config");
        this.chunkStorage = Preconditions.checkNotNull(chunkStorage, "chunkStorage");
        this.metadataStore = Preconditions.checkNotNull(metadataStore, "metadataStore");
        this.executor = Preconditions.checkNotNull(executor, "executor");
        this.readIndexCache = new ReadIndexCache(config.getMaxIndexedSegments(),
                config.getMaxIndexedChunksPerSegment(),
                config.getMaxIndexedChunks());
        this.closed = new AtomicBoolean(false);
    }

    /**
     * Initializes the ChunkedSegmentStorage and bootstrap the metadata about storage metadata segments by reading and processing the journal.
     *
     * @param metadataStore Metadata store.
     * @param containerId   container id.
     * @throws Exception In case of any errors.
     */
    public void bootstrap(int containerId, ChunkMetadataStore metadataStore) throws Exception {
        this.containerId = containerId;
        this.logPrefix = String.format("ChunkedSegmentStorage[%d]", containerId);
        this.metadataStore = Preconditions.checkNotNull(metadataStore, "metadataStore");
        this.systemJournal = new SystemJournal(containerId,
                epoch,
                chunkStorage,
                metadataStore,
                config);

        // Now bootstrap
        log.info("{} STORAGE BOOT: Started.", logPrefix);
        this.systemJournal.bootstrap();
        log.info("{} STORAGE BOOT: Ended.", logPrefix);
    }

    @Override
    public void initialize(long containerEpoch) {
        this.epoch = containerEpoch;
    }

    @Override
    public CompletableFuture<SegmentHandle> openWrite(String streamSegmentName) {
        checkInitialized();
        return execute(() -> {
            long traceId = LoggerHelpers.traceEnter(log, "openWrite", streamSegmentName);
            Preconditions.checkNotNull(streamSegmentName, "streamSegmentName");
            try (MetadataTransaction txn = metadataStore.beginTransaction()) {
                SegmentMetadata segmentMetadata = (SegmentMetadata) txn.get(streamSegmentName);
                checkSegmentExists(streamSegmentName, segmentMetadata);
                segmentMetadata.checkInvariants();
                // This segment was created by an older segment store. Need to start a fresh new chunk.
                if (segmentMetadata.getOwnerEpoch() < this.epoch) {
                    log.debug("{} openWrite - Segment needs ownership change - segment={}.", logPrefix, segmentMetadata.getName());
                    claimOwnership(txn, segmentMetadata);
                }
                // If created by newer instance then abort.
                checkOwnership(streamSegmentName, segmentMetadata);

                // This instance is the owner, return a handle.
                val retValue = SegmentStorageHandle.writeHandle(streamSegmentName);
                LoggerHelpers.traceLeave(log, "openWrite", traceId, retValue);
                return retValue;
            } catch (StorageMetadataWritesFencedOutException ex) {
                throw new StorageNotPrimaryException(streamSegmentName, ex);
            }
        });
    }

    /**
     * Checks ownership and adjusts the length of the segment if required.
     *
     * @param txn             Active {@link MetadataTransaction}.
     * @param segmentMetadata {@link SegmentMetadata} for the segment to change ownership for.
     * @throws ChunkStorageException In case of any chunk storage related errors.
     * @throws StorageMetadataException In case of any chunk metadata store related errors.
     */
    private void claimOwnership(MetadataTransaction txn, SegmentMetadata segmentMetadata) throws ChunkStorageException, StorageMetadataException {

        // Get the last chunk
        String lastChunkName = segmentMetadata.getLastChunk();
        if (null != lastChunkName) {
            ChunkMetadata lastChunk = (ChunkMetadata) txn.get(lastChunkName);
            log.debug("{} claimOwnership - current last chunk - segment={}, last chunk={}, Length={}.",
                    logPrefix,
                    segmentMetadata.getName(),
                    lastChunk.getName(),
                    lastChunk.getLength());
            try {
                ChunkInfo chunkInfo = chunkStorage.getInfo(lastChunkName);
                Preconditions.checkState(chunkInfo != null);
                Preconditions.checkState(lastChunk != null);
                // Adjust its length;
                if (chunkInfo.getLength() != lastChunk.getLength()) {
                    Preconditions.checkState(chunkInfo.getLength() > lastChunk.getLength());
                    // Whatever length you see right now is the final "sealed" length of the last chunk.
                    lastChunk.setLength(chunkInfo.getLength());
                    segmentMetadata.setLength(segmentMetadata.getLastChunkStartOffset() + lastChunk.getLength());
                    txn.update(lastChunk);
                    log.debug("{} claimOwnership - Length of last chunk adjusted - segment={}, last chunk={}, Length={}.",
                            logPrefix,
                            segmentMetadata.getName(),
                            lastChunk.getName(),
                            chunkInfo.getLength());
                }
            } catch (ChunkNotFoundException e) {
                // This probably means that this instance is fenced out and newer instance truncated this segment.
                // Try a commit of unmodified data to fail fast.
                log.debug("{} claimOwnership - Last chunk was missing, failing fast - segment={}, last chunk={}.",
                        logPrefix,
                        segmentMetadata.getName(),
                        lastChunk.getName());
                txn.update(segmentMetadata);
                txn.commit();
                throw e;
            }
        }

        // Claim ownership.
        // This is safe because the previous instance is definitely not an owner anymore. (even if this instance is no more owner)
        // If this instance is no more owner, then transaction commit will fail.So it is still safe.
        segmentMetadata.setOwnerEpoch(this.epoch);
        segmentMetadata.setOwnershipChanged(true);

        // Update and commit
        // If This instance is fenced this update will fail.
        txn.update(segmentMetadata);
        txn.commit();
    }

    @Override
    public CompletableFuture<SegmentHandle> create(String streamSegmentName, SegmentRollingPolicy rollingPolicy, Duration timeout) {
        checkInitialized();
        return execute(() -> {
            long traceId = LoggerHelpers.traceEnter(log, "create", streamSegmentName, rollingPolicy);
            Timer timer = new Timer();

            try (MetadataTransaction txn = metadataStore.beginTransaction()) {
                // Retrieve metadata and make sure it does not exist.
                SegmentMetadata oldSegmentMetadata = (SegmentMetadata) txn.get(streamSegmentName);
                if (null != oldSegmentMetadata) {
                    throw new StreamSegmentExistsException(streamSegmentName);
                }

                // Create a new record.
                SegmentMetadata newSegmentMetatadata = SegmentMetadata.builder()
                        .name(streamSegmentName)
                        .maxRollinglength(rollingPolicy.getMaxLength() == 0 ? SegmentRollingPolicy.NO_ROLLING.getMaxLength() : rollingPolicy.getMaxLength())
                        .ownerEpoch(this.epoch)
                        .build();

                newSegmentMetatadata.setActive(true);
                txn.create(newSegmentMetatadata);
                // commit.
                txn.commit();

                val retValue = SegmentStorageHandle.writeHandle(streamSegmentName);
                Duration elapsed = timer.getElapsed();
                log.debug("{} create - segment={}, rollingPolicy={}, latency={}.", logPrefix, streamSegmentName, rollingPolicy, elapsed.toMillis());
                LoggerHelpers.traceLeave(log, "create", traceId, retValue);
                return retValue;
            } catch (StorageMetadataAlreadyExistsException ex) {
                throw new StreamSegmentExistsException(streamSegmentName, ex);
            } catch (StorageMetadataWritesFencedOutException ex) {
                throw new StorageNotPrimaryException(streamSegmentName, ex);
            }
        });
    }

    @Override
    public CompletableFuture<Void> write(SegmentHandle handle, long offset, InputStream data, int length, Duration timeout) {
        checkInitialized();
        return execute(() -> {
            long traceId = LoggerHelpers.traceEnter(log, "write", handle, offset, length);
            Timer timer = new Timer();

            // Validate preconditions.
            Preconditions.checkArgument(null != handle, "handle");
            Preconditions.checkArgument(null != data, "data");
            String streamSegmentName = handle.getSegmentName();
            Preconditions.checkArgument(null != streamSegmentName, "streamSegmentName");
            Preconditions.checkArgument(!handle.isReadOnly(), "handle");
            Preconditions.checkArgument(offset >= 0, "offset");
            Preconditions.checkArgument(length >= 0, "length");

            ArrayList<SystemJournal.SystemJournalRecord> systemLogRecords = new ArrayList<>();
            List<ChunkNameOffsetPair> newReadIndexEntries = new ArrayList<ChunkNameOffsetPair>();
            int chunksAddedCount = 0;
            boolean isCommited = false;

            try (MetadataTransaction txn = metadataStore.beginTransaction()) {
                boolean didSegmentLayoutChange = false;

                // Retrieve metadata.
                SegmentMetadata segmentMetadata = (SegmentMetadata) txn.get(streamSegmentName);

                // Validate preconditions.
                checkSegmentExists(streamSegmentName, segmentMetadata);
                segmentMetadata.checkInvariants();
                checkNotSealed(streamSegmentName, segmentMetadata);
                checkOwnership(streamSegmentName, segmentMetadata);

                // Validate that offset is correct.
                if ((segmentMetadata.getLength()) != offset) {
                    throw new BadOffsetException(streamSegmentName, segmentMetadata.getLength(), offset);
                }

                boolean isSystemSegment = isStorageSystemSegment(segmentMetadata);

                // Check if this is a first write after ownership changed.
                boolean isFirstWriteAfterFailover = segmentMetadata.isOwnershipChanged();

                ChunkMetadata lastChunkMetadata = null;
                ChunkHandle chunkHandle = null;
                int bytesRemaining = length;
                long currentOffset = offset;

                // Get the last chunk segmentMetadata for the segment.
                if (null != segmentMetadata.getLastChunk()) {
                    lastChunkMetadata = (ChunkMetadata) txn.get(segmentMetadata.getLastChunk());
                }

                while (bytesRemaining > 0) {
                    // Check if new chunk needs to be added.
                    // This could be either because there are no existing chunks or last chunk has reached max rolling length.
                    if (null == lastChunkMetadata
                            || (lastChunkMetadata.getLength() >= segmentMetadata.getMaxRollinglength())
                            || isFirstWriteAfterFailover
                            || !shouldAppend()) {

                        // Create new chunk
                        String newChunkName = getNewChunkName(streamSegmentName,
                                segmentMetadata.getLength());
                        chunkHandle = chunkStorage.create(newChunkName);

                        String previousLastChunkName = lastChunkMetadata == null ? null : lastChunkMetadata.getName();

                        // update first and last chunks.
                        lastChunkMetadata = updateMetadataForChunkAddition(txn,
                                segmentMetadata,
                                newChunkName,
                                isFirstWriteAfterFailover,
                                lastChunkMetadata);

                        // Record the creation of new chunk.
                        if (isSystemSegment) {
                            addSystemLogRecord(systemLogRecords,
                                    streamSegmentName,
                                    segmentMetadata.getLength(),
                                    previousLastChunkName,
                                    newChunkName);
                            txn.markPinned(lastChunkMetadata);
                        }
                        // Update read index.
                        newReadIndexEntries.add(new ChunkNameOffsetPair(segmentMetadata.getLength(), newChunkName));

                        isFirstWriteAfterFailover = false;
                        didSegmentLayoutChange = true;
                        chunksAddedCount++;

                        log.debug("{} write - New chunk added - segment={}, chunk={}, offset={}.",
                                logPrefix, streamSegmentName, newChunkName, segmentMetadata.getLength());
                    } else {
                        // No new chunk needed just write data to existing chunk.
                        chunkHandle = chunkStorage.openWrite(lastChunkMetadata.getName());
                    }

                    // Calculate the data that needs to be written.
                    long offsetToWriteAt = currentOffset - segmentMetadata.getLastChunkStartOffset();
                    int writeSize = (int) Math.min(bytesRemaining, segmentMetadata.getMaxRollinglength() - offsetToWriteAt);

                    // Write data to last chunk.
                    int bytesWritten = writeToChunk(txn,
                            segmentMetadata,
                            offset,
                            data,
                            chunkHandle,
                            lastChunkMetadata,
                            offsetToWriteAt,
                            writeSize);

                    // Update the counts
                    bytesRemaining -= bytesWritten;
                    currentOffset += bytesWritten;
                }

                // Check invariants.
                segmentMetadata.checkInvariants();

                // commit all system log records if required.
                if (isSystemSegment && chunksAddedCount > 0) {
                    // commit all system log records.
                    Preconditions.checkState(chunksAddedCount == systemLogRecords.size());
                    txn.setExternalCommitStep(() -> {
                        systemJournal.commitRecords(systemLogRecords);
                        return null;
                    });
                }

                // if layout did not change then commit with lazyWrite.
                txn.commit(!didSegmentLayoutChange);
                isCommited = true;

                // Post commit actions.
                // Update the read index.
                readIndexCache.addIndexEntries(streamSegmentName, newReadIndexEntries);

                Duration elapsed = timer.getElapsed();
                log.debug("{} write - segment={}, offset={}, length={}, latency={}.", logPrefix, handle.getSegmentName(), offset, length, elapsed.toMillis());
                LoggerHelpers.traceLeave(log, "write", traceId, handle, offset);
                return null;
            } catch (StorageMetadataWritesFencedOutException ex) {
                throw new StorageNotPrimaryException(streamSegmentName, ex);
            } finally {
                if (!isCommited && chunksAddedCount > 0) {
                    // Collect garbage.
                    collectGarbage(newReadIndexEntries.stream().map(entry -> entry.getChunkName()).collect(Collectors.toList()));
                }
            }
        });
    }

    /**
     * Updates the segment metadata for the newly added chunk.
     */
    private ChunkMetadata updateMetadataForChunkAddition(MetadataTransaction txn,
                                                         SegmentMetadata segmentMetadata,
                                                         String newChunkName,
                                                         boolean isFirstWriteAfterFailover,
                                                         ChunkMetadata lastChunkMetadata) throws StorageMetadataException {
        ChunkMetadata newChunkMetadata = ChunkMetadata.builder()
                .name(newChunkName)
                .build();
        segmentMetadata.setLastChunk(newChunkName);
        if (lastChunkMetadata == null) {
            segmentMetadata.setFirstChunk(newChunkName);
        } else {
            lastChunkMetadata.setNextChunk(newChunkName);
            txn.update(lastChunkMetadata);
        }
        segmentMetadata.setLastChunkStartOffset(segmentMetadata.getLength());

        // Reset ownershipChanged flag after first write is done.
        if (isFirstWriteAfterFailover) {
            segmentMetadata.setOwnerEpoch(this.epoch);
            segmentMetadata.setOwnershipChanged(false);
            log.debug("{} write - First write after failover - segment={}.", logPrefix, segmentMetadata.getName());
        }
        segmentMetadata.incrementChunkCount();

        // Update the transaction.
        txn.update(newChunkMetadata);
        txn.update(segmentMetadata);
        return newChunkMetadata;
    }

    /**
     * Write to chunk.
     */
    private int writeToChunk(MetadataTransaction txn,
                             SegmentMetadata segmentMetadata,
                             long offset,
                             InputStream data,
                             ChunkHandle chunkHandle,
                             ChunkMetadata chunkWrittenMetadata,
                             long offsetToWriteAt,
                             int bytesCount) throws IOException, StorageMetadataException, BadOffsetException {
        int bytesWritten;
        Preconditions.checkState(0 != bytesCount, "Attempt to write zero bytes");
        try {

            // Finally write the data.
            try (BoundedInputStream bis = new BoundedInputStream(data, bytesCount)) {
                bytesWritten = chunkStorage.write(chunkHandle, offsetToWriteAt, bytesCount, bis);
            }

            // Update the metadata for segment and chunk.
            Preconditions.checkState(bytesWritten >= 0);
            segmentMetadata.setLength(segmentMetadata.getLength() + bytesWritten);
            chunkWrittenMetadata.setLength(chunkWrittenMetadata.getLength() + bytesWritten);
            txn.update(chunkWrittenMetadata);
            txn.update(segmentMetadata);
        } catch (IllegalArgumentException e) {
            try {
                throw new BadOffsetException(segmentMetadata.getName(), chunkStorage.getInfo(chunkHandle.getChunkName()).getLength(), offset);
            } catch (ChunkStorageException cse) {
                log.error("{} write - Error while retrieving ChunkInfo for {}.", logPrefix, chunkHandle.getChunkName());
                // The exact expected offset for the  operation does not matter, the StorageWriter will enter reconciliation loop anyway.
                throw new BadOffsetException(segmentMetadata.getName(), offset, offset);
            }
        }
        return bytesWritten;
    }

    /**
     * Gets whether given segment is a critical storage system segment.
     *
     * @param segmentMetadata Meatadata for the segment.
     * @return True if this is a storage system segment.
     */
    private boolean isStorageSystemSegment(SegmentMetadata segmentMetadata) {
        return null != systemJournal && segmentMetadata.isStorageSystemSegment();
    }

    /**
     * Adds a system log.
     *
     * @param systemLogRecords
     * @param streamSegmentName Name of the segment.
     * @param offset            Offset at which new chunk was added.
     * @param oldChunkName      Name of the previous last chunk.
     * @param newChunkName      Name of the new last chunk.
     */
    private void addSystemLogRecord(ArrayList<SystemJournal.SystemJournalRecord> systemLogRecords, String streamSegmentName, long offset, String oldChunkName, String newChunkName) {
        systemLogRecords.add(
                SystemJournal.ChunkAddedRecord.builder()
                        .segmentName(streamSegmentName)
                        .offset(offset)
                        .oldChunkName(oldChunkName == null ? null : oldChunkName)
                        .newChunkName(newChunkName)
                        .build());
    }

    /**
     * Delete the garbage chunks.
     *
     * @param chunksTodelete List of chunks to delete.
     */
    private void collectGarbage(Collection<String> chunksTodelete) {
        for (val chunkTodelete : chunksTodelete) {
            try {
                chunkStorage.delete(chunkStorage.openWrite(chunkTodelete));
                log.debug("{} collectGarbage - deleted chunk={}.", logPrefix, chunkTodelete);
            } catch (ChunkNotFoundException e) {
                log.debug("{} collectGarbage - Could not delete garbage chunk {}.", logPrefix, chunkTodelete);
            } catch (Exception e) {
                log.warn("{} collectGarbage - Could not delete garbage chunk {}.", logPrefix, chunkTodelete);
                // Add it to garbage chunks.
                synchronized (garbageChunks) {
                    garbageChunks.add(chunkTodelete);
                }
            }
        }
    }

    @Override
    public CompletableFuture<Void> seal(SegmentHandle handle, Duration timeout) {
        checkInitialized();
        return execute(() -> {
            long traceId = LoggerHelpers.traceEnter(log, "seal", handle);
            Preconditions.checkNotNull(handle, "handle");
            String streamSegmentName = handle.getSegmentName();
            Preconditions.checkNotNull(streamSegmentName, "streamSegmentName");
            Preconditions.checkArgument(!handle.isReadOnly(), "handle");

            try (MetadataTransaction txn = metadataStore.beginTransaction()) {
                SegmentMetadata segmentMetadata = (SegmentMetadata) txn.get(streamSegmentName);
                // Validate preconditions.
                checkSegmentExists(streamSegmentName, segmentMetadata);
                checkOwnership(streamSegmentName, segmentMetadata);

                // seal if it is not already sealed.
                if (!segmentMetadata.isSealed()) {
                    segmentMetadata.setSealed(true);
                    txn.update(segmentMetadata);
                    txn.commit();
                }

                log.debug("{} seal - segment={}.", logPrefix, handle.getSegmentName());
                LoggerHelpers.traceLeave(log, "seal", traceId, handle);
                return null;
            } catch (StorageMetadataWritesFencedOutException ex) {
                throw new StorageNotPrimaryException(streamSegmentName, ex);
            }
        });
    }

    @Override
    public CompletableFuture<Void> concat(SegmentHandle targetHandle, long offset, String sourceSegment, Duration timeout) {
        checkInitialized();
        return execute(() -> {
            long traceId = LoggerHelpers.traceEnter(log, "concat", targetHandle, offset, sourceSegment);
            Timer timer = new Timer();

            Preconditions.checkArgument(null != targetHandle, "targetHandle");
            Preconditions.checkArgument(!targetHandle.isReadOnly(), "targetHandle");
            Preconditions.checkArgument(null != sourceSegment, "targetHandle");
            Preconditions.checkArgument(offset >= 0, "offset");
            String targetSegmentName = targetHandle.getSegmentName();

            try (MetadataTransaction txn = metadataStore.beginTransaction()) {

                SegmentMetadata targetSegmentMetadata = (SegmentMetadata) txn.get(targetSegmentName);

                // Validate preconditions.
                checkSegmentExists(targetSegmentName, targetSegmentMetadata);
                targetSegmentMetadata.checkInvariants();
                checkNotSealed(targetSegmentName, targetSegmentMetadata);

                SegmentMetadata sourceSegmentMetadata = (SegmentMetadata) txn.get(sourceSegment);
                checkSegmentExists(sourceSegment, sourceSegmentMetadata);
                sourceSegmentMetadata.checkInvariants();

                // This is a critical assumption at this point which should not be broken,
                Preconditions.checkState(!targetSegmentMetadata.isStorageSystemSegment(), "Storage system segments cannot be concatenated.");
                Preconditions.checkState(!sourceSegmentMetadata.isStorageSystemSegment(), "Storage system segments cannot be concatenated.");

                checkSealed(sourceSegmentMetadata);
                checkOwnership(targetSegmentMetadata.getName(), targetSegmentMetadata);

                if (sourceSegmentMetadata.getStartOffset() != 0) {
                    throw new StreamSegmentTruncatedException(sourceSegment, sourceSegmentMetadata.getLength(), 0);
                }

                if (offset != targetSegmentMetadata.getLength()) {
                    throw new BadOffsetException(targetHandle.getSegmentName(), targetSegmentMetadata.getLength(), offset);
                }

                // Update list of chunks by appending sources list of chunks.
                ChunkMetadata targetLastChunk = (ChunkMetadata) txn.get(targetSegmentMetadata.getLastChunk());
                ChunkMetadata sourceFirstChunk = (ChunkMetadata) txn.get(sourceSegmentMetadata.getFirstChunk());

                if (targetLastChunk != null) {
                    targetLastChunk.setNextChunk(sourceFirstChunk.getName());
                    txn.update(targetLastChunk);
                } else {
                    if (sourceFirstChunk != null) {
                        targetSegmentMetadata.setFirstChunk(sourceFirstChunk.getName());
                        txn.update(sourceFirstChunk);
                    }
                }

                // Update segments's last chunk to point to the sources last segment.
                targetSegmentMetadata.setLastChunk(sourceSegmentMetadata.getLastChunk());

                // Update the length of segment.
                targetSegmentMetadata.setLastChunkStartOffset(targetSegmentMetadata.getLength() + sourceSegmentMetadata.getLastChunkStartOffset());
                targetSegmentMetadata.setLength(targetSegmentMetadata.getLength() + sourceSegmentMetadata.getLength() - sourceSegmentMetadata.getStartOffset());

                targetSegmentMetadata.setChunkCount(targetSegmentMetadata.getChunkCount() + sourceSegmentMetadata.getChunkCount());

                txn.update(targetSegmentMetadata);
                txn.delete(sourceSegment);

                // Finally defrag immediately.
                ArrayList<String> chunksToDelete = new ArrayList<>();
                if (shouldDefrag() && null != targetLastChunk) {
                    defrag(txn, targetSegmentMetadata, targetLastChunk.getName(), null, chunksToDelete);
                }

                targetSegmentMetadata.checkInvariants();

                // Finally commit transaction.
                txn.commit();

                // Collect garbage.
                collectGarbage(chunksToDelete);

                // Update the read index.
                readIndexCache.remove(sourceSegment);

                Duration elapsed = timer.getElapsed();
                log.debug("{} concat - target={}, source={}, offset={}, latency={}.", logPrefix, targetHandle.getSegmentName(), sourceSegment, offset, elapsed.toMillis());
                LoggerHelpers.traceLeave(log, "concat", traceId, targetHandle, offset, sourceSegment);

            } catch (StorageMetadataWritesFencedOutException ex) {
                throw new StorageNotPrimaryException(targetSegmentName, ex);
            }

            return null;
        });
    }

    private boolean shouldAppend() {
        return chunkStorage.supportsAppend() && config.isAppendEnabled();
    }

    private boolean shouldDefrag() {
        return shouldAppend() || chunkStorage.supportsConcat();
    }

    /**
     * Defragments the list of chunks for a given segment.
     * It finds eligible consecutive chunks that can be merged together.
     * The sublist such elgible chunks is replaced with single new chunk record corresponding to new large chunk.
     * Conceptually this is like deleting nodes from middle of the list of chunks.
     *
     * <Ul>
     * <li> In the absence of defragmentation, the number of chunks for individual segments keeps on increasing.
     * When we have too many small chunks (say because many transactions with little data on some segments), the segment
     * is fragmented - this may impact both the read throughput and the performance of the metadata store.
     * This problem is further intensified when we have stores that do not support append semantics (e.g., stock S3) and
     * each write becomes a separate chunk.
     * </li>
     * <li>
     * If the underlying storage provides some facility to stitch together smaller chunk into larger chunks, then we do
     * actually want to exploit that, specially when the underlying implementation is only a metadata operation. We want
     * to leverage multi-part uploads in object stores that support it (e.g., AWS S3, Dell EMC ECS) as they are typically
     * only metadata operations, reducing the overall cost of the merging them together. HDFS also supports merges,
     * whereas NFS has no concept of merging natively.
     *
     * As chunks become larger, append writes (read source completely and append it back at the end of target)
     * become inefficient. Consequently, a native option for merging is desirable. We use such native merge capability
     * when available, and if not available, then we use appends.
     * </li>
     * <li>
     * Ideally we want the defrag to be run in the background periodically and not on the write/concat path.
     * We can then fine tune that background task to run optimally with low overhead.
     * We might be able to give more knobs to tune its parameters (Eg. threshold on number of chunks).
     * </li>
     * <li>
     * <li>
     * Defrag operation will respect max rolling size and will not create chunks greater than that size.
     * </li>
     * </ul>
     *
     * What controls whether we invoke concat or simulate through appends?
     * There are a few different capabilities that ChunkStorage needs to provide.
     * <ul>
     * <li>Does ChunkStorage support appending to existing chunks? For vanilla S3 compatible this would return false.
     * This is indicated by supportsAppend.</li>
     * <li>Does ChunkStorage support for concatenating chunks ? This is indicated by supportsConcat.
     * If this is true then concat operation will be invoked otherwise chunks will be appended.</li>
     * <li>There are some obvious constraints - For ChunkStorage support any concat functionality it must support either
     * append or concat.</li>
     * <li>Also when ChunkStorage supports both concat and append, ChunkedSegmentStorage will invoke appropriate method
     * depending on size of target and source chunks. (Eg. ECS)</li>
     * </ul>
     *
     * <li>
     * What controls defrag?
     * There are two additional parameters that control when concat
     * <li>minSizeLimitForConcat: Size of chunk in bytes above which it is no longer considered a small object.
     * For small source objects, append is used instead of using concat. (For really small txn it is rather efficient to use append than MPU).</li>
     * <li>maxSizeLimitForConcat: Size of chunk in bytes above which it is no longer considered for concat. (Eg S3 might have max limit on chunk size).</li>
     * In short there is a size beyond which using append is not advisable. Conversely there is a size below which concat is not efficient.(minSizeLimitForConcat )
     * Then there is limit which concating does not make sense maxSizeLimitForConcat
     * </li>
     * <li>
     * What is the defrag algorithm
     * <pre>
     * While(segment.hasConcatableChunks()){
     *     Set<List<Chunk>> s = FindConsecutiveConcatableChunks();
     *     For (List<chunk> list : s){
     *        ConcatChunks (list);
     *     }
     * }
     * </pre>
     * </li>
     * </ul>
     *
     * @param txn             Active {@link MetadataTransaction}.
     * @param segmentMetadata {@link SegmentMetadata} for the segment to defrag.
     * @param startChunkName  Name of the first chunk to start defragmentation.
     * @param lastChunkName   Name of the last chunk before which to stop defragmentation. (last chunk is not concatenated).
     * @param chunksToDelete  List of chunks to which names of chunks to be deleted are added. It is the responsibility
     *                        of caller to garbage collect these chunks.
     * @throws ChunkStorageException In case of any chunk storage related errors.
     * @throws StorageMetadataException In case of any chunk metadata store related errors.
     */
    private void defrag(MetadataTransaction txn, SegmentMetadata segmentMetadata,
                        String startChunkName,
                        String lastChunkName,
                        ArrayList<String> chunksToDelete)
            throws StorageMetadataException, ChunkStorageException {
        // The algorithm is actually very simple.
        // It tries to concat all small chunks using appends first.
        // Then it tries to concat remaining chunks using concat if available.
        // To implement it using single loop we toggle between concat with append and concat modes. (Instead of two passes.)
        boolean useAppend = true;
        String targetChunkName = startChunkName;

        // Iterate through chunk list
        while (null != targetChunkName && !targetChunkName.equals(lastChunkName)) {
            ChunkMetadata target = (ChunkMetadata) txn.get(targetChunkName);

            ArrayList<ChunkInfo> chunksToConcat = new ArrayList<>();
            long targetSizeAfterConcat = target.getLength();

            // Add target to the list of chunks
            chunksToConcat.add(new ChunkInfo(targetSizeAfterConcat, targetChunkName));

            String nextChunkName = target.getNextChunk();
            ChunkMetadata next = null;

            // Gather list of chunks that can be appended together.
            while (null != nextChunkName) {
                next = (ChunkMetadata) txn.get(nextChunkName);

                if (useAppend && config.getMinSizeLimitForConcat() < next.getLength()) {
                    break;
                }

                if (targetSizeAfterConcat + next.getLength() > segmentMetadata.getMaxRollinglength() || next.getLength() > config.getMaxSizeLimitForConcat()) {
                    break;
                }

                chunksToConcat.add(new ChunkInfo(next.getLength(), nextChunkName));
                targetSizeAfterConcat += next.getLength();

                nextChunkName = next.getNextChunk();
            }
            // Note - After above while loop is exited nextChunkName points to chunk next to last one to be concat.
            // Which means target should now point to it as next after concat is complete.

            // If there are chunks that can be appended together then concat them.
            if (chunksToConcat.size() > 1) {
                // Concat

                ConcatArgument[] concatArgs = new ConcatArgument[chunksToConcat.size()];
                for (int i = 0; i < chunksToConcat.size(); i++) {
                    concatArgs[i] = ConcatArgument.fromChunkInfo(chunksToConcat.get(i));
                }

                if (!useAppend && chunkStorage.supportsConcat()) {
                    int length = chunkStorage.concat(concatArgs);
                } else {
                    concatUsingAppend(concatArgs);
                }

                // Delete chunks.
                for (int i = 1; i < chunksToConcat.size(); i++) {
                    chunksToDelete.add(chunksToConcat.get(i).getName());
                }

                // Set the pointers
                target.setLength(targetSizeAfterConcat);
                target.setNextChunk(nextChunkName);

                // If target is the last chunk after this then update metadata accordingly
                if (null == nextChunkName) {
                    segmentMetadata.setLastChunk(target.getName());
                    segmentMetadata.setLastChunkStartOffset(segmentMetadata.getLength() - target.getLength());
                }

                // Update metadata for affected chunks.
                for (int i = 1; i < concatArgs.length; i++) {
                    txn.delete(concatArgs[i].getName());
                    segmentMetadata.decrementChunkCount();
                }
                txn.update(target);
                txn.update(segmentMetadata);
            }

            // Move on to next place in list where we can concat if we are done with append based concats.
            if (!useAppend) {
                targetChunkName = nextChunkName;
            }

            // Toggle
            useAppend = !useAppend;
        }

        // Make sure no invariants are broken.
        segmentMetadata.checkInvariants();
    }

    private void concatUsingAppend(ConcatArgument[] concatArgs) throws ChunkStorageException {
        long writeAtOffset = concatArgs[0].getLength();
        val writeHandle = ChunkHandle.writeHandle(concatArgs[0].getName());
        for (int i = 1; i < concatArgs.length; i++) {
            int readAtOffset = 0;
            val arg = concatArgs[i];
            int bytesToRead = Math.toIntExact(arg.getLength());

            while (bytesToRead > 0) {
                byte[] buffer = new byte[Math.min(config.getMaxBufferSizeForChunkDataTransfer(), bytesToRead)];
                int size = chunkStorage.read(ChunkHandle.readHandle(arg.getName()), readAtOffset, buffer.length, buffer, 0);
                bytesToRead -= size;
                readAtOffset += size;
                writeAtOffset += chunkStorage.write(writeHandle, writeAtOffset, size, new ByteArrayInputStream(buffer, 0, size));
            }
        }
    }

    @Override
    public CompletableFuture<Void> delete(SegmentHandle handle, Duration timeout) {
        checkInitialized();
        return execute(() -> {
            long traceId = LoggerHelpers.traceEnter(log, "delete", handle);
            Timer timer = new Timer();

            String streamSegmentName = handle.getSegmentName();
            try (MetadataTransaction txn = metadataStore.beginTransaction()) {
                SegmentMetadata segmentMetadata = (SegmentMetadata) txn.get(streamSegmentName);

                // Check preconditions
                checkSegmentExists(streamSegmentName, segmentMetadata);
                checkOwnership(streamSegmentName, segmentMetadata);

                segmentMetadata.setActive(false);

                // Delete chunks
                String currentChunkName = segmentMetadata.getFirstChunk();
                ChunkMetadata currentMetadata;
                ArrayList<String> chunksToDelete = new ArrayList<>();
                while (currentChunkName != null) {
                    currentMetadata = (ChunkMetadata) txn.get(currentChunkName);
                    // Delete underlying file.
                    chunksToDelete.add(currentChunkName);
                    currentChunkName = currentMetadata.getNextChunk();
                    txn.delete(currentMetadata.getName());
                }

                // Commit.
                txn.delete(streamSegmentName);
                txn.commit();

                // Collect garbage.
                collectGarbage(chunksToDelete);

                // Update the read index.
                readIndexCache.remove(streamSegmentName);

                Duration elapsed = timer.getElapsed();
                log.debug("{} delete - segment={}, latency={}.", logPrefix, handle.getSegmentName(), elapsed.toMillis());
                LoggerHelpers.traceLeave(log, "delete", traceId, handle);
                return null;
            } catch (StorageMetadataWritesFencedOutException ex) {
                throw new StorageNotPrimaryException(streamSegmentName, ex);
            }
        });
    }

    @Override
    public CompletableFuture<Void> truncate(SegmentHandle handle, long offset, Duration timeout) {
        checkInitialized();
        return execute(() -> {
            long traceId = LoggerHelpers.traceEnter(log, "truncate", handle, offset);
            Timer timer = new Timer();

            Preconditions.checkArgument(null != handle, "handle");
            Preconditions.checkArgument(!handle.isReadOnly(), "handle");
            Preconditions.checkArgument(offset >= 0, "offset");

            String streamSegmentName = handle.getSegmentName();
            try (MetadataTransaction txn = metadataStore.beginTransaction()) {
                SegmentMetadata segmentMetadata = (SegmentMetadata) txn.get(streamSegmentName);

                // Check preconditions
                checkSegmentExists(streamSegmentName, segmentMetadata);
                checkNotSealed(streamSegmentName, segmentMetadata);
                checkOwnership(streamSegmentName, segmentMetadata);

                if (segmentMetadata.getLength() < offset || segmentMetadata.getStartOffset() > offset) {
                    throw new IllegalArgumentException(String.format("offset %d is outside of valid range [%d, %d) for segment %s",
                            offset, segmentMetadata.getStartOffset(), segmentMetadata.getLength(), streamSegmentName));
                }

                if (segmentMetadata.getStartOffset() == offset) {
                    // Nothing to do
                    return null;
                }

                String currentChunkName = segmentMetadata.getFirstChunk();
                ChunkMetadata currentMetadata;
                long oldLength = segmentMetadata.getLength();
                long startOffset = segmentMetadata.getFirstChunkStartOffset();
                ArrayList<String> chunksToDelete = new ArrayList<>();
                while (currentChunkName != null) {
                    currentMetadata = (ChunkMetadata) txn.get(currentChunkName);
                    Preconditions.checkState(null != currentMetadata, "currentMetadata is null.");

                    // If for given chunk start <= offset < end  then we have found the chunk that will be the first chunk.
                    if ((startOffset <= offset) && (startOffset + currentMetadata.getLength() > offset)) {
                        break;
                    }

                    startOffset += currentMetadata.getLength();
                    chunksToDelete.add(currentMetadata.getName());
                    segmentMetadata.decrementChunkCount();

                    // move to next chunk
                    currentChunkName = currentMetadata.getNextChunk();
                }
                segmentMetadata.setFirstChunk(currentChunkName);
                segmentMetadata.setStartOffset(offset);
                segmentMetadata.setFirstChunkStartOffset(startOffset);
                for (String toDelete : chunksToDelete) {
                    txn.delete(toDelete);
                    // Adjust last chunk if required.
                    if (toDelete.equals(segmentMetadata.getLastChunk())) {
                        segmentMetadata.setLastChunkStartOffset(segmentMetadata.getLength());
                        segmentMetadata.setLastChunk(null);
                    }
                }
                txn.update(segmentMetadata);

                // Check invariants.
                Preconditions.checkState(segmentMetadata.getLength() == oldLength, "truncate should not change segment length");
                segmentMetadata.checkInvariants();

                // Commit system logs.
                if (isStorageSystemSegment(segmentMetadata)) {
                    val finalStartOffset = startOffset;
                    txn.setExternalCommitStep(() -> {
                        systemJournal.commitRecord(
                                SystemJournal.TruncationRecord.builder()
                                        .segmentName(streamSegmentName)
                                        .offset(offset)
                                        .firstChunkName(segmentMetadata.getFirstChunk())
                                        .startOffset(finalStartOffset)
                                        .build());
                        return null;
                    });
                }

                // Finally commit.
                txn.commit();

                collectGarbage(chunksToDelete);

                // Update the read index by removing all entries below truncate offset.
                readIndexCache.truncateReadIndex(streamSegmentName, segmentMetadata.getStartOffset());

                Duration elapsed = timer.getElapsed();
                log.debug("{} truncate - segment={}, offset={}, latency={}.", logPrefix, handle.getSegmentName(), offset, elapsed.toMillis());
                LoggerHelpers.traceLeave(log, "truncate", traceId, handle, offset);
                return null;
            } catch (StorageMetadataWritesFencedOutException ex) {
                throw new StorageNotPrimaryException(streamSegmentName, ex);
            }
        });
    }

    @Override
    public boolean supportsTruncation() {
        return true;
    }

    @Override
    public Iterator<SegmentProperties> listSegments() throws IOException {
        throw new UnsupportedOperationException("listSegments is not yet supported");
    }

    @Override
    public CompletableFuture<SegmentHandle> openRead(String streamSegmentName) {
        checkInitialized();
        return execute(() -> {
            long traceId = LoggerHelpers.traceEnter(log, "openRead", streamSegmentName);
            // Validate preconditions and return handle.
            Preconditions.checkNotNull(streamSegmentName, "streamSegmentName");
            try (MetadataTransaction txn = metadataStore.beginTransaction()) {
                SegmentMetadata segmentMetadata = (SegmentMetadata) txn.get(streamSegmentName);
                checkSegmentExists(streamSegmentName, segmentMetadata);
                segmentMetadata.checkInvariants();
                // This segment was created by an older segment store. Then claim ownership and adjust length.
                if (segmentMetadata.getOwnerEpoch() < this.epoch) {
                    log.debug("{} openRead - Segment needs ownership change. segment={}.", logPrefix, segmentMetadata.getName());
                    // In case of a failover, length recorded in metadata will be lagging behind its actual length in the storage.
                    // This can happen with lazy commits that were still not committed at the time of failover.
                    claimOwnership(txn, segmentMetadata);
                }
                val retValue = SegmentStorageHandle.readHandle(streamSegmentName);
                LoggerHelpers.traceLeave(log, "openRead", traceId, retValue);
                return retValue;

            }
        });
    }

    @Override
    public CompletableFuture<Integer> read(SegmentHandle handle, long offset, byte[] buffer, int bufferOffset, int length, Duration timeout) {
        checkInitialized();
        return execute(() -> {
            long traceId = LoggerHelpers.traceEnter(log, "read", handle, offset, length);
            Timer timer = new Timer();

            // Validate preconditions.
            Preconditions.checkNotNull(handle, "handle");
            Preconditions.checkNotNull(buffer, "buffer");
            String streamSegmentName = handle.getSegmentName();
            Preconditions.checkNotNull(streamSegmentName, "streamSegmentName");

            Exceptions.checkArrayRange(bufferOffset, length, buffer.length, "bufferOffset", "length");

            if (offset < 0 || bufferOffset < 0 || length < 0 || buffer.length < bufferOffset + length) {
                throw new ArrayIndexOutOfBoundsException(String.format(
                        "Offset (%s) must be non-negative, and bufferOffset (%s) and length (%s) must be valid indices into buffer of size %s.",
                        offset, bufferOffset, length, buffer.length));
            }

            try (MetadataTransaction txn = metadataStore.beginTransaction()) {
                SegmentMetadata segmentMetadata = (SegmentMetadata) txn.get(streamSegmentName);

                // Validate preconditions.
                checkSegmentExists(streamSegmentName, segmentMetadata);

                segmentMetadata.checkInvariants();

                Preconditions.checkArgument(offset < segmentMetadata.getLength(), "Offset %s is beyond the last offset %s of the segment %s.",
                        offset, segmentMetadata.getLength(), streamSegmentName);

                if (offset < segmentMetadata.getStartOffset()) {
                    throw new StreamSegmentTruncatedException(streamSegmentName, segmentMetadata.getStartOffset(), offset);
                }

                if (length == 0) {
                    return 0;
                }

                String currentChunkName = segmentMetadata.getFirstChunk();
                ChunkMetadata chunkToReadFrom = null;

                Preconditions.checkState(null != currentChunkName);

                int bytesRemaining = length;
                int currentBufferOffset = bufferOffset;
                long currentOffset = offset;
                int totalBytesRead = 0;

                // Find the first chunk that contains the data.
                long startOffsetForCurrentChunk = segmentMetadata.getFirstChunkStartOffset();
                Timer timer1 = new Timer();
                int cntScanned = 0;
                // Find the name of the chunk in the cached read index that is floor to required offset.
                val floorEntry = readIndexCache.findFloor(streamSegmentName, offset);
                if (null != floorEntry) {
                    startOffsetForCurrentChunk = floorEntry.getOffset();
                    currentChunkName = floorEntry.getChunkName();
                }

                // Navigate to the chunk that contains the first byte of requested data.
                while (currentChunkName != null) {
                    chunkToReadFrom = (ChunkMetadata) txn.get(currentChunkName);
                    Preconditions.checkState(null != chunkToReadFrom, "chunkToReadFrom is null");
                    if (startOffsetForCurrentChunk <= currentOffset
                            && startOffsetForCurrentChunk + chunkToReadFrom.getLength() > currentOffset) {
                        // we have found a chunk that contains first byte we want to read
                        log.debug("{} read - found chunk to read - segment={}, chunk={}, startOffset={}, length={}, readOffset={}.",
                                logPrefix, streamSegmentName, chunkToReadFrom, startOffsetForCurrentChunk, chunkToReadFrom.getLength(), currentOffset);
                        break;
                    }
                    currentChunkName = chunkToReadFrom.getNextChunk();
                    startOffsetForCurrentChunk += chunkToReadFrom.getLength();

                    // Update read index with newly visited chunk.
                    if (null != currentChunkName) {
                        readIndexCache.addIndexEntry(streamSegmentName, currentChunkName, startOffsetForCurrentChunk);
                    }
                    cntScanned++;
                }
                log.debug("{} read - chunk lookup - segment={}, offset={}, scanned={}, latency={}.",
                        logPrefix, handle.getSegmentName(), offset, cntScanned, timer1.getElapsed().toMillis());

                // Now read.
                while (bytesRemaining > 0 && null != currentChunkName) {
                    int bytesToRead = Math.min(bytesRemaining, Math.toIntExact(chunkToReadFrom.getLength() - (currentOffset - startOffsetForCurrentChunk)));
                    //assert bytesToRead != 0;

                    if (currentOffset >= startOffsetForCurrentChunk + chunkToReadFrom.getLength()) {
                        // The current chunk is over. Move to the next one.
                        currentChunkName = chunkToReadFrom.getNextChunk();
                        if (null != currentChunkName) {
                            startOffsetForCurrentChunk += chunkToReadFrom.getLength();
                            chunkToReadFrom = (ChunkMetadata) txn.get(currentChunkName);
                            log.debug("{} read - reading from next chunk - segment={}, chunk={}", logPrefix, streamSegmentName, chunkToReadFrom);
                        }
                    } else {
                        Preconditions.checkState(bytesToRead != 0, "bytesToRead is 0");
                        // Read data from the chunk.
                        ChunkHandle chunkHandle = chunkStorage.openRead(chunkToReadFrom.getName());
                        int bytesRead = chunkStorage.read(chunkHandle, currentOffset - startOffsetForCurrentChunk, bytesToRead, buffer, currentBufferOffset);

                        bytesRemaining -= bytesRead;
                        currentOffset += bytesRead;
                        currentBufferOffset += bytesRead;
                        totalBytesRead += bytesRead;
                    }
                }

                Duration elapsed = timer.getElapsed();
                log.debug("{} read - segment={}, offset={}, bytesRead={}, latency={}.", logPrefix, handle.getSegmentName(), offset, totalBytesRead, elapsed.toMillis());
                LoggerHelpers.traceLeave(log, "read", traceId, handle, offset, totalBytesRead);
                return totalBytesRead;
            }
        });
    }

    @Override
    public CompletableFuture<SegmentProperties> getStreamSegmentInfo(String streamSegmentName, Duration timeout) {
        checkInitialized();
        return execute(() -> {
            long traceId = LoggerHelpers.traceEnter(log, "getStreamSegmentInfo", streamSegmentName);
            Preconditions.checkNotNull(streamSegmentName, "streamSegmentName");
            try (MetadataTransaction txn = metadataStore.beginTransaction()) {
                SegmentMetadata segmentMetadata = (SegmentMetadata) txn.get(streamSegmentName);
                if (null == segmentMetadata) {
                    throw new StreamSegmentNotExistsException(streamSegmentName);
                }
                segmentMetadata.checkInvariants();

                val retValue = StreamSegmentInformation.builder()
                        .name(streamSegmentName)
                        .sealed(segmentMetadata.isSealed())
                        .length(segmentMetadata.getLength())
                        .startOffset(segmentMetadata.getStartOffset())
                        .lastModified(new ImmutableDate(segmentMetadata.getLastModified()))
                        .build();
                LoggerHelpers.traceLeave(log, "getStreamSegmentInfo", traceId, retValue);
                return retValue;
            }
        });
    }

    @Override
    public CompletableFuture<Boolean> exists(String streamSegmentName, Duration timeout) {
        checkInitialized();
        return execute(() -> {
            long traceId = LoggerHelpers.traceEnter(log, "exists", streamSegmentName);
            Preconditions.checkNotNull(streamSegmentName, "streamSegmentName");
            try (MetadataTransaction txn = metadataStore.beginTransaction()) {
                SegmentMetadata segmentMetadata = (SegmentMetadata) txn.get(streamSegmentName);
                val retValue = segmentMetadata == null ? false : segmentMetadata.isActive();
                LoggerHelpers.traceLeave(log, "exists", traceId, retValue);
                return retValue;
            }
        });
    }

    @Override
    public void close() {
        try {
            if (null != this.metadataStore) {
                this.metadataStore.close();
            }
        } catch (Exception e) {
            log.warn("Error during close", e);
        }
        this.closed.set(true);
    }

    /**
     * Executes the given Callable and returns its result, while translating any Exceptions bubbling out of it into
     * StreamSegmentExceptions.
     *
     * @param operation The function to execute.
     * @param <R>       Return type of the operation.
     * @return CompletableFuture<R> of the return type of the operation.
     */
    private <R> CompletableFuture<R> execute(Callable<R> operation) {
        return CompletableFuture.supplyAsync(() -> {
            Exceptions.checkNotClosed(this.closed.get(), this);
            try {
                return operation.call();
            } catch (Exception e) {
                throw new CompletionException(e);
            }
        }, this.executor);
    }

    private String getNewChunkName(String segmentName, long offset) throws Exception {
        return NameUtils.getSegmentChunkName(segmentName, epoch, offset);
    }

    private void checkSegmentExists(String streamSegmentName, SegmentMetadata segmentMetadata) throws StreamSegmentNotExistsException {
        if (null == segmentMetadata || !segmentMetadata.isActive()) {
            throw new StreamSegmentNotExistsException(streamSegmentName);
        }
    }

    private void checkOwnership(String streamSegmentName, SegmentMetadata segmentMetadata) throws StorageNotPrimaryException {
        if (segmentMetadata.getOwnerEpoch() > this.epoch) {
            throw new StorageNotPrimaryException(streamSegmentName);
        }
    }

    private void checkNotSealed(String streamSegmentName, SegmentMetadata segmentMetadata) throws StreamSegmentSealedException {
        if (segmentMetadata.isSealed()) {
            throw new StreamSegmentSealedException(streamSegmentName);
        }
    }

    private void checkSealed(SegmentMetadata sourceSegmentMetadata) {
        if (!sourceSegmentMetadata.isSealed()) {
            throw new IllegalStateException("Source segment must be sealed.");
        }
    }

    private void checkInitialized() {
        Preconditions.checkState(null != this.metadataStore);
        Preconditions.checkState(0 != this.epoch);
        Preconditions.checkState(!closed.get());
    }
}
