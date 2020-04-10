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
 * Implements storage for segments using {@link ChunkStorageProvider} and {@link ChunkMetadataStore}.
 * The metadata about the segments is stored in metadataStore using two types of records {@link SegmentMetadata} and {@link ChunkMetadata}.
 * Any changes to layout must be made inside a {@link MetadataTransaction} which will atomically change the records upon {@link MetadataTransaction#commit()}
 * Detailed design is documented here https://github.com/pravega/pravega/wiki/PDP-34:-Simplified-Tier-2
 */
@Slf4j
public class ChunkStorageManager implements Storage {
    /**
     * Configuration options for this ChunkStorageManager instance.
     */
    @Getter
    private final ChunkStorageManagerConfig config;

    /**
     * Metadata store containing all storage data.
     * Initialized by segment container via {@link ChunkStorageManager#initialize(int, ChunkMetadataStore, SystemJournal)}.
     */
    @Getter
    private ChunkMetadataStore metadataStore;

    /**
     * Underlying {@link ChunkStorageProvider} to use to read and write data.
     */
    @Getter
    private final ChunkStorageProvider chunkStorage;

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
     * Initialized by segment container via {@link ChunkStorageManager#initialize(long)}.
     */
    @Getter
    private long epoch;

    /**
     * Id of the current Container.
     * Initialized by segment container via {@link ChunkStorageManager#initialize(int, ChunkMetadataStore, SystemJournal)}.
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
     * Creates a new instance of the ChunkStorageManager class.
     *
     * @param chunkStorage ChunkStorageProvider instance.
     * @param executor     An Executor for async operations.
     * @param config       Configuration options for this ChunkStorageManager instance.
     */
    public ChunkStorageManager(ChunkStorageProvider chunkStorage, Executor executor, ChunkStorageManagerConfig config) {
        this.config = Preconditions.checkNotNull(config, "config");
        this.chunkStorage = Preconditions.checkNotNull(chunkStorage, "chunkStorage");
        this.executor = Preconditions.checkNotNull(executor, "executor");
        this.readIndexCache = new ReadIndexCache(config.getMaxIndexedSegments(),
                config.getMaxIndexedChunksPerSegment(),
                config.getMaxIndexedChunks());
        this.closed = new AtomicBoolean(false);
    }

    /**
     * Creates a new instance of the ChunkStorageManager class.
     *
     * @param chunkStorage  ChunkStorageProvider instance.
     * @param metadataStore Metadata store.
     * @param executor      An Executor for async operations.
     * @param config        Configuration options for this ChunkStorageManager instance.
     */
    public ChunkStorageManager(ChunkStorageProvider chunkStorage, ChunkMetadataStore metadataStore, Executor executor, ChunkStorageManagerConfig config) {
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
     * Initializes the ChunkStorageManager.
     *
     * @param metadataStore Metadata store.
     * @param containerId   container id.
     * @param systemJournal SystemJournal that keeps track of changes to system segments and helps with bootstrap.
     * @throws Exception In case of any errors.
     */
    public void initialize(int containerId, ChunkMetadataStore metadataStore, SystemJournal systemJournal) throws Exception {
        this.containerId = containerId;
        this.logPrefix = String.format("ChunkStorageManager[%d]", containerId);
        this.metadataStore = Preconditions.checkNotNull(metadataStore, "metadataStore");
        this.systemJournal = Preconditions.checkNotNull(systemJournal, "systemJournal");
    }

    /**
     * Initializes this instance with the given ContainerEpoch.
     *
     * @param containerEpoch The Container Epoch to initialize with.
     */
    @Override
    public void initialize(long containerEpoch) {
        this.epoch = containerEpoch;
    }

    /**
     * Attempts to open the given Segment in read-write mode and make it available for use for this instance of the Storage
     * adapter.
     * A single active read-write SegmentHandle can exist at any given time for a particular Segment, regardless of owner,
     * while a read-write SegmentHandle can coexist with any number of read-only SegmentHandles for that Segment (obtained
     * by calling openRead()).
     * This can be accomplished in a number of different ways based on the actual implementation of the Storage
     * interface, but it can be compared to acquiring an exclusive lock on the given segment).
     *
     * @param streamSegmentName Name of the StreamSegment to be opened.
     * @return A CompletableFuture that, when completed, will contain a read-write SegmentHandle that can be used to access
     * the segment for read and write activities (ex: read, get, write, seal, concat).
     * If the segment is sealed, then a Read-Only handle is returned.
     * <p>
     * If the operation failed, it will be failed with the cause of the failure. Notable exceptions:
     * <ul>
     * <li> StreamSegmentNotExistsException: When the given Segment does not exist in Storage.
     * </ul>
     */
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
                if (segmentMetadata.getOwnerEpoch() > this.epoch) {
                    throw new StorageNotPrimaryException(streamSegmentName);
                }

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
     * @throws Exception In case of any errors.
     */
    private void claimOwnership(MetadataTransaction txn, SegmentMetadata segmentMetadata) throws Exception {
        // Claim ownership.
        // This is safe because the previous instance is definitely not an owner anymore. (even if this instance is no more owner)
        segmentMetadata.setOwnerEpoch(this.epoch);
        segmentMetadata.setOwnershipChanged(true);

        // Get the last chunk
        String lastChunkName = segmentMetadata.getLastChunk();
        if (null != lastChunkName) {
            ChunkMetadata lastChunk = (ChunkMetadata) txn.get(lastChunkName);
            ChunkInfo chunkInfo = chunkStorage.getInfo(lastChunkName);
            Preconditions.checkState(chunkInfo != null);
            Preconditions.checkState(lastChunk != null);
            // Adjust its length;
            if (chunkInfo.getLength() != lastChunk.getLength()) {
                Preconditions.checkState(chunkInfo.getLength() > lastChunk.getLength());
                // Whatever length you see right now is the final "sealed" length of the last chunk.
                lastChunk.setLength((int) chunkInfo.getLength());
                segmentMetadata.setLength(segmentMetadata.getLastChunkStartOffset() + lastChunk.getLength());
                txn.update(lastChunk);
                log.debug("{} claimOwnership - Length of last chunk adjusted - segment={}, last chunk={}, Length={}.",
                        logPrefix,
                        segmentMetadata.getName(),
                        lastChunk.getName(),
                        chunkInfo.getLength());
            }
        }
        // Update and commit
        // If This instance is fenced this update will fail.
        txn.update(segmentMetadata);
        txn.commit();
    }

    /**
     * Creates a new StreamSegment in this Storage Layer with the given Rolling Policy.
     *
     * @param streamSegmentName The full name of the StreamSegment.
     * @param rollingPolicy     The Rolling Policy to apply to this StreamSegment.
     * @param timeout           Timeout for the operation.
     * @return A CompletableFuture that, when completed, will contain a read-write SegmentHandle that can be used to access
     * * the segment for read and write activities (ex: read, get, write, seal, concat). If the operation failed, it will contain the cause of the
     * failure. Notable exceptions:
     * <ul>
     * <li> StreamSegmentExistsException: When the given Segment already exists in Storage.
     * </ul>
     */
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

    /**
     * Writes the given data to the StreamSegment.
     *
     * @param handle  A read-write SegmentHandle that points to a Segment to write to.
     * @param offset  The offset in the StreamSegment to write data at.
     * @param data    An InputStream representing the data to write.
     * @param length  The length of the InputStream.
     * @param timeout Timeout for the operation.
     * @return A CompletableFuture that, when completed, will indicate the operation succeeded. If the operation failed,
     * it will contain the cause of the failure. Notable exceptions:
     * <ul>
     * <li> BadOffsetException: When the given offset does not match the actual length of the segment in storage.
     * <li> StreamSegmentNotExistsException: When the given Segment does not exist in Storage.
     * <li> StorageNotPrimaryException: When this Storage instance is no longer primary for this Segment (it was fenced out).
     * </ul>
     * @throws IllegalArgumentException If handle is read-only.
     */
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

                if (segmentMetadata.isSealed()) {
                    throw new StreamSegmentSealedException(streamSegmentName);
                }

                if (segmentMetadata.getOwnerEpoch() > this.epoch) {
                    throw new StorageNotPrimaryException(streamSegmentName);
                }

                boolean isSystemSegment = isStorageSystemSegment(segmentMetadata);

                // Check if this is a first write after ownership changed.
                boolean isFirstWriteAfterFailover = segmentMetadata.isOwnershipChanged();

                // Write data  to the last segment.
                int bytesRemaining = length;
                long currentOffset = offset;

                ChunkMetadata lastChunkMetadata = null;
                ChunkHandle chunkHandle = null;
                ChunkMetadata chunkWrittenMetadata = null;

                while (bytesRemaining > 0) {
                    // Validate that offset is correct.
                    if ((segmentMetadata.getLength()) != currentOffset) {
                        throw new BadOffsetException(streamSegmentName, segmentMetadata.getLength(), currentOffset);
                    }

                    // Get the last chunk segmentMetadata for the segment.
                    if (null != segmentMetadata.getLastChunk()) {
                        lastChunkMetadata = (ChunkMetadata) txn.get(segmentMetadata.getLastChunk());
                    }

                    // Check if new chunk needs to be added.
                    // This could be either because there are no existing chunks or last chunk has reached max rolling length.
                    if (null == lastChunkMetadata
                            || (lastChunkMetadata.getLength() >= segmentMetadata.getMaxRollinglength())
                            || isFirstWriteAfterFailover
                            || !shouldAppend()) {

                        // Create new chunk
                        String newChunkName = getNewChunkName(streamSegmentName,
                                segmentMetadata.getLength());

                        chunkWrittenMetadata = ChunkMetadata.builder()
                                .name(newChunkName)
                                .build();
                        chunkHandle = chunkStorage.create(newChunkName);

                        // Record the creation of new chunk.
                        chunksAddedCount++;
                        if (isSystemSegment) {
                            addSystemLogRecord(systemLogRecords,
                                    streamSegmentName,
                                    segmentMetadata.getLength(),
                                    lastChunkMetadata == null ? null : lastChunkMetadata.getName(),
                                    newChunkName);
                            txn.markPinned(chunkWrittenMetadata);
                        }
                        // Update read index.
                        newReadIndexEntries.add(new ChunkNameOffsetPair(segmentMetadata.getLength(), newChunkName));

                        // update first and last chunks.
                        segmentMetadata.setLastChunk(newChunkName);
                        if (lastChunkMetadata == null) {
                            segmentMetadata.setFirstChunk(newChunkName);
                        } else {
                            lastChunkMetadata.setNextChunk(newChunkName);
                        }
                        segmentMetadata.incrementChunkCount();

                        // Update the transaction.
                        txn.update(chunkWrittenMetadata);
                        if (lastChunkMetadata != null) {
                            txn.update(lastChunkMetadata);
                        }
                        txn.update(segmentMetadata);
                        segmentMetadata.setLastChunkStartOffset(segmentMetadata.getLength());

                        //
                        if (isFirstWriteAfterFailover) {
                            segmentMetadata.setOwnerEpoch(this.epoch);
                            isFirstWriteAfterFailover = false;
                            segmentMetadata.setOwnershipChanged(false);
                            log.debug("{} write - First write after failover - segment={}.", logPrefix, streamSegmentName);
                        }
                        didSegmentLayoutChange = true;
                        log.debug("{} write - New chunk added - segment={}, chunk={}, offset={}.", logPrefix, streamSegmentName, newChunkName, segmentMetadata.getLength());
                    } else {
                        // No new chunk needed just write data to existing chunk.
                        chunkWrittenMetadata = lastChunkMetadata;
                        chunkHandle = chunkStorage.openWrite(lastChunkMetadata.getName());
                    }

                    // Calculate the data that needs to be written.
                    long offsetToWriteAt = currentOffset - segmentMetadata.getLastChunkStartOffset();
                    int bytesToWrite = (int) Math.min(bytesRemaining, segmentMetadata.getMaxRollinglength() - offsetToWriteAt);
                    Preconditions.checkState(0 != bytesToWrite, "Attempt to write zero bytes");

                    try {
                        int bytesWritten;
                        // Finally write the data.
                        try (BoundedInputStream bis = new BoundedInputStream(data, bytesToWrite)) {
                            bytesWritten = chunkStorage.write(chunkHandle, offsetToWriteAt, bytesToWrite, bis);
                        }

                        // Update the counts
                        bytesRemaining -= bytesWritten;
                        currentOffset += bytesWritten;

                        // Update the metadata for segment and chunk.
                        Preconditions.checkState(bytesWritten >= 0);
                        segmentMetadata.setLength(segmentMetadata.getLength() + bytesWritten);
                        chunkWrittenMetadata.setLength(chunkWrittenMetadata.getLength() + bytesWritten);
                        txn.update(chunkWrittenMetadata);
                        txn.update(segmentMetadata);
                    } catch (IndexOutOfBoundsException e) {
                        throw new BadOffsetException(streamSegmentName, chunkStorage.getInfo(chunkHandle.getChunkName()).getLength(), offset);
                    }
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
                if (chunkStorage.exists(chunkTodelete)) {
                    chunkStorage.delete(chunkStorage.openWrite(chunkTodelete));
                    log.debug("{} collectGarbage - chunk={}.", logPrefix, chunkTodelete);
                }
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

    /**
     * Seals a StreamSegment. No further modifications are allowed on the StreamSegment after this operation completes.
     *
     * @param handle  A read-write SegmentHandle that points to a Segment to Seal.
     * @param timeout Timeout for the operation.
     * @return A CompletableFuture that, when completed, will indicate that the operation completed. If the operation
     * failed, it will contain the cause of the failure. Notable exceptions:
     * <ul>
     * <li> StreamSegmentNotExistsException: When the given Segment does not exist in Storage.
     * <li> StorageNotPrimaryException: When this Storage instance is no longer primary for this Segment (it was fenced out).
     * </ul>
     * @throws IllegalArgumentException If handle is read-only.
     */
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

                if (segmentMetadata.getOwnerEpoch() > this.epoch) {
                    throw new StorageNotPrimaryException(streamSegmentName);
                }

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

    /**
     * Concatenates two StreamSegments together. The Source StreamSegment will be appended as one atomic block at the end
     * of the Target StreamSegment (but only if its length equals the given offset), after which the Source StreamSegment
     * will cease to exist. Prior to this operation, the Source StreamSegment must be sealed.
     *
     * @param targetHandle  A read-write SegmentHandle that points to the Target StreamSegment. After this operation
     *                      is complete, this is the surviving StreamSegment.
     * @param offset        The offset in the Target StreamSegment to concat at.
     * @param sourceSegment The Source StreamSegment. This StreamSegment will be concatenated to the Target StreamSegment.
     *                      After this operation is complete, this StreamSegment will no longer exist.
     * @param timeout       Timeout for the operation.
     * @return A CompletableFuture that, when completed, will indicate the operation succeeded. If the operation failed,
     * it will contain the cause of the failure. Notable exceptions:
     * <ul>
     * <li> BadOffsetException: When the given offset does not match the actual length of the target segment in storage.
     * <li> StreamSegmentNotExistsException: When the either the source Segment or the target Segment do not exist in Storage.
     * <li> StorageNotPrimaryException: When this Storage instance is no longer primary for the target Segment (it was fenced out).
     * <li> IllegalStateException: When the Source Segment is not Sealed.
     * </ul>
     * @throws IllegalArgumentException If targetHandle is read-only.
     */
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

                // Validate preconditions.
                SegmentMetadata targetSegmentMetadata = (SegmentMetadata) txn.get(targetSegmentName);
                checkSegmentExists(targetSegmentName, targetSegmentMetadata);

                targetSegmentMetadata.checkInvariants();

                if (targetSegmentMetadata.isSealed()) {
                    throw new StreamSegmentSealedException(targetSegmentName);
                }

                SegmentMetadata sourceSegmentMetadata = (SegmentMetadata) txn.get(sourceSegment);
                checkSegmentExists(sourceSegment, sourceSegmentMetadata);

                sourceSegmentMetadata.checkInvariants();

                // This is a critical assumption at this point which should not be broken,
                Preconditions.checkState(!targetSegmentMetadata.isStorageSystemSegment(), "Storage system segments cannot be concatenated.");
                Preconditions.checkState(!sourceSegmentMetadata.isStorageSystemSegment(), "Storage system segments cannot be concatenated.");

                if (!sourceSegmentMetadata.isSealed()) {
                    throw new IllegalStateException();
                }

                if (targetSegmentMetadata.getOwnerEpoch() > this.epoch) {
                    throw new StorageNotPrimaryException(targetSegmentMetadata.getName());
                }

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
                if (shouldDefrag() && null != targetLastChunk) {
                    defrag(txn, targetSegmentMetadata, targetLastChunk.getName(), null);
                }

                targetSegmentMetadata.checkInvariants();

                // Finally commit transaction.
                txn.commit();

                Duration elapsed = timer.getElapsed();
                log.debug("{} concat - target={}, source={}, offset={}, latency={}.", logPrefix, targetHandle.getSegmentName(), sourceSegment, offset, elapsed.toMillis());
                LoggerHelpers.traceLeave(log, "concat", traceId, targetHandle, offset, sourceSegment);

                // Update the read index.
                readIndexCache.remove(sourceSegment);

            } catch (StorageMetadataWritesFencedOutException ex) {
                throw new StorageNotPrimaryException(targetSegmentName, ex);
            }

            return null;
        });
    }

    private boolean shouldAppend() {
        return chunkStorage.supportsAppend() && !config.isAppendsDisabled();
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
     * <li> In general without defrag the number of chunks in the system just keeps on increasing.
     * In addition when we have too many small chunks (say because too many small transactions), the segment is fragmented -
     * this may impact the read throughput but also performance of metadata store.
     * This problem is further intensified when we have stores that do not support append semantics (eg. vanilla S3) and each write becomes a separate chunk.
     * </li>
     * <li>
     * If underlying storage provides some facility to stitch together smaller chunks into larger chunks then we do actually
     * want to exploit that. Especially when this operation is supposed to be "metadata only operation" even for them.
     * Obviously both ECS and S3 have MPU and is supposed to be metadata only operation for them.
     * HDFS also has native concat (I think metadata only). NFS has no concept of native concat.
     * As chunks become larger, it no longer makes sense to concat them using append writes (read source completely and append -ie. write- it back at the end of target.)
     * We do not always use native concat to implement concat. We also use appends.
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
     * <div>
     * What controls whether we invoke native concat or simulate through appends?
     * There are a few different capabilities that ChunkStorageProvider needs to provide.
     * <ul>
     * <li>Does ChunkStorageProvider support appending to existing chunks? For vanilla S3 compatible this would return false.
     * This is indicated by supportsAppend.</li>
     * <li>Does ChunkStorageProvider support for concatenating chunks natively? This is indicated by supportsConcat.
     * If this is true then native concat operation concat will be invoked otherwise concatWithAppend is invoked.</li>
     * <li>There are some obvious constraints - For ChunkStorageProvider support any concat functionality it must support either append or native concat.</li>
     * <li>Also when ChunkStorageProvider supports both native and append, ChunkStorageManager will invoke appropriate method
     * depending on size of target and source chunks. (Eg. ECS)</li>
     * </ul>
     * </div>
     * <li>
     * What controls defrag?
     * There are two additional parameters that control when native concat
     * <li>minSizeLimitForNativeConcat : Size of chunk in bytes above which it is no longer considered a small object. For small source objects, concatWithAppend is used instead of using concat. (For really small txn it is rather efficient to use append than MPU).</li>
     * <li>maxSizeLimitForNativeConcat: Size of chunk in bytes above which it is no longer considered for concat. (Eg S3 might have max limit on chunk size).</li>
     * In short there is a size beyond which using append is not advisable. Conversely there is a size below which native concat is not efficient.(minSizeLimitForNativeConcat )
     * Then there is limit which concating does not make sense maxSizeLimitForNativeConcat
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
     * @throws Exception In case of any errors.
     */
    private void defrag(MetadataTransaction txn, SegmentMetadata segmentMetadata, String startChunkName, String lastChunkName) throws Exception {
        // The algorithm is actually very simple.
        // It tries to concat all small chunks using appends first.
        // Then it tries to concat remaining chunks using native concat if available.
        // To implement it using single loop we toggle between concat with append and native concat modes. (Instead of two passes.)
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

                if (useAppend && config.getMinSizeLimitForNativeConcat() < next.getLength()) {
                    break;
                }

                if (targetSizeAfterConcat + next.getLength() > segmentMetadata.getMaxRollinglength() || next.getLength() > config.getMaxSizeLimitForNativeConcat()) {
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

    /**
     * Deletes a StreamSegment.
     *
     * @param handle  A read-write SegmentHandle that points to a Segment to Delete.
     * @param timeout Timeout for the operation.
     * @return A CompletableFuture that, when completed, will indicate the operation succeeded. If the operation failed,
     * it will contain the cause of the failure. Notable exceptions:
     * <ul>
     * <li> StreamSegmentNotExistsException: When the given Segment does not exist in Storage.
     * <li> StorageNotPrimaryException: When this Storage instance is no longer primary for this Segment (it was fenced out).
     * </ul>
     * @throws IllegalArgumentException If handle is read-only.
     */
    @Override
    public CompletableFuture<Void> delete(SegmentHandle handle, Duration timeout) {
        checkInitialized();
        return execute(() -> {
            long traceId = LoggerHelpers.traceEnter(log, "delete", handle);
            Timer timer = new Timer();

            String streamSegmentName = handle.getSegmentName();
            try (MetadataTransaction txn = metadataStore.beginTransaction()) {
                SegmentMetadata segmentMetadata = (SegmentMetadata) txn.get(streamSegmentName);

                checkSegmentExists(streamSegmentName, segmentMetadata);

                if (segmentMetadata.getOwnerEpoch() > this.epoch) {
                    throw new StorageNotPrimaryException(streamSegmentName);
                }

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

    private void checkSegmentExists(String streamSegmentName, SegmentMetadata segmentMetadata) throws StreamSegmentNotExistsException {
        if (null == segmentMetadata || !segmentMetadata.isActive()) {
            throw new StreamSegmentNotExistsException(streamSegmentName);
        }
    }

    /**
     * Truncates all data in the given StreamSegment prior to the given offset. This does not fill the truncated data
     * in the segment with anything, nor does it "shift" the remaining data to the beginning. After this operation is
     * complete, any attempt to access the truncated data will result in an exception.
     * <p>
     * Notes:
     * * Depending on implementation, this may not truncate at the exact offset. It may truncate at some point prior to
     * the given offset, but it will never truncate beyond the offset.
     *
     * @param handle  A read-write SegmentHandle that points to a Segment to write to.
     * @param offset  The offset in the StreamSegment to truncate to.
     * @param timeout Timeout for the operation.
     * @return A CompletableFuture that, when completed, will indicate the operation succeeded. If the operation failed,
     * it will contain the cause of the failure. Notable exceptions:
     * <ul>
     * <li> StreamSegmentNotExistsException: When the given Segment does not exist in Storage.
     * </ul>
     */
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

                checkSegmentExists(streamSegmentName, segmentMetadata);

                if (segmentMetadata.isSealed()) {
                    throw new StreamSegmentSealedException(streamSegmentName);
                }

                if (segmentMetadata.getOwnerEpoch() > this.epoch) {
                    throw new StorageNotPrimaryException(streamSegmentName);
                }

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
                txn.commit(chunksToDelete.size() == 0); // if layout did not change then commit with lazyWrite.

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

    /**
     * Gets a value indicating whether this Storage implementation can truncate Segments.
     *
     * @return True or false.
     */
    @Override
    public boolean supportsTruncation() {
        return true;
    }

    /**
     * Lists all the segments stored on the storage device.
     *
     * @return Iterator that can be used to enumerate and retrieve properties of all the segments.
     * @throws IOException if exception occurred while listing segments.
     */
    @Override
    public Iterator<SegmentProperties> listSegments() throws IOException {
        throw new UnsupportedOperationException("listSegments is not yet supported");
    }

    /**
     * Opens the given Segment in read-only mode without acquiring any locks or blocking on any existing write-locks and
     * makes it available for use for this instance of Storage.
     * Multiple read-only Handles can coexist at any given time and allow concurrent read-only access to the Segment,
     * regardless of whether there is another non-read-only SegmentHandle that modifies the segment at that time.
     *
     * @param streamSegmentName Name of the StreamSegment to be opened in read-only mode.
     * @return A CompletableFuture that, when completed, will contain a read-only SegmentHandle that can be used to
     * access the segment for non-modify activities (ex: read, get). If the operation failed, it will be failed with the
     * cause of the failure. Notable exceptions:
     * <ul>
     * <li> StreamSegmentNotExistsException: When the given Segment does not exist in Storage.
     * </ul>
     */
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
                    claimOwnership(txn, segmentMetadata);
                }
                val retValue = SegmentStorageHandle.readHandle(streamSegmentName);
                LoggerHelpers.traceLeave(log, "openRead", traceId, retValue);
                return retValue;

            }
        });
    }

    /**
     * Reads a range of bytes from the StreamSegment.
     *
     * @param handle       A SegmentHandle (read-only or read-write) that points to a Segment to read from.
     * @param offset       The offset in the StreamSegment to read data from.
     * @param buffer       A buffer to use for reading data.
     * @param bufferOffset The offset in the buffer to start writing data to.
     * @param length       The number of bytes to read.
     * @param timeout      Timeout for the operation.
     * @return A CompletableFuture that, when completed, will contain the number of bytes read. If the operation failed,
     * it will contain the cause of the failure. Notable exceptions:
     * <ul>
     * <li> StreamSegmentNotExistsException: When the given Segment does not exist in Storage.
     * </ul>
     * @throws ArrayIndexOutOfBoundsException If bufferOffset or bufferOffset + length are invalid for the buffer.
     */
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
                        log.debug("{} read - found chunk to read - segment={}, chunk={}.", logPrefix, streamSegmentName, chunkToReadFrom);
                        break;
                    }
                    currentChunkName = chunkToReadFrom.getNextChunk();
                    startOffsetForCurrentChunk += chunkToReadFrom.getLength();

                    // Update read index with newly visited chunk.
                    if (null != currentChunkName) {
                        readIndexCache.addIndexEntry(streamSegmentName, currentChunkName, startOffsetForCurrentChunk);
                    }
                }

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

    /**
     * Gets current information about a StreamSegment.
     *
     * @param streamSegmentName The full name of the StreamSegment.
     * @param timeout           Timeout for the operation.
     * @return A CompletableFuture that, when completed, will contain the information requested about the StreamSegment.
     * If the operation failed, it will contain the cause of the failure. Notable exceptions:
     * <ul>
     * <li> StreamSegmentNotExistsException: When the given Segment does not exist in Storage.
     * </ul>
     */
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

    /**
     * Determines whether the given StreamSegment exists or not.
     *
     * @param streamSegmentName The name of the StreamSegment.
     * @param timeout           Timeout for the operation.
     * @return A CompletableFuture that, when completed, will contain the information requested. If the operation failed,
     * it will contain the cause of the failure.
     */
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

    private void checkInitialized() {
        Preconditions.checkState(null != this.metadataStore);
        Preconditions.checkState(0 != this.epoch);
        Preconditions.checkState(!closed.get());
    }
}
