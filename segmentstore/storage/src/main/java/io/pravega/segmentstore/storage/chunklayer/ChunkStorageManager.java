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
import io.pravega.common.io.BoundedInputStream;
import io.pravega.common.util.ImmutableDate;
import io.pravega.segmentstore.contracts.BadOffsetException;
import io.pravega.segmentstore.contracts.SegmentProperties;
import io.pravega.segmentstore.contracts.StreamSegmentException;
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
import io.pravega.segmentstore.storage.metadata.StorageMetadataNotFoundException;
import io.pravega.segmentstore.storage.metadata.StorageMetadataVersionMismatchException;
import io.pravega.segmentstore.storage.metadata.StorageMetadataWritesFencedOutException;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

import java.io.InputStream;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

//import static io.pravega.shared.NameUtils.getSegmentChunkName;

/**
 * Implements segment storage using {@link ChunkStorageProvider} and {@link ChunkMetadataStore}.
 * The metadata about the segments is stored in metadataStore using two types of records {@link SegmentMetadata} and {@link ChunkMetadata}.
 * Any changes to layout must be made inside a {@link MetadataTransaction} which will atomically change the records upon {@link MetadataTransaction#commit()}
 */
@Slf4j
public class ChunkStorageManager implements Storage {
    /**
     * Default SegmentRollingPolicy to use.
     */
    @Getter
    private SegmentRollingPolicy defaultRollingPolicy;

    /**
     * Metadata store containing all storage data.
     */
    @Getter
    private ChunkMetadataStore metadataStore;

    /**
     * Underlying {@link ChunkStorageProvider} to use to read and write data.
     */
    @Getter
    private ChunkStorageProvider chunkStorage;

    /**
     * Storage executor object.
     */
    private Executor executor;

    /**
     * Tracks whether this instance is closed or not.
     */
    private final AtomicBoolean closed;

    /**
     * Current epoch of the {@link Storage} instance.
     */
    @Getter
    private long epoch;

    /**
     * Id of the current Container.
     */
    @Getter
    private int containerId;

    /**
     * {@link SystemJournal} that logs all changes to system segment layout so that they can be are used during system bootstrap.
     */
    @Getter
    private SystemJournal systemJournal;

    /**
     * Index of chunks for a segment by their start offsets.
     */
    private ConcurrentHashMap<String, ConcurrentSkipListMap<Long, String>> cachedReadIndex = new ConcurrentHashMap<String, ConcurrentSkipListMap<Long, String>>();

    /**
     * Creates a new instance of the ChunkStorageManager class.
     * @param chunkStorage ChunkStorageProvider instance.
     * @param executor    An Executor for async operations.
     * @param defaultRollingPolicy A SegmentRollingPolicy to apply to every StreamSegment that does not have its own policy
     *                             defined.
     */
    public ChunkStorageManager(ChunkStorageProvider chunkStorage, Executor executor, SegmentRollingPolicy defaultRollingPolicy) {
        this.defaultRollingPolicy = Preconditions.checkNotNull(defaultRollingPolicy, "defaultRollingPolicy");
        this.chunkStorage = Preconditions.checkNotNull(chunkStorage, "chunkStorage");
        this.executor = Preconditions.checkNotNull(executor, "executor");
        this.closed = new AtomicBoolean(false);
    }

    /**
     * Creates a new instance of the ChunkStorageManager class.
     * @param chunkStorage ChunkStorageProvider instance.
     * @param metadataStore Metadata store.
     * @param executor    An Executor for async operations.
     * @param defaultRollingPolicy A SegmentRollingPolicy to apply to every StreamSegment that does not have its own policy
     *                             defined.
     */
    public ChunkStorageManager(ChunkStorageProvider chunkStorage, ChunkMetadataStore metadataStore, Executor executor, SegmentRollingPolicy defaultRollingPolicy) {
        this.defaultRollingPolicy = Preconditions.checkNotNull(defaultRollingPolicy, "defaultRollingPolicy");
        this.chunkStorage = Preconditions.checkNotNull(chunkStorage, "chunkStorage");
        this.metadataStore = Preconditions.checkNotNull(metadataStore, "metadataStore");
        this.executor = Preconditions.checkNotNull(executor, "executor");
        this.closed = new AtomicBoolean(false);
    }

    /**
     * Initializes the ChunkStorageManager.
     * @param metadataStore Metadata store.
     * @param containerId container id.
     * @param systemJournal SystemJournal that keeps track of changes to system segments and helps with bootstrap.
     * @throws Exception Any exceptions.
     */
    public void initialize(int containerId, ChunkMetadataStore metadataStore, SystemJournal systemJournal) throws Exception {
        this.containerId = containerId;
        this.metadataStore = Preconditions.checkNotNull(metadataStore, "metadataStore");
        this.systemJournal = Preconditions.checkNotNull(systemJournal, "systemJournal");
        this.closed.set(false);
    }

    /**
     * Initializes this instance with the given ContainerEpoch.
     *
     * @param containerEpoch The Container Epoch to initialize with.
     */
    @Override
    public void initialize(long containerEpoch) {
        this.epoch = containerEpoch;
        //Preconditions.checkState(!closed.get());
        this.closed.set(false);
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
        return execute( () -> {
            long traceId = LoggerHelpers.traceEnter(log, "openWrite", streamSegmentName);
            Preconditions.checkNotNull(streamSegmentName, "streamSegmentName");
            try (MetadataTransaction txn = metadataStore.beginTransaction()) {
                SegmentMetadata segmentMetadata = (SegmentMetadata) metadataStore.get(txn, streamSegmentName);
                if (null == segmentMetadata || !segmentMetadata.isActive()) {
                    throw new StreamSegmentNotExistsException(streamSegmentName);
                }
                segmentMetadata.checkInvariants();
                // This segment was created by an older segment store. Need to start a fresh new chunk.
                if (segmentMetadata.getOwnerEpoch() < this.epoch) {
                    log.debug("openWrite:Segment needs ownership change. segment: {} ", segmentMetadata.getName());
                    // Claim ownership.
                    // This is safe because the previous instance is definitely not an owner anymore. (even if this instance is no more owner)
                    segmentMetadata.setOwnerEpoch(this.epoch);
                    segmentMetadata.setOwnershipChanged(true);

                    // Get the last chunk
                    String lastChunkName = segmentMetadata.getLastChunk();
                    if (null != lastChunkName) {
                        ChunkMetadata lastChunk = (ChunkMetadata) metadataStore.get(txn, lastChunkName);
                        ChunkInfo chunkInfo = chunkStorage.getInfo(lastChunkName);
                        Preconditions.checkState( chunkInfo != null);
                        Preconditions.checkState( lastChunk != null);
                        // Adjust its length;
                        if (chunkInfo.getLength() != lastChunk.getLength()) {
                            Preconditions.checkState( chunkInfo.getLength() > lastChunk.getLength());
                            // Whatever length you see right now is the final "sealed" length of the last chunk.
                            lastChunk.setLength((int) chunkInfo.getLength());
                            segmentMetadata.setLength(segmentMetadata.getLastChunkStartOffset() + lastChunk.getLength());
                            metadataStore.update(txn, lastChunk);
                            log.debug("openWrite:Length of last chunk addjusted. segment: {}, last chunk: {} Length: {}",
                                    segmentMetadata.getName(),
                                    lastChunk.getName(),
                                    chunkInfo.getLength());
                        }
                    }
                    // Update and commit
                    // If This instance is fenced this update will fail.
                    metadataStore.update(txn, segmentMetadata);
                    txn.commit();
                }
                // If created by newer instance then abort.
                if (segmentMetadata.getOwnerEpoch() > this.epoch) {
                    throw new StorageNotPrimaryException(streamSegmentName);
                }

                // This instance is the owner, return a handle.
                val retValue = SegmentStorageHandle.writeHandle(streamSegmentName);
                LoggerHelpers.traceLeave(log, "openWrite", traceId, retValue);
                return retValue;
            } catch (StorageMetadataVersionMismatchException ex) {
                throw ex;
            } catch (StorageMetadataWritesFencedOutException ex) {
                throw new StorageNotPrimaryException(streamSegmentName, ex);
            } catch (StreamSegmentException se) {
                throw se;
            }
        });
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
        return execute( () -> {
            long traceId = LoggerHelpers.traceEnter(log, "create", streamSegmentName, rollingPolicy);

            try (MetadataTransaction txn = metadataStore.beginTransaction()) {
                // Retrieve metadata and make sure it does not exist.
                SegmentMetadata oldSegmentMetadata = (SegmentMetadata) metadataStore.get(txn, streamSegmentName);
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
                metadataStore.create(txn, newSegmentMetatadata);
                // commit.
                txn.commit();

                val retValue =  SegmentStorageHandle.writeHandle(streamSegmentName);
                LoggerHelpers.traceLeave(log, "create", traceId, retValue);
                return retValue;
            } catch (StorageMetadataAlreadyExistsException ex) {
                throw new StreamSegmentExistsException(streamSegmentName, ex);
            } catch (StorageMetadataVersionMismatchException ex) {
                throw ex;
            } catch (StorageMetadataWritesFencedOutException ex) {
                throw new StorageNotPrimaryException(streamSegmentName, ex);
            }
            //return SegmentStorageHandle.writeHandle(streamSegmentName);
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
        return execute( () -> {
            long traceId = LoggerHelpers.traceEnter(log, "write", handle, offset, length);
            // Validate preconditions.
            Preconditions.checkNotNull(handle, "handle");
            Preconditions.checkNotNull(data, "data");
            String streamSegmentName = handle.getSegmentName();
            Preconditions.checkNotNull(streamSegmentName, "streamSegmentName");
            Preconditions.checkArgument(!handle.isReadOnly(), "handle");

            ArrayList<String> systemLogRecords = new ArrayList<>();
            HashMap<Long, String> newReadIndexEntries = new HashMap<Long, String>();
            int chunksAddedCount = 0;

            try (MetadataTransaction txn = metadataStore.beginTransaction()) {
                boolean didSegmentLayoutChange = false;

                // Retrieve metadata.
                SegmentMetadata segmentMetadata = (SegmentMetadata) metadataStore.get(txn, streamSegmentName);

                // Validate preconditions.
                if (null == segmentMetadata || !segmentMetadata.isActive()) {
                    throw new StreamSegmentNotExistsException(streamSegmentName);
                }

                segmentMetadata.checkInvariants();

                if (segmentMetadata.isSealed()) {
                    throw new StreamSegmentSealedException(streamSegmentName);
                }

                if (segmentMetadata.getOwnerEpoch() > this.epoch) {
                    throw new StorageNotPrimaryException(streamSegmentName);
                }

                // Check if this is a first write after ownership changed.
                boolean isFirstWriteAfterFailover =  segmentMetadata.isOwnershipChanged();

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
                        lastChunkMetadata = (ChunkMetadata) metadataStore.get(txn, segmentMetadata.getLastChunk());
                    }

                    // Check if new chunk needs to be added.
                    // This could be either because there are no existing chunks or last chunk has reached max rolling length.
                    if ( null == lastChunkMetadata
                        || (lastChunkMetadata.getLength() >= segmentMetadata.getMaxRollinglength())
                        || isFirstWriteAfterFailover
                        || !chunkStorage.supportsAppend()) {

                        // Create new chunk
                        String newChunkName = getNewChunkName(streamSegmentName,
                                segmentMetadata.getLength());

                        chunkWrittenMetadata = ChunkMetadata.builder()
                                .name(newChunkName)
                                .build();
                        chunkHandle = chunkStorage.create(newChunkName);

                        // Record the creation of new chunk.
                        chunksAddedCount++;
                        addSystemLogRecord(systemLogRecords, streamSegmentName, segmentMetadata.getLength(), lastChunkMetadata, newChunkName);

                        // Update read index.
                        newReadIndexEntries.put(segmentMetadata.getLength(), newChunkName);

                        // update first and last chunks.
                        segmentMetadata.setLastChunk(newChunkName);
                        if (lastChunkMetadata == null) {
                            segmentMetadata.setFirstChunk(newChunkName);
                        } else {
                            lastChunkMetadata.setNextChunk(newChunkName);
                        }

                        // Update the transaction.
                        metadataStore.update(txn, chunkWrittenMetadata);
                        if (lastChunkMetadata != null) {
                            metadataStore.update(txn, lastChunkMetadata);
                        }
                        metadataStore.update(txn, segmentMetadata);
                        segmentMetadata.setLastChunkStartOffset(segmentMetadata.getLength());

                        //
                        if (isFirstWriteAfterFailover) {
                            segmentMetadata.setOwnerEpoch(this.epoch);
                            isFirstWriteAfterFailover = false;
                            segmentMetadata.setOwnershipChanged(false);
                            log.debug("write: First write after failover. segment: {}", streamSegmentName);
                        }
                        didSegmentLayoutChange = true;
                        log.debug("write: New chunk added. segment: {} chunk: {} offset:{}", streamSegmentName, newChunkName, segmentMetadata.getLength());
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
                        if (0 == bytesWritten) {
                            // Log warning
                            log.warn("No bytes written");
                        }

                        // Update the
                        bytesRemaining -= bytesWritten;
                        currentOffset += bytesWritten;

                        // Update the metadata for segment and chunk.
                        Preconditions.checkState(bytesWritten >= 0);
                        segmentMetadata.setLength(segmentMetadata.getLength() + bytesWritten);
                        chunkWrittenMetadata.setLength(chunkWrittenMetadata.getLength() + bytesWritten);
                        metadataStore.update(txn, chunkWrittenMetadata);
                        metadataStore.update(txn, segmentMetadata);
                    } catch (IndexOutOfBoundsException e) {
                        throw new BadOffsetException(streamSegmentName, chunkStorage.getInfo(chunkHandle.getChunkName()).getLength(), offset);
                    }
                }

                // Check invariants.
                segmentMetadata.checkInvariants();
                // if layout did not change or it is system segment then commit with lazyWrite.
                txn.commit(!didSegmentLayoutChange || isSystemSegment(streamSegmentName));

                if (isSystemSegment(streamSegmentName) && chunksAddedCount > 0) {
                    Preconditions.checkState(chunksAddedCount == systemLogRecords.size());
                    systemJournal.commitRecords(systemLogRecords);

                    // Update the read index.
                    val readIndex = getReadIndex(streamSegmentName);
                    for (val entry : newReadIndexEntries.entrySet()) {
                        readIndex.put(entry.getKey(), entry.getValue());
                    }
                }

                LoggerHelpers.traceLeave(log, "write", traceId, handle, offset);
                return null;
            } catch (StorageMetadataNotFoundException ex) {
                throw new StreamSegmentNotExistsException(streamSegmentName, ex);
            } catch (StorageMetadataVersionMismatchException ex) {
                throw new BadOffsetException(streamSegmentName, offset, offset); // TODO FIX THIS
            } catch (StorageMetadataWritesFencedOutException ex) {
                throw new StorageNotPrimaryException(streamSegmentName, ex);
            } catch (Exception ex) {
                throw ex;
            }
        });
    }

    private boolean isSystemSegment(String streamSegmentName) {
        return null != systemJournal && systemJournal.isSystemSegment(streamSegmentName);
    }

    private void addSystemLogRecord(ArrayList<String> systemLogRecords, String streamSegmentName, long offset, ChunkMetadata lastChunkMetadata, String newChunkName) {
        if (isSystemSegment(streamSegmentName)) {
            val systemLogRecord = systemJournal.getChunkAddedRecord(streamSegmentName,
                offset,
                lastChunkMetadata == null ? null : lastChunkMetadata.getName(),
                newChunkName);
            systemLogRecords.add(systemLogRecord);
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
        return execute( () -> {
            long traceId = LoggerHelpers.traceEnter(log, "seal", handle);
            Preconditions.checkNotNull(handle, "handle");
            String streamSegmentName = handle.getSegmentName();
            Preconditions.checkNotNull(streamSegmentName, "streamSegmentName");
            Preconditions.checkArgument(!handle.isReadOnly(), "handle");

            try (MetadataTransaction txn = metadataStore.beginTransaction()) {
                SegmentMetadata segmentMetadata = (SegmentMetadata) metadataStore.get(txn, streamSegmentName);
                // Validate preconditions.
                if (null == segmentMetadata || !segmentMetadata.isActive()) {
                    throw new StreamSegmentNotExistsException(streamSegmentName);
                }

                if (segmentMetadata.getOwnerEpoch() > this.epoch) {
                    throw new StorageNotPrimaryException(streamSegmentName);
                }

                // seal if it is not already sealed.
                if (!segmentMetadata.isSealed()) {
                    segmentMetadata.setSealed(true);
                    metadataStore.update(txn, segmentMetadata);
                    txn.commit();
                }
                LoggerHelpers.traceLeave(log, "seal", traceId, handle);
                return null;
            } catch (StorageMetadataVersionMismatchException ex) {
                throw ex;
            } catch (StorageMetadataNotFoundException ex) {
                throw new StreamSegmentNotExistsException(streamSegmentName, ex);
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
        return execute( () -> {
            long traceId = LoggerHelpers.traceEnter(log, "concat", targetHandle, offset, sourceSegment);

            String targetSegmentName = targetHandle.getSegmentName();
            try (MetadataTransaction txn = metadataStore.beginTransaction()) {

                // Validate preconditions.
                SegmentMetadata targetSegmentMetadata = (SegmentMetadata) metadataStore.get(txn, targetSegmentName);
                if (null == targetSegmentMetadata || !targetSegmentMetadata.isActive()) {
                    throw new StreamSegmentNotExistsException(targetSegmentName);
                }

                targetSegmentMetadata.checkInvariants();

                SegmentMetadata sourceSegmentMetadata = (SegmentMetadata) metadataStore.get(txn, sourceSegment);
                if (null == sourceSegmentMetadata || !sourceSegmentMetadata.isActive()) {
                    throw new StreamSegmentNotExistsException(sourceSegment);
                }

                sourceSegmentMetadata.checkInvariants();

                if (!sourceSegmentMetadata.isSealed()) {
                    throw new IllegalStateException();
                }

                if (targetSegmentMetadata.getOwnerEpoch() > this.epoch) {
                    throw new StorageNotPrimaryException(targetSegmentMetadata.getName());
                }

                if (sourceSegmentMetadata.getStartOffset() != 0) {
                    throw new StreamSegmentTruncatedException(targetSegmentName, targetSegmentMetadata.getLength(), 0);
                }

                // The Entire source is before end of target, just treat it as no-op.
                if (offset + sourceSegmentMetadata.getLength() <= targetSegmentMetadata.getLength()) {
                    return null;
                }

                // Update list of chunks by appending sources list of chunks.
                ChunkMetadata targetLastChunk = (ChunkMetadata) metadataStore.get(txn, targetSegmentMetadata.getLastChunk());
                ChunkMetadata sourceFirstChunk = (ChunkMetadata) metadataStore.get(txn, sourceSegmentMetadata.getFirstChunk());

                if (targetLastChunk != null) {
                    targetLastChunk.setNextChunk(sourceFirstChunk.getName());
                } else {
                    targetSegmentMetadata.setFirstChunk(sourceFirstChunk.getName());
                }

                // Update segments's last chunk to point to the sources last segment.
                targetSegmentMetadata.setLastChunk(sourceSegmentMetadata.getLastChunk());

                // Update the length of segment.
                targetSegmentMetadata.setLastChunkStartOffset(targetSegmentMetadata.getLength() + sourceSegmentMetadata.getLastChunkStartOffset());
                targetSegmentMetadata.setLength(targetSegmentMetadata.getLength() + sourceSegmentMetadata.getLength() - sourceSegmentMetadata.getStartOffset());

                metadataStore.update(txn, targetLastChunk);
                metadataStore.update(txn, sourceFirstChunk);
                metadataStore.update(txn, targetSegmentMetadata);
                metadataStore.delete(txn, sourceSegment);

                if (chunkStorage.supportsConcat() && null != targetLastChunk ) {
                    defrag(txn, targetSegmentMetadata, targetLastChunk.getName());
                }

                targetSegmentMetadata.checkInvariants();

                // Finally commit transaction.
                txn.commit();
                LoggerHelpers.traceLeave(log, "concat", traceId, targetHandle, offset, sourceSegment);

                // Update the read index.
                cachedReadIndex.remove(sourceSegment);

            } catch (StorageMetadataVersionMismatchException ex) {
                throw ex;
            } catch (StorageMetadataNotFoundException ex) {
                throw new StreamSegmentNotExistsException(targetSegmentName, ex);
            } catch (StorageMetadataWritesFencedOutException ex) {
                throw new StorageNotPrimaryException(targetSegmentName, ex);
            }

            return null;
        });
    }

    private void defrag(MetadataTransaction txn, SegmentMetadata targetSegmentMetadata, String targetChunkName) throws Exception {
        // Iterate through chunk list
        while (null != targetChunkName) {
            ChunkMetadata target = (ChunkMetadata) metadataStore.get(txn, targetChunkName);

            ArrayList<String> sources = new ArrayList<>();
            long size = target.getLength();

            String nextChunkName = target.getNextChunk();
            ChunkMetadata next = null;

            // Gather list of chunks that can be appended together.
            while (null != nextChunkName) {
                next = (ChunkMetadata) metadataStore.get(txn, nextChunkName);

                if (size + next.getLength() <= targetSegmentMetadata.getMaxRollinglength()) {
                    sources.add(nextChunkName);
                    size += next.getLength();
                } else {
                    break;
                }
                nextChunkName = next.getNextChunk();
            }
            // Note - After this loop is exited nextChunkName points to chunk next to last one to be concat.
            // Which means target should now point to it as next after concat is complete.

            // If there are chunks that can be appended together then concat them.
            if (sources.size() > 0) {
                // Concat
                ChunkHandle[] arr = new ChunkHandle[sources.size()];
                arr = sources.stream().map(chunkName -> ChunkHandle.readHandle(chunkName)).collect(Collectors.toList()).toArray(arr);
                int length = chunkStorage.concat(ChunkHandle.writeHandle(targetChunkName), arr);

                // Set the pointers
                target.setLength(size);
                target.setNextChunk(nextChunkName);

                // If target is the last chunk after this then update metadata accordingly
                if (null == nextChunkName) {
                    targetSegmentMetadata.setLastChunk(target.getName());
                    targetSegmentMetadata.setLastChunkStartOffset(targetSegmentMetadata.getLength() - target.getLength());
                }

                // Update metadata for affected chunks.
                for (String chunkName : sources) {
                    metadataStore.delete(txn, chunkName);
                }
                metadataStore.update(txn, target);
                metadataStore.update(txn, targetSegmentMetadata);
            }

            // Move on to next place in list where we can concat.
            targetChunkName = nextChunkName;
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
        return execute( () -> {
            long traceId = LoggerHelpers.traceEnter(log, "delete", handle);
            String streamSegmentName = handle.getSegmentName();
            try (MetadataTransaction txn = metadataStore.beginTransaction()) {
                SegmentMetadata segmentMetadata = (SegmentMetadata) metadataStore.get(txn, streamSegmentName);

                if (null != segmentMetadata) {
                    if (segmentMetadata.getOwnerEpoch() > this.epoch) {
                        throw new StorageNotPrimaryException(streamSegmentName);
                    }

                    segmentMetadata.setActive(false);

                    // Delete chunks
                    String currentChunkName = segmentMetadata.getFirstChunk();
                    ChunkMetadata currentMetadata;
                    ArrayList<String> chunksToDelete = new ArrayList<>();
                    while (currentChunkName != null) {
                        currentMetadata = (ChunkMetadata) metadataStore.get(txn, currentChunkName);
                        // Delete underlying file.
                        chunksToDelete.add(currentChunkName);
                        currentChunkName = currentMetadata.getNextChunk();
                        metadataStore.delete(txn, currentMetadata.getName());
                    }

                    // Commit.
                    metadataStore.delete(txn, streamSegmentName);
                    txn.commit();

                    for (String toDelete : chunksToDelete) {
                        chunkStorage.delete(chunkStorage.openWrite(toDelete));
                    }

                    // Update the read index.
                    cachedReadIndex.remove(streamSegmentName);
                }
                LoggerHelpers.traceLeave(log, "delete", traceId, handle);
                return null;
            } catch (StorageMetadataVersionMismatchException ex) {
                throw ex;
            } catch (StorageMetadataNotFoundException ex) {
                throw new StreamSegmentNotExistsException(streamSegmentName, ex);
            } catch (StorageMetadataWritesFencedOutException ex) {
                throw new StorageNotPrimaryException(streamSegmentName, ex);
            }
        });
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
        return execute( () -> {
            long traceId = LoggerHelpers.traceEnter(log, "truncate", handle, offset);
            String streamSegmentName = handle.getSegmentName();
            try (MetadataTransaction txn = metadataStore.beginTransaction()) {
                SegmentMetadata segmentMetadata = (SegmentMetadata) metadataStore.get(txn, streamSegmentName);

                if (!segmentMetadata.isActive()) {
                    throw new StreamSegmentNotExistsException(streamSegmentName);
                }

                if (segmentMetadata.getOwnerEpoch() > this.epoch) {
                    throw new StorageNotPrimaryException(streamSegmentName);
                }

                if (segmentMetadata.getLength() <= offset || segmentMetadata.getStartOffset() > offset) {
                    throw new IllegalArgumentException(streamSegmentName);
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
                    currentMetadata = (ChunkMetadata) metadataStore.get(txn, currentChunkName);
                    Preconditions.checkState(null != currentMetadata, "currentMetadata is null.");

                    // If for given chunk start <= offset < end  then we have found the chunk that will be the first chunk.
                    if ((startOffset <= offset) && (startOffset + currentMetadata.getLength() > offset)) {
                        break;
                    }

                    startOffset += currentMetadata.getLength();
                    chunksToDelete.add(currentMetadata.getName());

                    // move to next chunk
                    currentChunkName = currentMetadata.getNextChunk();
                }
                segmentMetadata.setFirstChunk(currentChunkName);
                segmentMetadata.setStartOffset(offset);
                segmentMetadata.setFirstChunkStartOffset(startOffset);
                for (String toDelete : chunksToDelete) {
                    metadataStore.delete(txn, toDelete);
                }
                metadataStore.update(txn, segmentMetadata);

                // Check invariants.
                Preconditions.checkState(segmentMetadata.getLength() == oldLength, "truncate should not change segment length");
                segmentMetadata.checkInvariants();

                // Finally commit.
                txn.commit(chunksToDelete.size() == 0); // if layout did not change then commit with lazyWrite.

                if (null != systemJournal && systemJournal.isSystemSegment(streamSegmentName)) {
                    systemJournal.commitRecord(systemJournal.getSegmentTruncatedRecord(streamSegmentName,
                                                                    offset,
                                                                    segmentMetadata.getFirstChunk(),
                                                                    startOffset));
                }

                for (String toDelete : chunksToDelete) {
                    chunkStorage.delete(chunkStorage.openWrite(toDelete));
                }

                // Update the read index by removing all entries below truncate offset.
                val readIndex = getReadIndex(streamSegmentName);
                if (null != readIndex) {
                    val headMap = readIndex.headMap(segmentMetadata.getStartOffset());
                    if (null != headMap) {
                        ArrayList<Long> keysToRemove = new ArrayList<Long>();
                        keysToRemove.addAll(headMap.keySet());
                        for (val keyToRemove : keysToRemove) {
                            cachedReadIndex.remove(keyToRemove);
                        }
                    }
                }

                LoggerHelpers.traceLeave(log, "truncate", traceId, handle, offset);
                return null;
            } catch (StorageMetadataNotFoundException ex) {
                throw new StreamSegmentNotExistsException(streamSegmentName, ex);
            } catch (StorageMetadataVersionMismatchException ex) {
                throw ex;
            } catch (StorageMetadataWritesFencedOutException ex) {
                throw new StorageNotPrimaryException(streamSegmentName, ex);
            }
            //return null;
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
        return execute( () -> {
            long traceId = LoggerHelpers.traceEnter(log, "openRead", streamSegmentName);
            // Validate preconditions and return handle.
            Preconditions.checkNotNull(streamSegmentName, "streamSegmentName");
            try (MetadataTransaction txn = metadataStore.beginTransaction()) {
                SegmentMetadata segmentMetadata = (SegmentMetadata) metadataStore.get(txn, streamSegmentName);
                if (null == segmentMetadata || !segmentMetadata.isActive()) {
                    throw new StreamSegmentNotExistsException(streamSegmentName);
                }
                val retValue = SegmentStorageHandle.readHandle(streamSegmentName);
                LoggerHelpers.traceLeave(log, "openRead", traceId, retValue);
                return retValue;

            } catch (StorageMetadataNotFoundException ex) {
                throw new StreamSegmentNotExistsException(streamSegmentName, ex);
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
        return execute( () -> {
            long traceId = LoggerHelpers.traceEnter(log, "read", handle, offset, length);
            // Validate preconditions.
            Preconditions.checkNotNull(handle, "handle");
            Preconditions.checkNotNull(buffer, "buffer");
            String streamSegmentName = handle.getSegmentName();
            Preconditions.checkNotNull(streamSegmentName, "streamSegmentName");
            if (bufferOffset < 0 || bufferOffset > buffer.length || bufferOffset + length >  buffer.length) {
                throw new ArrayIndexOutOfBoundsException("bufferOffset");
            }

            try (MetadataTransaction txn = metadataStore.beginTransaction()) {
                SegmentMetadata segmentMetadata = (SegmentMetadata) metadataStore.get(txn, streamSegmentName);

                // Validate preconditions.
                if (null == segmentMetadata || !segmentMetadata.isActive()) {
                    throw new StreamSegmentNotExistsException(streamSegmentName);
                }

                segmentMetadata.checkInvariants();

                if (length == 0) {
                    return 0;
                }

                if ( offset > segmentMetadata.getLength() || offset < 0) {
                    throw new ArrayIndexOutOfBoundsException("offset = " + offset + "  segmentMetadata =" + segmentMetadata);
                }

                if (offset < segmentMetadata.getStartOffset() ) {
                    throw new StreamSegmentTruncatedException(streamSegmentName, segmentMetadata.getStartOffset(), offset);
                }

                String currentChunkName = segmentMetadata.getFirstChunk();
                ChunkMetadata chunkToReadFrom = null;

                if (null == currentChunkName) {
                    throw new ArrayIndexOutOfBoundsException("offset");
                }

                int bytesRemaining = length;
                int currentBufferOffset = bufferOffset;
                long currentOffset = offset;
                int totalBytesRead = 0;

                // Find the first chunk that contains the data.
                long startOffsetForCurrentChunk = segmentMetadata.getFirstChunkStartOffset();

                // Find the name of the chunk in the cached read index that is floor to required offset.
                val readIndex = getReadIndex(streamSegmentName);
                if (readIndex.size() > 0) {
                    val floorEntry = readIndex.floorEntry(offset);
                    if (null != floorEntry) {
                        startOffsetForCurrentChunk = floorEntry.getKey();
                        currentChunkName = floorEntry.getValue();
                    }
                }

                // Navigate to the chunk that contains the first byte of requested data.
                while (currentChunkName != null) {
                    chunkToReadFrom = (ChunkMetadata) metadataStore.get(txn, currentChunkName);
                    Preconditions.checkState(null != chunkToReadFrom, "chunkToReadFrom is null");
                    if (   startOffsetForCurrentChunk <= currentOffset
                        && startOffsetForCurrentChunk + chunkToReadFrom.getLength() > currentOffset) {
                        // we have found a chunk that contains first byte we want to read
                        log.debug("read: found chunk to read segment: {} chunk: {}", streamSegmentName, chunkToReadFrom);
                        break;
                    }
                    currentChunkName = chunkToReadFrom.getNextChunk();
                    startOffsetForCurrentChunk += chunkToReadFrom.getLength();

                    // Update read index with newly visited chunk.
                    if (null != currentChunkName) {
                        readIndex.put(startOffsetForCurrentChunk, currentChunkName);
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
                            chunkToReadFrom = (ChunkMetadata) metadataStore.get(txn, currentChunkName);
                            log.debug("read: reading from next chunk segment: {} chunk: {}", streamSegmentName, chunkToReadFrom);
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

                if (0 == totalBytesRead) {
                    log.debug("zero bytes read");
                }
                LoggerHelpers.traceLeave(log, "read", traceId, handle, offset, totalBytesRead);
                return totalBytesRead;
            } catch (StorageMetadataNotFoundException ex) {
                throw new StreamSegmentNotExistsException(streamSegmentName, ex);
            } catch (StorageMetadataVersionMismatchException ex) {
                throw ex;
            } catch (StorageMetadataWritesFencedOutException ex) {
                throw new StorageNotPrimaryException(streamSegmentName, ex);
            } catch (Exception ex) {
                throw ex;
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
        return execute( () -> {
            long traceId = LoggerHelpers.traceEnter(log, "getStreamSegmentInfo", streamSegmentName);
            Preconditions.checkNotNull(streamSegmentName, "streamSegmentName");
            try (MetadataTransaction txn = metadataStore.beginTransaction()) {
                SegmentMetadata segmentMetadata = (SegmentMetadata) metadataStore.get(txn, streamSegmentName);
                if (null == segmentMetadata ) {
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
            } catch (StorageMetadataNotFoundException ex) {
                throw new StreamSegmentNotExistsException(streamSegmentName, ex);
            } catch (StorageMetadataVersionMismatchException ex) {
                throw ex;
            } catch (StorageMetadataWritesFencedOutException ex) {
                throw new StorageNotPrimaryException(streamSegmentName, ex);
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
        return execute( () -> {
            long traceId = LoggerHelpers.traceEnter(log, "exists", streamSegmentName);
            Preconditions.checkNotNull(streamSegmentName, "streamSegmentName");
            try (MetadataTransaction txn = metadataStore.beginTransaction()) {
                SegmentMetadata segmentMetadata = (SegmentMetadata) metadataStore.get(txn, streamSegmentName);
                val retValue = segmentMetadata == null ? false : segmentMetadata.isActive();
                LoggerHelpers.traceLeave(log, "exists", traceId, retValue);
                return retValue;
            } catch (StorageMetadataNotFoundException ex) {
                throw new StreamSegmentNotExistsException(streamSegmentName, ex);
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

        }
        this.closed.set(true);
    }

    /**
     *
     * @param streamSegmentName
     * @return
     */
    private ConcurrentSkipListMap<Long, String> getReadIndex(String streamSegmentName) {
        ConcurrentSkipListMap<Long, String> readIndex;
        if (cachedReadIndex.containsKey(streamSegmentName)) {
            readIndex = cachedReadIndex.get(streamSegmentName);
        } else {
            val newReadIndex = new ConcurrentSkipListMap<Long, String>();
            val oldReadIndex = cachedReadIndex.putIfAbsent(streamSegmentName, newReadIndex);
            readIndex = null != oldReadIndex ? oldReadIndex : newReadIndex;
        }
        return readIndex;
    }

    /**
     * Executes the given Callable and returns its result, while translating any Exceptions bubbling out of it into
     * StreamSegmentExceptions.
     *
     * @param operation     The function to execute.
     * @param <R>           Return type of the operation.
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
        //String name = getSegmentChunkName(segmentName, offset);
        //return name;
        return java.util.UUID.randomUUID().toString();
    }

    private void checkInitialized() {
        Preconditions.checkState(null != this.metadataStore);
        Preconditions.checkState(0 != this.epoch);
        Preconditions.checkState(!closed.get());
    }
}
