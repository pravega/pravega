/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.storage.impl.bookkeepertier2;

import com.google.common.base.Preconditions;
import io.pravega.common.LoggerHelpers;
import io.pravega.segmentstore.contracts.SegmentProperties;
import io.pravega.segmentstore.contracts.StreamSegmentNotExistsException;
import io.pravega.segmentstore.storage.SegmentHandle;
import io.pravega.segmentstore.storage.Storage;
import java.io.InputStream;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;

/**
 * Storage adapter for Apache bookkeeper based storage.
 *
 * Each segment is represented by a log built on top of bookkeeper ledgers. This implementation follows some of recommendations
 * here: https://bookkeeper.apache.org/docs/r4.4.0/bookkeeperLedgers2Logs.html
 * This log is called a StorageLedger. A storage ledger consists of a number of ledgers and metadata for the segment and the ledgers.
 * The metadata is stored in ZK. It is accessed using the async curator framework.
 *
 * Fencing: The recommended implementation of fencing as described in the URL ensures that the latest caller owns the log.
 * In case of Storage implementation the requirement is different. A Storage with higher epoc is supposed to own the log,
 * irrespective of the time the fencing happens. Because of this a CAS operation in ZK for a storage ledger decides ownership.
 *
 * Here is the algorithm that describes ownership change through the openWrite() call:
 *
 * 1. Check the current epoc for the given StorageLedger.
 * 2. If the current epoc is larger, set it to the new epoc, other wise throw StorageNotPrimary error.
 * 3. Try and fence all the ledgers. The last ledger may be empty, in this case delete the ledger.
 * 4. Create a new ledger and add it to the list of ledgers as well as to the ZK.
 *
 * Concat: Apache bookkeeper does not have a native concat which appends two ledgers on the server side. To overcome this,
 * concatenation is implemented as a pure metadata operation. Here is the algorithm used for concat() call:
 *
 * 1. Get the target and source ledgers.
 * 2. Update the offset of the source ledgers to the offsets after the concatenation.
 * 3. Update all the metadata about ledgers in one transaction using transaction() API for curator.
 */
@Slf4j
public class BookkeeperStorage implements Storage {
    private static final int NUM_RETRIES = 3;

    //region members

    private final BookkeeperStorageConfig config;
    private final ExecutorService executor;
    private final AtomicBoolean closed;
    private final StorageLedgerManager manager;
    private final CuratorFramework zkClient;

    //endregion

    //region constructor

    /**
     * Creates a new instance of the BookkeeperStorage class.
     *
     * @param config   The configuration to use.
     * @param zkClient Curator framework to interact with ZK.
     * @param executor The executor to use for running async operations.
     */
    public BookkeeperStorage(BookkeeperStorageConfig config, CuratorFramework zkClient, ExecutorService executor) {
        Preconditions.checkNotNull(config, "config");
        Preconditions.checkNotNull(executor, "executor");
        this.closed = new AtomicBoolean(false);
        this.config = config;
        this.zkClient = zkClient;
        this.executor = executor;
        manager = new StorageLedgerManager(config, zkClient, executor);
    }

    //endregion

    //region Storage implementation

    /**
     * Initialize is a no op here as we do not need a locking mechanism in case of file system write.
     *
     * @param containerEpoch The Container Epoch to initialize with (ignored here).
     */
    @Override
    public void initialize(long containerEpoch) {
        manager.initialize(containerEpoch);
    }

    @Override
    public CompletableFuture<SegmentHandle> openRead(String streamSegmentName) {
        long traceId = LoggerHelpers.traceEnter(log, "openRead", streamSegmentName);

        return manager.exists(streamSegmentName, null).thenApply(exist -> {
            if (exist) {
                return BookkeeperSegmentHandle.readHandle(streamSegmentName);
            } else {
                throw new CompletionException(new StreamSegmentNotExistsException(streamSegmentName));
            }
        }).exceptionally((Throwable exception) -> {
            throw new CompletionException(new StreamSegmentNotExistsException(streamSegmentName));
        }).thenApply( handle -> {
            LoggerHelpers.traceLeave(log, "openRead", traceId, streamSegmentName);
            return handle;
        });
    }

    @Override
    public CompletableFuture<Integer> read(SegmentHandle handle,
                                           long offset,
                                           byte[] buffer,
                                           int bufferOffset,
                                           int length,
                                           Duration timeout) {
        long traceId = LoggerHelpers.traceEnter(log, "read", handle.getSegmentName());
        if (offset < 0 || bufferOffset < 0 || length < 0) {
            throw new ArrayIndexOutOfBoundsException();
        }
        if (bufferOffset + length > buffer.length) {
            throw new ArrayIndexOutOfBoundsException();
        }
        return manager.read(handle.getSegmentName(), offset, buffer, bufferOffset, length, timeout)
                .thenApply(retVal -> {
                    LoggerHelpers.traceLeave(log, "read", traceId, handle.getSegmentName());
                    return retVal;
                });

    }

    @Override
    public CompletableFuture<SegmentProperties> getStreamSegmentInfo(String streamSegmentName, Duration timeout) {
        long traceId = LoggerHelpers.traceEnter(log, "getStreamSegmentInfo", streamSegmentName);

        return manager.getOrRetrieveStorageLedgerDetails(streamSegmentName, true)
                      .thenApply(segmentProperties -> {
                          LoggerHelpers.traceLeave(log, "getStreamSegmentInfo", traceId, streamSegmentName);
                          return segmentProperties;
                      });
    }

    @Override
    public CompletableFuture<Boolean> exists(String streamSegmentName, Duration timeout) {
        long traceId = LoggerHelpers.traceEnter(log, "exists", streamSegmentName);
       return manager.exists(streamSegmentName, timeout)
                     .thenApply(bool -> {
                         LoggerHelpers.traceLeave(log, "exists", traceId, streamSegmentName);
                         return bool;
                     })
                     .exceptionally(t -> false);
    }

    @Override
    public CompletableFuture<SegmentHandle> openWrite(String streamSegmentName) {
        long traceId = LoggerHelpers.traceEnter(log, "openWrite", streamSegmentName);
        return manager.fence(streamSegmentName)
                      .thenApply(u -> {
                          SegmentHandle retVal = BookkeeperSegmentHandle.writeHandle(streamSegmentName);
                          LoggerHelpers.traceLeave(log, "openWrite", traceId, streamSegmentName);
                          return retVal;
                      });
    }

    @Override
    public CompletableFuture<SegmentProperties> create(String streamSegmentName, Duration timeout) {
        long traceId = LoggerHelpers.traceEnter(log, "create", streamSegmentName);
        return manager.create(streamSegmentName, timeout)
                .thenCompose(str -> this.getStreamSegmentInfo(streamSegmentName, timeout))
                .thenApply(segmentProperties -> {
                    LoggerHelpers.traceLeave(log, "create", traceId, streamSegmentName);
                    return segmentProperties;
                });
    }

    @Override
    public CompletableFuture<Void> write(SegmentHandle handle,
                                         long offset,
                                         InputStream data,
                                         int length,
                                         Duration timeout) {
        Preconditions.checkArgument(!handle.isReadOnly(), "handle must not be read-only.");
        return manager.write(handle.getSegmentName(), offset, data, length);
    }

    @Override
    public CompletableFuture<Void> seal(SegmentHandle handle, Duration timeout) {
        Preconditions.checkArgument(!handle.isReadOnly(), "handle must not be read-only.");
        return manager.seal(handle.getSegmentName());
    }

    @Override
    public CompletableFuture<Void> concat(SegmentHandle targetHandle, long offset, String sourceSegment,
                                          Duration timeout) {
        return manager.concat(targetHandle.getSegmentName(), sourceSegment, offset, timeout)
                .thenCompose(v -> manager.delete(sourceSegment));
    }

    @Override
    public CompletableFuture<Void> delete(SegmentHandle handle, Duration timeout) {
        return manager.delete(handle.getSegmentName());
    }

    //endregion

    //region AutoClosable

    @Override
    public void close() {
        this.closed.set(true);
    }

    //endregion
}
