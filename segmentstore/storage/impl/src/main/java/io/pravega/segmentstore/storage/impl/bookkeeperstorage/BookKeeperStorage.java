/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.storage.impl.bookkeeperstorage;

import com.google.common.base.Preconditions;
import io.pravega.common.Exceptions;
import io.pravega.common.LoggerHelpers;
import io.pravega.common.Timer;
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
 * This log is called a LogStorage. A storage ledger consists of a number of ledgers and metadata for the segment and the ledgers.
 * The metadata is stored in ZK. It is accessed using the async curator framework.
 *
 * Fencing: The recommended implementation of fencing as described in the URL ensures that the latest caller owns the log.
 * In case of Storage implementation the requirement is different. A Storage with higher epoch is supposed to own the log,
 * irrespective of the time the fencing happens. Because of this a CAS operation in ZK for a storage ledger followed by fencing decides ownership.
 *
 * Here is the algorithm that describes ownership change through the openWrite() call:
 *
 * 1. Check the current epoch for the given LogStorage.
 * 2. If the current epoch is larger, set it to the new epoc, otherwise throw StorageNotPrimaryException.
 * 3. Try and fence all the ledgers. The last ledger may be empty, in this case delete the ledger.
 * 4. Create a new ledger and add it to the list of ledgers as well as to the ZK conditionally.
 *    We ensure that only one such action is successful. In case such a ledger already exists, the call fails.
 *
 * Concat: Apache bookkeeper does not have a native concat which appends two ledgers on the server side. To overcome this,
 * concatenation is implemented as a pure metadata operation. Here is the algorithm used for concat() call:
 *
 * 1. Get the target and source ledgers.
 * 2. Update the offset of the source ledgers to the offsets after the concatenation.
 * 3. Update all the metadata about ledgers in one transaction using transaction() API of curator.
 *    These operations are done conditionally so that only one operation succeeds.
 */
@Slf4j
class BookKeeperStorage implements Storage {

    //region members

    private final BookKeeperStorageConfig config;
    private final ExecutorService executor;
    private final AtomicBoolean closed;
    private final LogStorageManager manager;
    private final CuratorFramework zkClient;
    private boolean initialized;

    //endregion

    //region constructor

    /**
     * Creates a new instance of the BookKeeperStorage class.
     *
     * @param config   The configuration to use.
     * @param zkClient Curator framework to interact with ZK.
     * @param executor The executor to use for running async operations.
     */
    public BookKeeperStorage(BookKeeperStorageConfig config, CuratorFramework zkClient, ExecutorService executor) {
        Preconditions.checkNotNull(config, "config");
        Preconditions.checkNotNull(executor, "executor");
        Preconditions.checkNotNull(zkClient, "zkClient");
        this.closed = new AtomicBoolean(false);
        this.config = config;
        this.zkClient = zkClient;
        this.executor = executor;
        manager = new LogStorageManager(config, zkClient, executor);
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
        this.initialized = true;
    }

    @Override
    public CompletableFuture<SegmentHandle> openRead(String streamSegmentName) {
        long traceId = LoggerHelpers.traceEnter(log, "openRead", streamSegmentName);
        ensureInitializedAndNotClosed();

        return manager.exists(streamSegmentName, null).thenApply(exist -> {
            if (exist) {
                return BookKeeperSegmentHandle.readHandle(streamSegmentName);
            } else {
                throw new CompletionException(new StreamSegmentNotExistsException(streamSegmentName));
            }
        })
        .thenApply( handle -> {
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
        ensureInitializedAndNotClosed();
        Timer timer = new Timer();

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
                })
                .thenApply(lengthRead -> {
                    BookKeeperStorageMetrics.READ_LATENCY.reportSuccessEvent(timer.getElapsed());
                    BookKeeperStorageMetrics.READ_BYTES.add(lengthRead);
                    return lengthRead;
                });
    }

    @Override
    public CompletableFuture<SegmentProperties> getStreamSegmentInfo(String streamSegmentName, Duration timeout) {
        long traceId = LoggerHelpers.traceEnter(log, "getStreamSegmentInfo", streamSegmentName);
        ensureInitializedAndNotClosed();

        return manager.getOrRetrieveStorageLedgerDetails(streamSegmentName, true)
                      .thenApply(segmentProperties -> {
                          LoggerHelpers.traceLeave(log, "getStreamSegmentInfo", traceId, streamSegmentName);
                          return segmentProperties;
                      });
    }

    @Override
    public CompletableFuture<Boolean> exists(String streamSegmentName, Duration timeout) {
        long traceId = LoggerHelpers.traceEnter(log, "exists", streamSegmentName);
        ensureInitializedAndNotClosed();

       return manager.exists(streamSegmentName, timeout)
                     .thenApply(bool -> {
                         LoggerHelpers.traceLeave(log, "exists", traceId, streamSegmentName);
                         return bool;
                     });
    }

    @Override
    public CompletableFuture<SegmentHandle> openWrite(String streamSegmentName) {
        long traceId = LoggerHelpers.traceEnter(log, "openWrite", streamSegmentName);
        ensureInitializedAndNotClosed();

        return manager.fence(streamSegmentName)
                      .thenApply(u -> {
                          SegmentHandle retVal = BookKeeperSegmentHandle.writeHandle(streamSegmentName);
                          LoggerHelpers.traceLeave(log, "openWrite", traceId, streamSegmentName);
                          return retVal;
                      });
    }

    @Override
    public CompletableFuture<SegmentProperties> create(String streamSegmentName, Duration timeout) {
        long traceId = LoggerHelpers.traceEnter(log, "create", streamSegmentName);
        ensureInitializedAndNotClosed();

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
        ensureInitializedAndNotClosed();

        Timer timer = new Timer();
        Preconditions.checkArgument(!handle.isReadOnly(), "handle must not be read-only.");
        Preconditions.checkArgument(handle instanceof BookKeeperSegmentHandle, "handle must be instance of bookkeeper segment handle.");
        return manager.write(handle.getSegmentName(), offset, data, length)
                      .thenAccept(lengthWritten -> {
                          BookKeeperStorageMetrics.WRITE_LATENCY.reportSuccessEvent(timer.getElapsed());
                          BookKeeperStorageMetrics.WRITE_BYTES.add(lengthWritten);
                      });
    }

    @Override
    public CompletableFuture<Void> seal(SegmentHandle handle, Duration timeout) {
        Preconditions.checkArgument(!handle.isReadOnly(), "handle must not be read-only.");
        ensureInitializedAndNotClosed();

        return manager.seal(handle.getSegmentName());
    }

    @Override
    public CompletableFuture<Void> concat(SegmentHandle targetHandle, long offset, String sourceSegment,
                                          Duration timeout) {
        ensureInitializedAndNotClosed();

        return manager.concat(targetHandle.getSegmentName(), sourceSegment, offset, timeout);
    }

    @Override
    public CompletableFuture<Void> delete(SegmentHandle handle, Duration timeout) {
        ensureInitializedAndNotClosed();

        return manager.delete(handle.getSegmentName());
    }

    //endregion

    //region AutoClosable

    @Override
    public void close() {
        this.closed.set(true);
    }

    private void ensureInitializedAndNotClosed() {
        Exceptions.checkNotClosed(this.closed.get(), this);
        Preconditions.checkState(this.initialized, "BookKeeperStorage is not initialized.");
    }
    //endregion
}
