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
import io.pravega.segmentstore.contracts.StreamSegmentException;
import io.pravega.segmentstore.contracts.StreamSegmentInformation;
import io.pravega.segmentstore.storage.SegmentHandle;
import io.pravega.segmentstore.storage.SyncStorage;
import java.io.InputStream;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;

/**
 * Storage adapter for Apache BookKeeper based storage.
 *
 * Each segment is represented by a log built on top of BookKeeper ledgers. This implementation follows some recommendations
 * here: https://bookkeeper.apache.org/docs/4.5.0/api/ledger-api/
 * This log is called a LogStorage. A LogStorage consists of a number of ledgers and metadata for the segment and the ledgers.
 * The metadata is stored in ZK. It is accessed using the curator framework.
 *
 * Fencing: The recommended implementation of fencing as described in the URL ensures that the latest caller owns the log.
 * In case of Storage implementation the requirement is different. A Storage with higher epoch is supposed to own the log,
 * irrespective of the time the fencing happens. Because of this a CAS operation in ZK for a storage ledger followed by fencing decides ownership.
 *
 * Here is the algorithm that describes ownership change through the openWrite() call:
 *
 * 1. Check the current epoch for the given LogStorage.
 * 2. If the current epoch is larger, set it to the new epoch, otherwise throw StorageNotPrimaryException.
 * 3. Try and fence all the ledgers. The last ledger may be empty, in this case delete the ledger.
 * 4. Create a new ledger and add it to the list of ledgers as well as to the ZK conditionally.
 *    We ensure that only one such action is successful. In case such a ledger already exists, the call fails.
 *
 * Concat: Apache BookKeeper does not have a native concat which appends two ledgers on the server side. To overcome this,
 * concatenation is implemented as a pure metadata operation. Here is the algorithm used for concat() call:
 *
 * 1. Get the target and source ledgers.
 * 2. Update the offset of the source ledgers to the offsets after the concatenation.
 * 3. Update all the metadata about ledgers in one transaction using transaction() API of curator.
 *    These operations are done conditionally so that only one operation succeeds.
 */
@Slf4j
class BookKeeperStorage implements SyncStorage {

    //region members

    private final BookKeeperStorageConfig config;
    private final AtomicBoolean closed;
    private final LogStorageManager manager;
    private final AtomicBoolean initialized;

    //endregion

    //region constructor

    /**
     * Creates a new instance of the BookKeeperStorage class.
     *
     * @param config   The configuration to use.
     * @param zkClient Curator framework to interact with ZK.
     */
    public BookKeeperStorage(BookKeeperStorageConfig config, CuratorFramework zkClient) {
        Preconditions.checkNotNull(config, "config");
        Preconditions.checkNotNull(zkClient, "zkClient");
        this.closed = new AtomicBoolean(false);
        this.config = config;
        manager = new LogStorageManager(config, zkClient);
        initialized = new AtomicBoolean(false);
    }

    //endregion

    //region SyncStorage implementation

    /**
     * Initialize the bookkeeper based storage.
     *
     * @param containerEpoch The Container Epoch to initialize with (ignored here).
     */
    @Override
    public void initialize(long containerEpoch) {
        manager.initialize(containerEpoch);
        this.initialized.set(true);
    }

    @Override
    public SegmentHandle openRead(String streamSegmentName) throws StreamSegmentException {
        long traceId = LoggerHelpers.traceEnter(log, "openRead", streamSegmentName);

        // BookKeeper does not support read only open well. Need to own the segment.
        openWrite(streamSegmentName);

        return BookKeeperSegmentHandle.readHandle(streamSegmentName);
    }

    @Override
    public int read(SegmentHandle handle, long offset, byte[] buffer, int bufferOffset, int length) throws StreamSegmentException {
        long traceId = LoggerHelpers.traceEnter(log, "read", handle.getSegmentName());
        ensureInitializedAndNotClosed();
        Timer timer = new Timer();

        if (offset < 0 || bufferOffset < 0 || length < 0) {
            throw new ArrayIndexOutOfBoundsException();
        }
        if (bufferOffset + length > buffer.length) {
            throw new ArrayIndexOutOfBoundsException();
        }
        int retVal = manager.read(handle.getSegmentName(), offset, buffer, bufferOffset, length);
        BookKeeperStorageMetrics.READ_LATENCY.reportSuccessEvent(timer.getElapsed());
        BookKeeperStorageMetrics.READ_BYTES.add(retVal);
        LoggerHelpers.traceLeave(log, "read", traceId, handle.getSegmentName());
        return retVal;
    }

    @Override
    public SegmentProperties getStreamSegmentInfo(String streamSegmentName) throws StreamSegmentException {
        long traceId = LoggerHelpers.traceEnter(log, "getStreamSegmentInfo", streamSegmentName);
        ensureInitializedAndNotClosed();

        SegmentProperties segmentProperties = manager.getOrRetrieveStorageLedgerDetails(streamSegmentName);
        LoggerHelpers.traceLeave(log, "getStreamSegmentInfo", traceId, streamSegmentName);
        return segmentProperties;
    }

    @Override
    public boolean exists(String streamSegmentName) {
        long traceId = LoggerHelpers.traceEnter(log, "exists", streamSegmentName);
        ensureInitializedAndNotClosed();

        boolean bool = manager.exists(streamSegmentName);
        LoggerHelpers.traceLeave(log, "exists", traceId, streamSegmentName);
        return bool;
    }

    @Override
    public SegmentHandle openWrite(String streamSegmentName) throws StreamSegmentException {
        long traceId = LoggerHelpers.traceEnter(log, "openWrite", streamSegmentName);
        ensureInitializedAndNotClosed();

        manager.fence(streamSegmentName);
        SegmentHandle retVal = BookKeeperSegmentHandle.writeHandle(streamSegmentName);
        LoggerHelpers.traceLeave(log, "openWrite", traceId, streamSegmentName);
        return retVal;
    }

    @Override
    public SegmentProperties create(String streamSegmentName) throws StreamSegmentException {
        long traceId = LoggerHelpers.traceEnter(log, "create", streamSegmentName);
        ensureInitializedAndNotClosed();

        manager.create(streamSegmentName);
        StreamSegmentInformation segmentProperties = StreamSegmentInformation.builder()
                                                                             .name(streamSegmentName)
                                                                             .build();
        LoggerHelpers.traceLeave(log, "create", traceId, streamSegmentName);
        return segmentProperties;
    }

    @Override
    public void write(SegmentHandle handle, long offset, InputStream data, int length) throws StreamSegmentException {
        ensureInitializedAndNotClosed();

        Timer timer = new Timer();
        Preconditions.checkArgument(!handle.isReadOnly(), "handle must not be read-only.");
        Preconditions.checkArgument(handle instanceof BookKeeperSegmentHandle, "handle must be instance of BookKeeper segment handle.");
        int lengthWritten = manager.write(handle.getSegmentName(), offset, data, length);
        BookKeeperStorageMetrics.WRITE_LATENCY.reportSuccessEvent(timer.getElapsed());
        BookKeeperStorageMetrics.WRITE_BYTES.add(lengthWritten);
    }

    @Override
    public void seal(SegmentHandle handle) throws StreamSegmentException {
        Preconditions.checkArgument(!handle.isReadOnly(), "handle must not be read-only.");
        ensureInitializedAndNotClosed();

        manager.seal(handle.getSegmentName());
    }

    @Override
    public void unseal(SegmentHandle handle) {
        throw new UnsupportedOperationException("Unseal is not implemented for BookKeeperStorage");
    }

    @Override
    public void concat(SegmentHandle targetHandle, long offset, String sourceSegment) throws StreamSegmentException {
        ensureInitializedAndNotClosed();
        manager.concat(targetHandle.getSegmentName(), sourceSegment, offset);
    }

    @Override
    public void truncate(SegmentHandle handle, long offset) throws StreamSegmentException {
        throw new UnsupportedOperationException("Truncate is not implemented for BookKeeperStorage");
    }

    @Override
    public boolean supportsTruncation() {
        return false;
    }

    @Override
    public void delete(SegmentHandle handle) throws StreamSegmentException {
        ensureInitializedAndNotClosed();
        manager.delete(handle.getSegmentName());
    }

    //endregion

    //region AutoClosable

    @Override
    public void close() {
        this.closed.set(true);
    }

    private void ensureInitializedAndNotClosed() {
        Exceptions.checkNotClosed(this.closed.get(), this);
        Preconditions.checkState(this.initialized.get(), "BookKeeperStorage is not initialized.");
    }
    //endregion
}