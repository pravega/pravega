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
import io.pravega.shared.MetricsNames;
import io.pravega.shared.metrics.Counter;
import io.pravega.shared.metrics.MetricsProvider;
import io.pravega.shared.metrics.OpStatsLogger;
import io.pravega.shared.metrics.StatsLogger;

import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Base implementation of {@link ChunkStorageProvider}.
 * It implements common functionality for
 * Delegates to specific implemntations by calling various abstract methods which must be overridden in derived classes.
 */
public abstract class BaseChunkStorageProvider implements ChunkStorageProvider {

    protected static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(BaseChunkStorageProvider.class);

    protected static final StatsLogger STATS_LOGGER = MetricsProvider.createStatsLogger("BaseChunkStorageProvider");


    protected static final OpStatsLogger READ_LATENCY = STATS_LOGGER.createStats(MetricsNames.STORAGE_READ_LATENCY);
    protected static final OpStatsLogger WRITE_LATENCY = STATS_LOGGER.createStats(MetricsNames.STORAGE_WRITE_LATENCY);
    protected static final OpStatsLogger CREATE_LATENCY = STATS_LOGGER.createStats(MetricsNames.STORAGE_CREATE_LATENCY);
    protected static final OpStatsLogger DELETE_LATENCY = STATS_LOGGER.createStats(MetricsNames.STORAGE_DELETE_LATENCY);
    protected static final OpStatsLogger CONCAT_LATENCY = STATS_LOGGER.createStats(MetricsNames.STORAGE_CONCAT_LATENCY);

    protected static final Counter READ_BYTES = STATS_LOGGER.createCounter(MetricsNames.STORAGE_READ_BYTES);
    protected static final Counter WRITE_BYTES = STATS_LOGGER.createCounter(MetricsNames.STORAGE_WRITE_BYTES);
    protected static final Counter CONCAT_BYTES = STATS_LOGGER.createCounter(MetricsNames.STORAGE_CONCAT_BYTES);

    protected static final Counter CREATE_COUNT = STATS_LOGGER.createCounter(MetricsNames.STORAGE_CREATE_COUNT);
    protected static final Counter DELETE_COUNT = STATS_LOGGER.createCounter(MetricsNames.STORAGE_DELETE_COUNT);
    protected static final Counter CONCAT_COUNT = STATS_LOGGER.createCounter(MetricsNames.STORAGE_CONCAT_COUNT);

    protected static final Counter LARGE_CONCAT_COUNT = STATS_LOGGER.createCounter(MetricsNames.STORAGE_LARGE_CONCAT_COUNT);

    protected final AtomicBoolean closed;
    protected final Executor executor;

    /**
     * Constructor.
     * @param executor Executor to use for async operations if any.
     */
    public BaseChunkStorageProvider(Executor executor) {
        this.closed = new AtomicBoolean(false);
        this.executor = Preconditions.checkNotNull(executor);
    }

    /**
     * Gets a value indicating whether this Storage implementation supports truncate operation on underlying storage object.
     *
     * @return True or false.
     */
    @Override
    public boolean supportsTruncation() {
        return false;
    }

    /**
     * Gets a value indicating whether this Storage implementation supports append operation on underlying storage object.
     *
     * @return True or false.
     */
    @Override
    public boolean supportsAppend() {
        return true;
    }

    /**
     * Gets a value indicating whether this Storage implementation supports merge operation on underlying storage object.
     *
     * @return True or false.
     */
    @Override
    public boolean supportsConcat() {
        return false;
    }

    /**
     * Determines whether named file/object exists in underlying storage.
     *
     * @param chunkName Name of the storage object to check.
     * @return True if the object exists, false otherwise.
     */
    @Override
    public boolean exists(String chunkName)  throws IOException {
        // Validate parameters
        Preconditions.checkArgument(null != chunkName, "chunkName must not be null");

        long traceId = LoggerHelpers.traceEnter(log, "exists", chunkName);

        // Call concrete implementation.
        boolean retValue = doesExist(chunkName);

        LoggerHelpers.traceLeave(log, "exists", traceId, chunkName);
        return retValue;
    }


    /**
     * Creates a new file.
     *
     * @param chunkName String name of the storage object to create.
     * @return ChunkHandle A writable handle for the recently created chunk.
     * @throws IOException Throws IOException in case of I/O related exceptions.
     */
    @Override
    public ChunkHandle create(String chunkName) throws IOException {
        // Validate parameters
        Preconditions.checkArgument(null != chunkName, "chunkName must not be null");

        long traceId = LoggerHelpers.traceEnter(log, "create", chunkName);
        Timer timer = new Timer();

        // Call concrete implementation.
        ChunkHandle handle = doCreate(chunkName);

        // Record metrics.
        Duration elapsed = timer.getElapsed();
        CREATE_LATENCY.reportSuccessEvent(elapsed);
        CREATE_COUNT.inc();

        log.debug("Create chunk={} latency={}.", chunkName, elapsed.toMillis());
        LoggerHelpers.traceLeave(log, "create", traceId, chunkName);
        return handle;
    }

    /**
     * Deletes a file.
     *
     * @param handle ChunkHandle of the storage object to delete.
     * @return True if the object was deleted, false otherwise.
     * @throws IOException Throws IOException in case of I/O related exceptions.
     */
    @Override
    public boolean delete(ChunkHandle handle) throws IOException {
        // Validate parameters
        Preconditions.checkArgument(null != handle, "handle must not be null");
        long traceId = LoggerHelpers.traceEnter(log, "delete", handle.getChunkName());
        Timer timer = new Timer();

        // Call concrete implementation.
        boolean retValue = doDelete(handle);

        // Record metrics.
        Duration elapsed = timer.getElapsed();
        DELETE_LATENCY.reportSuccessEvent(elapsed);
        DELETE_COUNT.inc();

        log.debug("Delete chunk={} latency={}.", handle.getChunkName(), elapsed.toMillis());
        LoggerHelpers.traceLeave(log, "delete", traceId, handle.getChunkName());
        return retValue;
    }

    /**
     * Opens storage object for Read.
     *
     * @param chunkName String name of the storage object to read from.
     * @return ChunkHandle A readable handle for the given chunk.
     * @throws IOException Throws IOException in case of I/O related exceptions.
     * @throws IllegalArgumentException If argument is invalid.
     */
    @Override
    public ChunkHandle openRead(String chunkName) throws IOException, IllegalArgumentException {
        // Validate parameters
        Preconditions.checkArgument(null != chunkName, "chunkName must not be null");

        long traceId = LoggerHelpers.traceEnter(log, "openRead", chunkName);

        // Call concrete implementation.
        ChunkHandle handle = doOpenRead(chunkName);

        LoggerHelpers.traceLeave(log, "openRead", traceId, chunkName);
        return handle;
    }


    /**
     * Opens storage object for Write (or modifications).
     *
     * @param chunkName String name of the storage object to write to or modify.
     * @return ChunkHandle A writable handle for the given chunk.
     * @throws IOException Throws IOException in case of I/O related exceptions.
     * @throws IllegalArgumentException If argument is invalid.
     */
    @Override
    public ChunkHandle openWrite(String chunkName) throws IOException, IllegalArgumentException {
        // Validate parameters
        Preconditions.checkArgument(null != chunkName, "chunkName must not be null");

        long traceId = LoggerHelpers.traceEnter(log, "openWrite", chunkName);

        // Call concrete implementation.
        ChunkHandle handle = doOpenWrite(chunkName);

        LoggerHelpers.traceLeave(log, "openWrite", traceId, chunkName);
        return handle;
    }

    /**
     * Retrieves the ChunkInfo for given name.
     *
     * @param chunkName String name of the storage object to read from.
     * @return ChunkInfo Information about the given chunk.
     * @throws IOException Throws IOException in case of I/O related exceptions.
     * @throws IllegalArgumentException If argument is invalid.
     */
    @Override
    public ChunkInfo getInfo(String chunkName) throws IOException, IllegalArgumentException {
        // Validate parameters
        Preconditions.checkNotNull(chunkName);
        long traceId = LoggerHelpers.traceEnter(log, "getInfo", chunkName);

        // Call concrete implementation.
        ChunkInfo info = doGetInfo(chunkName);

        LoggerHelpers.traceLeave(log, "getInfo", traceId, chunkName);
        return info;
    }

    /**
     * Reads a range of bytes from the underlying storage object.
     *
     * @param handle       ChunkHandle of the storage object to read from.
     * @param fromOffset   Offset in the file from which to start reading.
     * @param length       Number of bytes to read.
     * @param buffer       Byte buffer to which data is copied.
     * @param bufferOffset Offset in the buffer at which to start copying read data.
     * @return int Number of bytes read.
     * @throws IOException Throws IOException in case of I/O related exceptions.
     * @throws IllegalArgumentException If argument is invalid.
     * @throws IndexOutOfBoundsException If the index is out of bounds.
     */
    @Override
    public int read(ChunkHandle handle, long fromOffset, int length, byte[] buffer, int bufferOffset) throws IOException, NullPointerException, IndexOutOfBoundsException {
        // Validate parameters
        Preconditions.checkArgument(null != handle, "handle");
        Preconditions.checkArgument(null != buffer, "buffer");
        Preconditions.checkArgument(fromOffset >= 0, "fromOffset must be non-negative");
        Preconditions.checkArgument(length >= 0 && length <= buffer.length, "length");
        Preconditions.checkElementIndex(bufferOffset, buffer.length, "bufferOffset");

        long traceId = LoggerHelpers.traceEnter(log, "read", handle.getChunkName(), fromOffset, bufferOffset, length);
        Timer timer = new Timer();

        // Call concrete implementation.
        int bytesRead = doRead(handle, fromOffset, length, buffer, bufferOffset);

        Duration elapsed = timer.getElapsed();
        READ_LATENCY.reportSuccessEvent(elapsed);
        READ_BYTES.add(bytesRead);

        log.debug("Read chunk={} offset={} bytesWritten={} latency={}.", handle.getChunkName(), fromOffset, length, elapsed.toMillis());
        LoggerHelpers.traceLeave(log, "read", traceId, bytesRead);
        return bytesRead;
    }

    /**
     * Writes the given data to the underlying storage object.
     *
     * @param handle ChunkHandle of the storage object to write to.
     * @param offset Offset in the file to start writing.
     * @param length Number of bytes to write.
     * @param data   An InputStream representing the data to write.
     * @return int Number of bytes written.
     * @throws IOException Throws IOException in case of I/O related exceptions.
     */
    @Override
    public int write(ChunkHandle handle, long offset, int length, InputStream data) throws IOException {
        // Validate parameters
        Preconditions.checkArgument(null != handle, "handle must not be null");
        Preconditions.checkArgument(null != data, "data must not be null");
        Preconditions.checkArgument(offset >= 0, "offset must be non-negative");
        Preconditions.checkArgument(length >= 0, "length must be non-negative");

        long traceId = LoggerHelpers.traceEnter(log, "write", handle.getChunkName(), offset, length);
        Timer timer = new Timer();

        // Call concrete implementation.
        int bytesWritten = doWrite(handle, offset, length, data);

        Duration elapsed = timer.getElapsed();

        WRITE_LATENCY.reportSuccessEvent(elapsed);
        WRITE_BYTES.add(bytesWritten);

        log.debug("Write chunk={} offset={} bytesWritten={} latency={}.", handle.getChunkName(), offset, length, elapsed.toMillis());
        LoggerHelpers.traceLeave(log, "read", traceId, bytesWritten);
        return bytesWritten;
    }

    /**
     * Concatenates two or more chunks.
     *
     * @param target  String Name of the storage object to concat to.
     * @param sources Array of ChunkHandle to existing chunks to be appended to the target. The chunks are appended in the same sequence the names are provided.
     * @return int Number of bytes concatenated.
     * @throws IOException Throws IOException in case of I/O related exceptions.
     * @throws UnsupportedOperationException If this operation is not supported by this provider.
     */
    @Override
    public int concat(ChunkHandle target, ChunkHandle... sources) throws IOException, UnsupportedOperationException {
        // Validate parameters
        Preconditions.checkArgument(null != target, "target must not be null");
        Preconditions.checkArgument(null != sources, "sources must not be null");

        long traceId = LoggerHelpers.traceEnter(log, "concat", target.getChunkName());
        Timer timer = new Timer();

        // Call concrete implementation.
        int retValue = doConcat(target, sources);

        Duration elapsed = timer.getElapsed();
        log.debug("Concat target={} latency={}.", target.getChunkName(), elapsed.toMillis());

        CONCAT_LATENCY.reportSuccessEvent(elapsed);
        CONCAT_BYTES.add(0);
        CONCAT_COUNT.inc();

        LoggerHelpers.traceLeave(log, "concat", traceId, target.getChunkName());
        return retValue;
    }

    /**
     * Truncates a given chunk.
     *
     * @param handle ChunkHandle of the storage object to truncate.
     * @param offset Offset to truncate to.
     * @return True if the object was truncated, false otherwise.
     * @throws IOException Throws IOException in case of I/O related exceptions.
     * @throws UnsupportedOperationException If this operation is not supported by this provider.
     */
    @Override
    public boolean truncate(ChunkHandle handle, long offset) throws IOException, UnsupportedOperationException {
        // Validate parameters
        Preconditions.checkArgument(null != handle, "handle must not be null");

        long traceId = LoggerHelpers.traceEnter(log, "truncate", handle.getChunkName());

        // Call concrete implementation.
        boolean retValue = doTruncate(handle, offset);

        LoggerHelpers.traceLeave(log, "truncate", traceId, handle.getChunkName());
        return retValue;
    }

    /**
     * Sets readonly attribute for the chunk.
     *
     * @param handle ChunkHandle of the storage object.
     * @param isReadonly True if chunk is set to be readonly.
     * @return True if the operation was successful, false otherwise.
     * @throws IOException Throws IOException in case of I/O related exceptions.
     * @throws UnsupportedOperationException If this operation is not supported by this provider.
     */
    @Override
    public boolean setReadonly(ChunkHandle handle, boolean isReadonly) throws IOException, UnsupportedOperationException {
        // Validate parameters
        Preconditions.checkArgument(null != handle, "handle must not be null");

        long traceId = LoggerHelpers.traceEnter(log, "setReadonly", handle.getChunkName());

        // Call concrete implementation.
        boolean retValue = doSetReadonly(handle, isReadonly);

        LoggerHelpers.traceLeave(log, "setReadonly", traceId, handle.getChunkName());
        return retValue;
    }

    /**
     * Closes.
     * @throws Exception
     */
    @Override
    public void close() throws Exception {
        this.closed.set(true);
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

    abstract protected ChunkInfo doGetInfo(String chunkName) throws IOException, IllegalArgumentException;

    abstract protected ChunkHandle doCreate(String chunkName) throws IOException, IllegalArgumentException;

    abstract protected boolean doesExist(String chunkName) throws IOException, IllegalArgumentException;

    abstract protected boolean doDelete(ChunkHandle handle) throws IOException, IllegalArgumentException;

    abstract protected ChunkHandle doOpenRead(String chunkName) throws IOException, IllegalArgumentException;

    abstract protected ChunkHandle doOpenWrite(String chunkName) throws IOException, IllegalArgumentException;

    abstract protected int doRead(ChunkHandle handle, long fromOffset, int length, byte[] buffer, int bufferOffset) throws IOException, NullPointerException, IndexOutOfBoundsException;

    abstract protected int  doWrite(ChunkHandle handle, long offset, int length, InputStream data) throws IOException, IndexOutOfBoundsException;

    abstract protected int doConcat(ChunkHandle target, ChunkHandle... sources) throws IOException, UnsupportedOperationException;

    abstract protected boolean doTruncate(ChunkHandle handle, long offset) throws IOException, UnsupportedOperationException;

    abstract protected boolean doSetReadonly(ChunkHandle handle, boolean isReadOnly) throws IOException, UnsupportedOperationException;
}
