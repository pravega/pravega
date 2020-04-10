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
import lombok.extern.slf4j.Slf4j;

import java.io.InputStream;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Base implementation of {@link ChunkStorageProvider}.
 * It implements common functionality that can be used by derived classes.
 * Delegates to specific implementations by calling various abstract methods which must be overridden in derived classes.
 * Detailed design is documented here https://github.com/pravega/pravega/wiki/PDP-34:-Simplified-Tier-2
 */
@Slf4j
public abstract class BaseChunkStorageProvider implements ChunkStorageProvider {

    private final AtomicBoolean closed;

    /**
     * Constructor.
     *
     */
    public BaseChunkStorageProvider() {
        this.closed = new AtomicBoolean(false);
    }

    /**
     * Gets a value indicating whether this Storage implementation supports truncate operation on chunks.
     *
     * @return True or false.
     */
    @Override
    abstract public boolean supportsTruncation();

    /**
     * Gets a value indicating whether this Storage implementation supports append operation on chunks.
     *
     * @return True or false.
     */
    @Override
    abstract public boolean supportsAppend();

    /**
     * Gets a value indicating whether this Storage implementation supports merge operation either natively or through appends.
     *
     * @return True or false.
     */
    @Override
    abstract public boolean supportsConcat();

    /**
     * Determines whether named file/object exists in underlying storage.
     *
     * @param chunkName Name of the chunk to check.
     * @return True if the object exists, false otherwise.
     */
    @Override
    final public boolean exists(String chunkName) throws ChunkStorageException {
        Exceptions.checkNotClosed(this.closed.get(), this);
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
     * @param chunkName Name of the chunk to create.
     * @return ChunkHandle A writable handle for the recently created chunk.
     * @throws ChunkStorageException Throws ChunkStorageException in case of I/O related exceptions.
     *
     */
    @Override
    final public ChunkHandle create(String chunkName) throws ChunkStorageException {
        Exceptions.checkNotClosed(this.closed.get(), this);
        // Validate parameters
        Preconditions.checkArgument(null != chunkName, "chunkName must not be null");

        long traceId = LoggerHelpers.traceEnter(log, "create", chunkName);
        Timer timer = new Timer();

        // Call concrete implementation.
        ChunkHandle handle = doCreate(chunkName);

        // Record metrics.
        Duration elapsed = timer.getElapsed();
        ChunkStorageProviderMetrics.CREATE_LATENCY.reportSuccessEvent(elapsed);
        ChunkStorageProviderMetrics.CREATE_COUNT.inc();

        log.debug("Create - chunk={}, latency={}.", chunkName, elapsed.toMillis());
        LoggerHelpers.traceLeave(log, "create", traceId, chunkName);

        return handle;
    }

    /**
     * Deletes a chunk.
     *
     * @param handle ChunkHandle of the chunk to delete.
     * @throws ChunkStorageException Throws ChunkStorageException in case of I/O related exceptions.
     */
    @Override
    final public void delete(ChunkHandle handle) throws ChunkStorageException {
        Exceptions.checkNotClosed(this.closed.get(), this);
        // Validate parameters
        Preconditions.checkArgument(null != handle, "handle must not be null");
        Preconditions.checkArgument(!handle.isReadOnly(), "handle must not be readonly");
        long traceId = LoggerHelpers.traceEnter(log, "delete", handle.getChunkName());
        Timer timer = new Timer();

        // Call concrete implementation.
        doDelete(handle);

        // Record metrics.
        Duration elapsed = timer.getElapsed();
        ChunkStorageProviderMetrics.DELETE_LATENCY.reportSuccessEvent(elapsed);
        ChunkStorageProviderMetrics.DELETE_COUNT.inc();

        log.debug("Delete - chunk={}, latency={}.", handle.getChunkName(), elapsed.toMillis());
        LoggerHelpers.traceLeave(log, "delete", traceId, handle.getChunkName());

    }

    /**
     * Opens chunk for Read.
     *
     * @param chunkName String name of the chunk to read from.
     * @return ChunkHandle A readable handle for the given chunk.
     * @throws ChunkStorageException Throws ChunkStorageException in case of I/O related exceptions.
     * @throws IllegalArgumentException If argument is invalid.
     */
    @Override
    final public ChunkHandle openRead(String chunkName) throws ChunkStorageException, IllegalArgumentException {
        Exceptions.checkNotClosed(this.closed.get(), this);
        // Validate parameters
        Preconditions.checkArgument(null != chunkName, "chunkName must not be null");

        long traceId = LoggerHelpers.traceEnter(log, "openRead", chunkName);

        // Call concrete implementation.
        ChunkHandle handle = doOpenRead(chunkName);

        LoggerHelpers.traceLeave(log, "openRead", traceId, chunkName);

        return handle;
    }

    /**
     * Opens chunk for Write (or modifications).
     *
     * @param chunkName String name of the chunk to write to or modify.
     * @return ChunkHandle A writable handle for the given chunk.
     * @throws ChunkStorageException Throws ChunkStorageException in case of I/O related exceptions.
     * @throws IllegalArgumentException If argument is invalid.
     */
    @Override
    final public ChunkHandle openWrite(String chunkName) throws ChunkStorageException, IllegalArgumentException {
        Exceptions.checkNotClosed(this.closed.get(), this);
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
     * @param chunkName String name of the chunk to read from.
     * @return ChunkInfo Information about the given chunk.
     * @throws ChunkStorageException Throws ChunkStorageException in case of I/O related exceptions.
     * @throws IllegalArgumentException If argument is invalid.
     */
    @Override
    final public ChunkInfo getInfo(String chunkName) throws ChunkStorageException, IllegalArgumentException {
        Exceptions.checkNotClosed(this.closed.get(), this);
        // Validate parameters
        Preconditions.checkNotNull(chunkName);
        long traceId = LoggerHelpers.traceEnter(log, "getInfo", chunkName);

        // Call concrete implementation.
        ChunkInfo info = doGetInfo(chunkName);

        LoggerHelpers.traceLeave(log, "getInfo", traceId, chunkName);

        return info;
    }

    /**
     * Reads a range of bytes from the underlying chunk.
     *
     * @param handle       ChunkHandle of the chunk to read from.
     * @param fromOffset   Offset in the chunk from which to start reading.
     * @param length       Number of bytes to read.
     * @param buffer       Byte buffer to which data is copied.
     * @param bufferOffset Offset in the buffer at which to start copying read data.
     * @return int Number of bytes read.
     * @throws ChunkStorageException Throws ChunkStorageException in case of I/O related exceptions.
     * @throws IllegalArgumentException If argument is invalid.
     * @throws IndexOutOfBoundsException If the index is out of bounds.
     */
    @Override
    final public int read(ChunkHandle handle, long fromOffset, int length, byte[] buffer, int bufferOffset) throws ChunkStorageException, NullPointerException, IndexOutOfBoundsException {
        Exceptions.checkNotClosed(this.closed.get(), this);
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
        ChunkStorageProviderMetrics.READ_LATENCY.reportSuccessEvent(elapsed);
        ChunkStorageProviderMetrics.READ_BYTES.add(bytesRead);

        log.debug("Read - chunk={}, offset={}, bytesRead={}, latency={}.", handle.getChunkName(), fromOffset, length, elapsed.toMillis());
        LoggerHelpers.traceLeave(log, "read", traceId, bytesRead);

        return bytesRead;
    }

    /**
     * Writes the given data to the underlying chunk.
     *
     * @param handle ChunkHandle of the chunk to write to.
     * @param offset Offset in the chunk to start writing.
     * @param length Number of bytes to write.
     * @param data   An InputStream representing the data to write.
     * @return int Number of bytes written.
     * @throws ChunkStorageException Throws ChunkStorageException in case of I/O related exceptions.
     */
    @Override
    final public int write(ChunkHandle handle, long offset, int length, InputStream data) throws ChunkStorageException {
        Exceptions.checkNotClosed(this.closed.get(), this);
        // Validate parameters
        Preconditions.checkArgument(null != handle, "handle must not be null");
        Preconditions.checkArgument(!handle.isReadOnly(), "handle must not be readonly");
        Preconditions.checkArgument(null != data, "data must not be null");
        Preconditions.checkArgument(offset >= 0, "offset must be non-negative");
        Preconditions.checkArgument(length >= 0, "length must be non-negative");

        long traceId = LoggerHelpers.traceEnter(log, "write", handle.getChunkName(), offset, length);
        Timer timer = new Timer();

        // Call concrete implementation.
        int bytesWritten = doWrite(handle, offset, length, data);

        Duration elapsed = timer.getElapsed();

        ChunkStorageProviderMetrics.WRITE_LATENCY.reportSuccessEvent(elapsed);
        ChunkStorageProviderMetrics.WRITE_BYTES.add(bytesWritten);

        log.debug("Write - chunk={}, offset={}, bytesWritten={}, latency={}.", handle.getChunkName(), offset, length, elapsed.toMillis());
        LoggerHelpers.traceLeave(log, "read", traceId, bytesWritten);

        return bytesWritten;
    }

    /**
     * Concatenates two or more chunks. The first chunk is concatenated to.
     *
     * @param chunks Array of ConcatArgument objects containing info about existing chunks to be concatenated together.
     *               The chunks must be concatenated in the same sequence the arguments are provided.
     * @return int Number of bytes concatenated.
     * @throws ChunkStorageException Throws ChunkStorageException in case of I/O related exceptions.
     * @throws UnsupportedOperationException If this operation is not supported by this provider.
     */
    @Override
    final public int concat(ConcatArgument[] chunks) throws ChunkStorageException, UnsupportedOperationException {
        Exceptions.checkNotClosed(this.closed.get(), this);
        checkConcatArgs(chunks);

        long traceId = LoggerHelpers.traceEnter(log, "concat", chunks[0].getName());
        Timer timer = new Timer();

        // Call concrete implementation.
        int retValue = doConcat(chunks);

        Duration elapsed = timer.getElapsed();
        log.debug("concat - target={}, latency={}.", chunks[0].getName(), elapsed.toMillis());

        ChunkStorageProviderMetrics.CONCAT_LATENCY.reportSuccessEvent(elapsed);
        ChunkStorageProviderMetrics.CONCAT_BYTES.add(retValue);
        ChunkStorageProviderMetrics.CONCAT_COUNT.inc();
        ChunkStorageProviderMetrics.LARGE_CONCAT_COUNT.inc();

        LoggerHelpers.traceLeave(log, "concat", traceId, chunks[0].getName());

        return retValue;
    }

    private void checkConcatArgs(ConcatArgument[] chunks) {
        // Validate parameters
        Preconditions.checkArgument(null != chunks, "chunks must not be null");
        Preconditions.checkArgument(chunks.length >= 2, "There must be at least two chunks");

        Preconditions.checkArgument(null != chunks[0], "target chunk must not be null");
        Preconditions.checkArgument(chunks[0].getLength() >= 0, "target chunk lenth must be non negative.");

        for (int i = 1; i < chunks.length; i++) {
            Preconditions.checkArgument(null != chunks[i], "source chunk must not be null");
            Preconditions.checkArgument(chunks[i].getLength() >= 0, "source chunk lenth must be non negative.");
            Preconditions.checkArgument(!chunks[i].getName().equals(chunks[0].getName()), "source chunk is same as target");
            Preconditions.checkArgument(!chunks[i].getName().equals(chunks[i - 1].getName()), "duplicate chunk found");
        }
    }

    /**
     * Truncates a given chunk.
     *
     * @param handle ChunkHandle of the chunk to truncate.
     * @param offset Offset to truncate to.
     * @return True if the object was truncated, false otherwise.
     * @throws ChunkStorageException Throws ChunkStorageException in case of I/O related exceptions.
     * @throws UnsupportedOperationException If this operation is not supported by this provider.
     */
    @Override
    final public boolean truncate(ChunkHandle handle, long offset) throws ChunkStorageException, UnsupportedOperationException {
        Exceptions.checkNotClosed(this.closed.get(), this);
        // Validate parameters
        Preconditions.checkArgument(null != handle, "handle must not be null");
        Preconditions.checkArgument(!handle.isReadOnly(), "handle must not be readonly");
        Preconditions.checkArgument(offset > 0, "handle must not be readonly");

        long traceId = LoggerHelpers.traceEnter(log, "truncate", handle.getChunkName());

        // Call concrete implementation.
        boolean retValue = doTruncate(handle, offset);

        LoggerHelpers.traceLeave(log, "truncate", traceId, handle.getChunkName());

        return retValue;
    }

    /**
     * Sets readonly attribute for the chunk.
     *
     * @param handle ChunkHandle of the chunk.
     * @param isReadonly True if chunk is set to be readonly.
     * @return True if the operation was successful, false otherwise.
     * @throws ChunkStorageException Throws ChunkStorageException in case of I/O related exceptions.
     * @throws UnsupportedOperationException If this operation is not supported by this provider.
     */
    @Override
    final public boolean setReadOnly(ChunkHandle handle, boolean isReadonly) throws ChunkStorageException, UnsupportedOperationException {
        Exceptions.checkNotClosed(this.closed.get(), this);
        // Validate parameters
        Preconditions.checkArgument(null != handle, "handle must not be null");

        long traceId = LoggerHelpers.traceEnter(log, "setReadOnly", handle.getChunkName());

        // Call concrete implementation.
        boolean retValue = doSetReadOnly(handle, isReadonly);

        LoggerHelpers.traceLeave(log, "setReadOnly", traceId, handle.getChunkName());

        return retValue;
    }

    /**
     * Closes.
     * @throws Exception In case of any error.
     */
    @Override
    public void close() throws Exception {
        this.closed.set(true);
    }

    /**
     * Checks whether this instance is closed or not.
     * @return True if this instance is closed, false otherwise.
     */
    protected boolean isClosed() {
        return this.closed.get();
    }

    /**
     * Retrieves the ChunkInfo for given name.
     *
     * @param chunkName String name of the chunk to read from.
     * @return ChunkInfo Information about the given chunk.
     * @throws ChunkStorageException Throws ChunkStorageException in case of I/O related exceptions.
     * @throws IllegalArgumentException If argument is invalid.
     */
    abstract protected ChunkInfo doGetInfo(String chunkName) throws ChunkStorageException, IllegalArgumentException;

    /**
     * Creates a new chunk.
     *
     * @param chunkName String name of the chunk to create.
     * @return ChunkHandle A writable handle for the recently created chunk.
     * @throws ChunkStorageException Throws ChunkStorageException in case of I/O related exceptions.
     * @throws IllegalArgumentException If argument is invalid.
     */
    abstract protected ChunkHandle doCreate(String chunkName) throws ChunkStorageException, IllegalArgumentException;

    /**
     * Determines whether named chunk exists in underlying storage.
     *
     * @param chunkName Name of the chunk to check.
     * @return True if the object exists, false otherwise.
     * @throws ChunkStorageException Throws ChunkStorageException in case of I/O related exceptions.
     * @throws IllegalArgumentException If argument is invalid.
     */
    abstract protected boolean doesExist(String chunkName) throws ChunkStorageException, IllegalArgumentException;

    /**
     * Deletes a chunk.
     *
     * @param handle ChunkHandle of the chunk to delete.
     * @throws ChunkStorageException Throws ChunkStorageException in case of I/O related exceptions.
     * @throws IllegalArgumentException If argument is invalid.
     */
    abstract protected void doDelete(ChunkHandle handle) throws ChunkStorageException, IllegalArgumentException;

    /**
     * Opens chunk for Read.
     *
     * @param chunkName String name of the chunk to read from.
     * @return ChunkHandle A readable handle for the given chunk.
     * @throws ChunkStorageException Throws ChunkStorageException in case of I/O related exceptions.
     * @throws IllegalArgumentException If argument is invalid.
     */
    abstract protected ChunkHandle doOpenRead(String chunkName) throws ChunkStorageException, IllegalArgumentException;

    /**
     * Opens chunk for Write (or modifications).
     *
     * @param chunkName String name of the chunk to write to or modify.
     * @return ChunkHandle A writable handle for the given chunk.
     * @throws ChunkStorageException Throws ChunkStorageException in case of I/O related exceptions.
     * @throws IllegalArgumentException If argument is invalid.
     */
    abstract protected ChunkHandle doOpenWrite(String chunkName) throws ChunkStorageException, IllegalArgumentException;

    /**
     * Reads a range of bytes from the underlying chunk.
     *
     * @param handle ChunkHandle of the chunk to read from.
     * @param fromOffset Offset in the chunk from which to start reading.
     * @param length Number of bytes to read.
     * @param buffer Byte buffer to which data is copied.
     * @param bufferOffset Offset in the buffer at which to start copying read data.
     * @return int Number of bytes read.
     * @throws ChunkStorageException Throws ChunkStorageException in case of I/O related exceptions.
     * @throws IllegalArgumentException If argument is invalid.
     * @throws NullPointerException  If the parameter is null.
     * @throws IndexOutOfBoundsException If the index is out of bounds.
     */
    abstract protected int doRead(ChunkHandle handle, long fromOffset, int length, byte[] buffer, int bufferOffset) throws ChunkStorageException, NullPointerException, IndexOutOfBoundsException;

    /**
     * Writes the given data to the chunk.
     *
     * @param handle ChunkHandle of the chunk to write to.
     * @param offset Offset in the chunk to start writing.
     * @param length Number of bytes to write.
     * @param data An InputStream representing the data to write.
     * @return int Number of bytes written.
     * @throws ChunkStorageException Throws ChunkStorageException in case of I/O related exceptions.
     * @throws IndexOutOfBoundsException Throws IndexOutOfBoundsException in case of invalid index.
     */
    abstract protected int doWrite(ChunkHandle handle, long offset, int length, InputStream data) throws ChunkStorageException, IndexOutOfBoundsException;

    /**
     * Concatenates two or more chunks using storage native functionality. (Eg. Multipart upload.)
     *
     * @param chunks Array of ConcatArgument objects containing info about existing chunks to be concatenated together.
     *               The chunks must be concatenated in the same sequence the arguments are provided.
     * @return int Number of bytes concatenated.
     * @throws ChunkStorageException Throws ChunkStorageException in case of I/O related exceptions.
     * @throws UnsupportedOperationException If this operation is not supported by this provider.
     */
    abstract protected int doConcat(ConcatArgument[] chunks) throws ChunkStorageException, UnsupportedOperationException;

    /**
     * Truncates a given chunk.
     *
     * @param handle ChunkHandle of the chunk to truncate.
     * @param offset Offset to truncate to.
     * @return True if the object was truncated, false otherwise.
     * @throws ChunkStorageException Throws ChunkStorageException in case of I/O related exceptions.
     * @throws UnsupportedOperationException If this operation is not supported by this provider.
     */
    abstract protected boolean doTruncate(ChunkHandle handle, long offset) throws ChunkStorageException, UnsupportedOperationException;

    /**
     * Sets readonly attribute for the chunk.
     *
     * @param handle ChunkHandle of the chunk.
     * @param isReadOnly True if chunk is set to be readonly.
     * @return True if the operation was successful, false otherwise.
     * @throws ChunkStorageException Throws ChunkStorageException in case of I/O related exceptions.
     * @throws UnsupportedOperationException If this operation is not supported by this provider.
     */
    abstract protected boolean doSetReadOnly(ChunkHandle handle, boolean isReadOnly) throws ChunkStorageException, UnsupportedOperationException;
}
