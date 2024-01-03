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

import com.google.common.annotations.Beta;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import io.pravega.common.Exceptions;
import io.pravega.common.LoggerHelpers;
import io.pravega.common.Timer;
import java.io.InputStream;
import java.time.Duration;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.Getter;
import lombok.Setter;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

/**
 * Base implementation of {@link ChunkStorage}.
 * It implements common functionality that can be used by derived classes.
 * Delegates to specific implementations by calling various abstract methods which must be overridden in derived classes.
 *
 * Below are minimum requirements that any implementation must provide.
 * Note that it is the responsibility of storage provider specific implementation to make sure following guarantees are provided even
 * though underlying storage may not provide all primitives or guarantees.
 * <ul>
 * <li>Once an operation is executed and acknowledged as successful then the effects must be permanent and consistent (as opposed to eventually consistent)</li>
 * <li>{@link ChunkStorage#create(String)}  and {@link ChunkStorage#delete(ChunkHandle)} are not idempotent.</li>
 * <li>{@link ChunkStorage#exists(String)} and {@link ChunkStorage#getInfo(String)} must reflect effects of most recent operation performed.</li>
 * </ul>
 *
 * There are a few different capabilities that ChunkStorage may provide.
 * <ul>
 * <li> Does {@link ChunkStorage} support appending to existing chunks?
 * This is indicated by {@link ChunkStorage#supportsAppend()}. For example S3 compatible Chunk Storage this would return false. </li>
 * <li> Does {@link ChunkStorage}  support for concatenating chunks? This is indicated by {@link ChunkStorage#supportsConcat()}.
 * If this is true then concat operation concat will be invoked otherwise append functionality is invoked.</li>
 * <li>In addition {@link ChunkStorage} may provide ability to truncate chunks at given offsets (either at front end or at tail end). 
 * This is indicated by {@link ChunkStorage#supportsTruncation()}. </li>
 * </ul>
 * There are some obvious constraints - If ChunkStorage supports concat but not natively then it must support append .
 *
 * For concats, {@link ChunkStorage} supports both native and append, ChunkedSegmentStorage will invoke appropriate method depending on size of target and source chunks. (Eg. ECS)
 *
 * The implementations in this repository are tested using following test suites.
 * <ul>
 * <li>SimpleStorageTests</li>
 * <li>ChunkedRollingStorageTests</li>
 * <li>ChunkStorageTests</li>
 * <li>SystemJournalTests</li>
 * </ul>
 */
@Slf4j
@Beta
public abstract class AsyncBaseChunkStorage implements ChunkStorage {

    private final AtomicBoolean closed;

    private final Executor executor;

    /**
     * Constructor.
     *
     * @param executor  An Executor for async operations.
     */
    public AsyncBaseChunkStorage(Executor executor) {
        this.closed = new AtomicBoolean(false);
        this.executor = Preconditions.checkNotNull(executor, "executor");
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
     * @return A CompletableFuture that, when completed, will contain True if the object exists, False otherwise.
     */
    @Override
    final public CompletableFuture<Boolean> exists(String chunkName) {
        Exceptions.checkNotClosed(this.closed.get(), this);
        // Validate parameters
        checkChunkName(chunkName);

        val traceId = LoggerHelpers.traceEnter(log, "exists", chunkName);
        val opContext = new OperationContext();

        // Call concrete implementation.
        val returnFuture = checkExistsAsync(chunkName, opContext);
        if (log.isTraceEnabled()) {
            returnFuture.thenAcceptAsync(retValue -> LoggerHelpers.traceLeave(log, "exists", traceId, chunkName), executor);
        }

        return returnFuture;
    }

    /**
     * Creates a new chunk.
     *
     * @param chunkName Name of the chunk to create.
     * @return A CompletableFuture that, when completed, will contain a writable handle for the recently created chunk.
     * If the operation failed, it will be completed with the appropriate exception. Notable Exceptions:
     * {@link ChunkStorageException} In case of I/O related exceptions.
     */
    @Override
    final public CompletableFuture<ChunkHandle> create(String chunkName) {
        Exceptions.checkNotClosed(this.closed.get(), this);
        // Validate parameters
        checkChunkName(chunkName);

        val traceId = LoggerHelpers.traceEnter(log, "create", chunkName);
        val opContext = new OperationContext();

        // Call concrete implementation.
        val returnFuture = doCreateAsync(chunkName, opContext);
        returnFuture.thenAcceptAsync(handle -> {
            // Record metrics.
            val elapsed = opContext.getInclusiveLatency();
            ChunkStorageMetrics.CREATE_LATENCY.reportSuccessEvent(elapsed);
            ChunkStorageMetrics.CREATE_COUNT.inc();
            log.debug("Create - chunk={}, latency={}.", chunkName, elapsed.toMillis());
            LoggerHelpers.traceLeave(log, "create", traceId, chunkName);
        }, executor);
        return returnFuture;
    }

    @Override
    final public CompletableFuture<ChunkHandle> createWithContent(String chunkName, int length, InputStream data) {
        Exceptions.checkNotClosed(this.closed.get(), this);
        // Validate parameters
        checkChunkName(chunkName);
        Preconditions.checkArgument(null != data, "data must not be null");
        Preconditions.checkArgument(length > 0, "length must be non-zero and non-negative. Chunk=%s length=%s", chunkName, length);

        val traceId = LoggerHelpers.traceEnter(log, "CreateWithContent", chunkName);
        val opContext = new OperationContext();

        // Call concrete implementation.
        val returnFuture = doCreateWithContentAsync(chunkName, length, data, opContext);
        returnFuture.thenAcceptAsync(handle -> {
            // Record metrics.
            val elapsed = opContext.getInclusiveLatency();
            // Write metrics for create
            ChunkStorageMetrics.WRITE_LATENCY.reportSuccessEvent(elapsed);
            ChunkStorageMetrics.WRITE_BYTES.add(length);
            ChunkStorageMetrics.CREATE_COUNT.inc();
            log.debug("CreateWithContent - chunk={}, bytesWritten={}, latency={}.", handle.getChunkName(), length, elapsed.toMillis());
            LoggerHelpers.traceLeave(log, "CreateWithContent", traceId, chunkName);
        }, executor);
        return returnFuture;
    }

    /**
     * Deletes a chunk.
     *
     * @param handle ChunkHandle of the chunk to delete.
     * @return A CompletableFuture that, when completed, will indicate that the operation completed.
     * If the operation failed, it will be completed with the appropriate exception. Notable Exceptions:
     * {@link ChunkStorageException} In case of I/O related exceptions.
     */
    @Override
    final public CompletableFuture<Void> delete(ChunkHandle handle) {
        Exceptions.checkNotClosed(this.closed.get(), this);
        // Validate parameters
        Preconditions.checkArgument(null != handle, "handle must not be null");
        checkChunkName(handle.getChunkName());
        Preconditions.checkArgument(!handle.isReadOnly(), "handle must not be readonly. Chunk=%s", handle.getChunkName());

        val traceId = LoggerHelpers.traceEnter(log, "delete", handle.getChunkName());
        val opContext = new OperationContext();

        // Call concrete implementation.
        val returnFuture = doDeleteAsync(handle, opContext);
        returnFuture.thenRunAsync(() -> {
            // Record metrics.
            val elapsed = opContext.getInclusiveLatency();
            ChunkStorageMetrics.DELETE_LATENCY.reportSuccessEvent(elapsed);
            ChunkStorageMetrics.DELETE_COUNT.inc();

            log.debug("Delete - chunk={}, latency={}.", handle.getChunkName(), elapsed.toMillis());
            LoggerHelpers.traceLeave(log, "delete", traceId, handle.getChunkName());
        }, executor);

        return returnFuture;
    }

    /**
     * Opens chunk for Read.
     *
     * @param chunkName String name of the chunk to read from.
     * @return A CompletableFuture that, when completed, will contain readable handle for the given chunk.
     * @throws IllegalArgumentException If argument is invalid.
     * @throws CompletionException If the operation failed, it will be completed with the appropriate exception. Notable Exceptions:
     * {@link ChunkStorageException} In case of I/O related exceptions.
     */
    @Override
    final public CompletableFuture<ChunkHandle> openRead(String chunkName) {
        Exceptions.checkNotClosed(this.closed.get(), this);
        // Validate parameters
        checkChunkName(chunkName);

        val traceId = LoggerHelpers.traceEnter(log, "openRead", chunkName);
        val opContext = new OperationContext();

        // Call concrete implementation.
        val returnFuture = doOpenReadAsync(chunkName, opContext);
        if (log.isTraceEnabled()) {
            returnFuture.thenAcceptAsync(handle -> LoggerHelpers.traceLeave(log, "openRead", traceId, chunkName), executor);
        }
        return returnFuture;
    }

    /**
     * Opens chunk for Write (or modifications).
     *
     * @param chunkName String name of the chunk to write to or modify.
     * @return A CompletableFuture that, when completed, will contain a writable handle for the given chunk.
     * @throws IllegalArgumentException If argument is invalid.
     * @throws CompletionException If the operation failed, it will be completed with the appropriate exception. Notable Exceptions:
     * {@link ChunkStorageException} In case of I/O related exceptions.
     */
    @Override
    final public CompletableFuture<ChunkHandle> openWrite(String chunkName) {
        Exceptions.checkNotClosed(this.closed.get(), this);
        // Validate parameters
        checkChunkName(chunkName);

        long traceId = LoggerHelpers.traceEnter(log, "openWrite", chunkName);
        val opContext = new OperationContext();

        // Call concrete implementation.
        val returnFuture = doOpenWriteAsync(chunkName, opContext);
        if (log.isTraceEnabled()) {
            returnFuture.thenAcceptAsync(handle -> LoggerHelpers.traceLeave(log, "openWrite", traceId, chunkName), executor);
        }
        return returnFuture;
    }

    /**
     * Retrieves the ChunkInfo for given name.
     *
     * @param chunkName String name of the chunk to read from.
     * @return A CompletableFuture that, when completed, will contain information about the given chunk.
     * @throws IllegalArgumentException If argument is invalid.
     * @throws CompletionException If the operation failed, it will be completed with the appropriate exception. Notable Exceptions:
     * {@link ChunkStorageException} In case of I/O related exceptions.
     */
    @Override
    final public CompletableFuture<ChunkInfo> getInfo(String chunkName) {
        Exceptions.checkNotClosed(this.closed.get(), this);
        // Validate parameters
        checkChunkName(chunkName);
        long traceId = LoggerHelpers.traceEnter(log, "getInfo", chunkName);
        val opContext = new OperationContext();

        // Call concrete implementation.
        val returnFuture = doGetInfoAsync(chunkName, opContext);
        if (log.isTraceEnabled()) {
            returnFuture.thenAcceptAsync(info -> LoggerHelpers.traceLeave(log, "getInfo", traceId, chunkName), executor);
        }
        return returnFuture;
    }

    /**
     * Reads a range of bytes from the underlying chunk.
     *
     * @param handle       ChunkHandle of the chunk to read from.
     * @param fromOffset   Offset in the chunk from which to start reading.
     * @param length       Number of bytes to read.
     * @param buffer       Byte buffer to which data is copied.
     * @param bufferOffset Offset in the buffer at which to start copying read data.
     * @return A CompletableFuture that, when completed, will contain number of bytes read.
     * @throws IllegalArgumentException  If argument is invalid.
     * @throws IndexOutOfBoundsException If the index is out of bounds or offset is not a valid offset in the underlying file/object.
     * @throws CompletionException If the operation failed, it will be completed with the appropriate exception. Notable Exceptions:
     * {@link ChunkStorageException} In case of I/O related exceptions.
     */
    @Override
    final public CompletableFuture<Integer> read(ChunkHandle handle, long fromOffset, int length, byte[] buffer, int bufferOffset) {
        Exceptions.checkNotClosed(this.closed.get(), this);
        // Validate parameters
        Preconditions.checkArgument(null != handle, "handle must not be null");
        checkChunkName(handle.getChunkName());
        Preconditions.checkArgument(null != buffer, "buffer must not be null");
        Preconditions.checkArgument(fromOffset >= 0, "fromOffset must be non-negative. Chunk=%s fromOffset=%s", handle.getChunkName(), fromOffset);
        Preconditions.checkArgument(length >= 0 && length <= buffer.length,
                "length must be non-negative and must not exceed buffer. Chunk=%s length=%s buffer.length=%s", handle.getChunkName(), length, buffer.length);
        Preconditions.checkElementIndex(bufferOffset, buffer.length, "bufferOffset");

        val traceId = LoggerHelpers.traceEnter(log, "read", handle.getChunkName(), fromOffset, bufferOffset, length);
        val opContext = new OperationContext();

        // Call concrete implementation.
        val returnFuture = doReadAsync(handle, fromOffset, length, buffer, bufferOffset, opContext);
        returnFuture.thenAcceptAsync(bytesRead -> {
            val elapsed = opContext.getInclusiveLatency();
            ChunkStorageMetrics.READ_LATENCY.reportSuccessEvent(elapsed);
            ChunkStorageMetrics.READ_BYTES.add(bytesRead);

            log.debug("Read - chunk={}, offset={}, bytesRead={}, latency={}.", handle.getChunkName(), fromOffset, length, elapsed.toMillis());
            LoggerHelpers.traceLeave(log, "read", traceId, bytesRead);
        }, executor);

        return returnFuture;
    }

    /**
     * Writes the given data to the underlying chunk.
     *
     * <ul>
     * <li>It is expected that in cases where it can not overwrite the existing data at given offset, the implementation should throw IndexOutOfBoundsException.</li>
     * For storage where underlying files/objects are immutable once written, the implementation should return false on {@link ChunkStorage#supportsAppend()}.
     * <li>In such cases only valid offset is 0.</li>
     * <li>For storages where underlying files/objects can only be appended but not overwritten, it must match actual current length of underlying file/object.</li>
     * <li>In all cases the offset can not be greater that actual current length of underlying file/object. </li>
     * </ul>
     * @param handle ChunkHandle of the chunk to write to.
     * @param offset Offset in the chunk to start writing.
     * @param length Number of bytes to write.
     * @param data   An InputStream representing the data to write.
     * @return A CompletableFuture that, when completed, will contain number of bytes written.
     * @throws CompletionException If the operation failed, it will be completed with the appropriate exception. Notable Exceptions:
     * {@link ChunkStorageException} In case of I/O related exceptions.
     */
    @Override
    final public CompletableFuture<Integer> write(ChunkHandle handle, long offset, int length, InputStream data) {
        Exceptions.checkNotClosed(this.closed.get(), this);
        // Validate parameters
        Preconditions.checkArgument(null != handle, "handle must not be null");
        checkChunkName(handle.getChunkName());
        Preconditions.checkArgument(!handle.isReadOnly(), "handle must not be readonly. Chunk = %s", handle.getChunkName());
        Preconditions.checkArgument(null != data, "data must not be null");
        Preconditions.checkArgument(offset >= 0, "offset must be non-negative. Chunk=%s offset=%s", handle.getChunkName(), offset);
        Preconditions.checkArgument(length >= 0, "length must be non-negative. Chunk=%s length=%s", handle.getChunkName(), length);
        if (!supportsAppend()) {
            Preconditions.checkArgument(offset == 0, "offset must be 0 because storage does not support appends.");
        }

        val traceId = LoggerHelpers.traceEnter(log, "write", handle.getChunkName(), offset, length);
        val opContext = new OperationContext();

        // Call concrete implementation.
        val returnFuture = doWriteAsync(handle, offset, length, data, opContext);
        returnFuture.thenAcceptAsync(bytesWritten -> {
            Preconditions.checkState(bytesWritten == length, "Wrong number of bytes written. Expected(%s) Actual(%s)", length, bytesWritten);
            val elapsed = opContext.getInclusiveLatency();

            ChunkStorageMetrics.WRITE_LATENCY.reportSuccessEvent(elapsed);
            ChunkStorageMetrics.WRITE_BYTES.add(bytesWritten);

            log.debug("Write - chunk={}, offset={}, bytesWritten={}, latency={}.", handle.getChunkName(), offset, length, elapsed.toMillis());
            LoggerHelpers.traceLeave(log, "write", traceId, bytesWritten);
        }, executor);
        return returnFuture;
    }

    /**
     * Concatenates two or more chunks. The first chunk is concatenated to.
     *
     * @param chunks Array of ConcatArgument objects containing info about existing chunks to be concatenated together.
     *               The chunks must be concatenated in the same sequence the arguments are provided.
     * @return A CompletableFuture that, when completed, will contain number of bytes concatenated.
     * @throws UnsupportedOperationException If this operation is not supported by this provider.
     * @throws CompletionException If the operation failed, it will be completed with the appropriate exception. Notable Exceptions:
     * {@link ChunkStorageException} In case of I/O related exceptions.
     */
    @Override
    final public CompletableFuture<Integer> concat(ConcatArgument[] chunks) {
        if (!supportsConcat()) {
            throw new UnsupportedOperationException("Chunk storage does not support doConcat");
        }
        Exceptions.checkNotClosed(this.closed.get(), this);
        checkConcatArgs(chunks);

        val traceId = LoggerHelpers.traceEnter(log, "concat", chunks[0].getName());
        val opContext = new OperationContext();

        // Call concrete implementation.
        val returnFuture = doConcatAsync(chunks, opContext);

        returnFuture.thenAcceptAsync(retValue -> {
            val elapsed = opContext.getInclusiveLatency();
            log.debug("concat - target={}, latency={}.", chunks[0].getName(), elapsed.toMillis());

            ChunkStorageMetrics.CONCAT_LATENCY.reportSuccessEvent(elapsed);
            ChunkStorageMetrics.CONCAT_BYTES.add(retValue);
            ChunkStorageMetrics.CONCAT_COUNT.inc();
            ChunkStorageMetrics.LARGE_CONCAT_COUNT.inc();

            LoggerHelpers.traceLeave(log, "concat", traceId, chunks[0].getName());

        }, executor);

        return returnFuture;
    }

    private void checkConcatArgs(ConcatArgument[] chunks) {
        // Validate parameters
        Preconditions.checkArgument(null != chunks, "chunks must not be null");
        Preconditions.checkArgument(chunks.length >= 2, "There must be at least two chunks");

        Preconditions.checkArgument(null != chunks[0], "target chunk must not be null");
        Preconditions.checkArgument(chunks[0].getLength() >= 0, "target chunk length must be non negative.");
        checkChunkName(chunks[0].getName());

        for (int i = 1; i < chunks.length; i++) {
            Preconditions.checkArgument(null != chunks[i], "source chunk must not be null");
            checkChunkName(chunks[i].getName());
            Preconditions.checkArgument(chunks[i].getLength() >= 0, "source chunk length must be non negative.");
            Preconditions.checkArgument(!chunks[i].getName().equals(chunks[0].getName()), "source chunk is same as target");
            Preconditions.checkArgument(!chunks[i].getName().equals(chunks[i - 1].getName()), "duplicate chunk found");
        }
    }

    /**
     * Truncates a given chunk.
     *
     * @param handle ChunkHandle of the chunk to truncate.
     * @param offset Offset to truncate to.
     * @return A CompletableFuture that, when completed, will contain True if the object was truncated, false otherwise.
     * @throws UnsupportedOperationException If this operation is not supported by this provider.
     * @throws CompletionException If the operation failed, it will be completed with the appropriate exception. Notable Exceptions:
     * {@link ChunkStorageException} In case of I/O related exceptions.
     */
    @Override
    final public CompletableFuture<Boolean> truncate(ChunkHandle handle, long offset) {
        Exceptions.checkNotClosed(this.closed.get(), this);
        // Validate parameters
        Preconditions.checkArgument(null != handle, "handle must not be null");
        checkChunkName(handle.getChunkName());
        Preconditions.checkArgument(!handle.isReadOnly(), "handle must not be readonly. Chunk=%s", handle.getChunkName());
        Preconditions.checkArgument(offset >= 0, "offset must be non-negative. Chunk=%s offset=%s", handle.getChunkName(), offset);

        val traceId = LoggerHelpers.traceEnter(log, "truncate", handle.getChunkName());

        // Call concrete implementation.
        val opContext = new OperationContext();
        val returnFuture = doTruncateAsync(handle, offset, opContext);
        if (log.isTraceEnabled()) {
            returnFuture.thenAcceptAsync(retValue -> LoggerHelpers.traceLeave(log, "truncate", traceId, handle.getChunkName()), executor);
        }
        return returnFuture;
    }

    /**
     * Sets readonly attribute for the chunk.
     *
     * @param handle     ChunkHandle of the chunk.
     * @param isReadonly True if chunk is set to be readonly.
     * @return A CompletableFuture that, when completed, will indicate that the operation completed.
     *          If the operation failed, it will contain the cause of the failure.
     * @throws UnsupportedOperationException If this operation is not supported by this provider.
     * @throws CompletionException If the operation failed, it will be completed with the appropriate exception. Notable Exceptions:
     * {@link ChunkStorageException} In case of I/O related exceptions.
     */
    @Override
    final public CompletableFuture<Void> setReadOnly(ChunkHandle handle, boolean isReadonly) {
        Exceptions.checkNotClosed(this.closed.get(), this);
        // Validate parameters
        Preconditions.checkArgument(null != handle, "handle must not be null");
        checkChunkName(handle.getChunkName());

        val traceId = LoggerHelpers.traceEnter(log, "setReadOnly", handle.getChunkName());

        // Call concrete implementation.
        val opContext = new OperationContext();
        val returnFuture = doSetReadOnlyAsync(handle, isReadonly, opContext);
        if (log.isTraceEnabled()) {
            returnFuture.thenAcceptAsync(v -> LoggerHelpers.traceLeave(log, "setReadOnly", traceId, handle.getChunkName()), executor);
        }
        return returnFuture;
    }

    /**
     * Get used space in bytes.
     *
     * @return Return the total size of storage used in bytes. 0 should be returned if not supported.
     * If the operation failed, it will contain the cause of the failure.
     * @throws CompletionException           If the operation failed, it will be completed with the appropriate exception. Notable Exceptions:
     *                                       {@link ChunkStorageException} In case of I/O related exceptions.
     */
    final public CompletableFuture<Long> getUsedSpace() {
        Exceptions.checkNotClosed(this.closed.get(), this);

        val traceId = LoggerHelpers.traceEnter(log, "getUsedSpace");

        // Call concrete implementation.
        val opContext = new OperationContext();
        val returnFuture = doGetUsedSpaceAsync(opContext);
        if (log.isTraceEnabled()) {
            returnFuture.thenAcceptAsync(v -> LoggerHelpers.traceLeave(log, "getUsedSpace", traceId), executor);
        }
        return returnFuture;
    }

    /**
     * Closes.
     *
     */
    @Override
    public void close() {
        this.closed.set(true);
    }

    @Override
    public void report() {
        // Nothing to report yet.
    }

    /**
     * Retrieves the ChunkInfo for given name.
     *
     * @param chunkName String name of the chunk to read from.
     * @param opContext Context for the given operation.
     * @return A CompletableFuture that, when completed, will contain information about the given chunk.
     * @throws IllegalArgumentException If argument is invalid.
     * @throws CompletionException If the operation failed, it will be completed with the appropriate exception. Notable Exceptions:
     * {@link ChunkStorageException} In case of I/O related exceptions.
     */
    abstract protected CompletableFuture<ChunkInfo> doGetInfoAsync(String chunkName, OperationContext opContext);

    /**
     * Creates a new chunk.
     *
     * @param chunkName String name of the chunk to create.
     * @param opContext Context for the given operation.
     * @return A CompletableFuture that, when completed, will contain a writable handle for the recently created chunk.
     * @throws IllegalArgumentException If argument is invalid.
     * @throws CompletionException If the operation failed, it will be completed with the appropriate exception. Notable Exceptions:
     * {@link ChunkStorageException} In case of I/O related exceptions.
     */
    abstract protected CompletableFuture<ChunkHandle> doCreateAsync(String chunkName, OperationContext opContext);

    /**
     * Creates a new chunk with provided content.
     *
     * @param chunkName String name of the chunk to create.
     * @param length Number of bytes to write.
     * @param data   An InputStream representing the data to write.
     * @param opContext Context for the given operation.
     * @return A CompletableFuture that, when completed, will contain a writable handle for the recently created chunk.
     * @throws IllegalArgumentException If argument is invalid.
     * @throws CompletionException If the operation failed, it will be completed with the appropriate exception. Notable Exceptions:
     * {@link ChunkStorageException} In case of I/O related exceptions.
     */
    abstract protected CompletableFuture<ChunkHandle> doCreateWithContentAsync(String chunkName, int length, InputStream data, OperationContext opContext);

    /**
     * Determines whether named chunk exists in underlying storage.
     *
     * @param chunkName Name of the chunk to check.
     * @param opContext Context for the given operation.
     * @return A CompletableFuture that, when completed, will contain True if the object exists, false otherwise.
     * @throws IllegalArgumentException If argument is invalid.
     * @throws CompletionException If the operation failed, it will be completed with the appropriate exception. Notable Exceptions:
     * {@link ChunkStorageException} In case of I/O related exceptions.
     */
    abstract protected CompletableFuture<Boolean> checkExistsAsync(String chunkName, OperationContext opContext);

    /**
     * Deletes a chunk.
     *
     * @param handle ChunkHandle of the chunk to delete.
     * @param opContext Context for the given operation.
     * @return A CompletableFuture that, when completed, will indicate that the operation completed.
     *          If the operation failed, it will contain the cause of the failure.
     * @throws IllegalArgumentException If argument is invalid.
     * @throws CompletionException If the operation failed, it will be completed with the appropriate exception. Notable Exceptions:
     * {@link ChunkStorageException} In case of I/O related exceptions.
     */
    abstract protected CompletableFuture<Void> doDeleteAsync(ChunkHandle handle, OperationContext opContext);

    /**
     * Opens chunk for Read.
     *
     * @param chunkName String name of the chunk to read from.
     * @param opContext Context for the given operation.
     * @return A CompletableFuture that, when completed, will contain a readable handle for the given chunk.
     * @throws IllegalArgumentException If argument is invalid.
     * @throws CompletionException If the operation failed, it will be completed with the appropriate exception. Notable Exceptions:
     * {@link ChunkStorageException} In case of I/O related exceptions.
     */
    abstract protected CompletableFuture<ChunkHandle> doOpenReadAsync(String chunkName, OperationContext opContext);

    /**
     * Opens chunk for Write (or modifications).
     *
     * @param chunkName String name of the chunk to write to or modify.
     * @param opContext Context for the given operation.
     * @return A CompletableFuture that, when completed, will contain a writable handle for the given chunk.
     * @throws IllegalArgumentException If argument is invalid.
     * @throws CompletionException If the operation failed, it will be completed with the appropriate exception. Notable Exceptions:
     * {@link ChunkStorageException} In case of I/O related exceptions.
     */
    abstract protected CompletableFuture<ChunkHandle> doOpenWriteAsync(String chunkName, OperationContext opContext);

    /**
     * Reads a range of bytes from the underlying chunk.
     *
     * @param handle       ChunkHandle of the chunk to read from.
     * @param fromOffset   Offset in the chunk from which to start reading.
     * @param length       Number of bytes to read.
     * @param buffer       Byte buffer to which data is copied.
     * @param bufferOffset Offset in the buffer at which to start copying read data.
     * @param opContext Context for the given operation.
     * @return A CompletableFuture that, when completed, will contain number of bytes read.
     * @throws IllegalArgumentException  If argument is invalid.
     * @throws NullPointerException      If the parameter is null.
     * @throws IndexOutOfBoundsException If the index is out of bounds.
     * @throws CompletionException If the operation failed, it will be completed with the appropriate exception. Notable Exceptions:
     * {@link ChunkStorageException} In case of I/O related exceptions.
     */
    abstract protected CompletableFuture<Integer> doReadAsync(ChunkHandle handle, long fromOffset, int length, byte[] buffer, int bufferOffset, OperationContext opContext);

    /**
     * Writes the given data to the chunk.
     *
     * @param handle ChunkHandle of the chunk to write to.
     * @param offset Offset in the chunk to start writing.
     * @param length Number of bytes to write.
     * @param data   An InputStream representing the data to write.
     * @param opContext Context for the given operation.
     * @return A CompletableFuture that, when completed, will contain number of bytes written.
     * @throws IndexOutOfBoundsException If the index is out of bounds.
     * @throws IllegalArgumentException Throws IllegalArgumentException in case of invalid index.
     * @throws CompletionException If the operation failed, it will be completed with the appropriate exception. Notable Exceptions:
     * {@link ChunkStorageException} In case of I/O related exceptions.
     */
    abstract protected CompletableFuture<Integer> doWriteAsync(ChunkHandle handle, long offset, int length, InputStream data, OperationContext opContext);

    /**
     * Concatenates two or more chunks using storage native functionality. (Eg. Multipart upload.)
     *
     * @param chunks Array of ConcatArgument objects containing info about existing chunks to be concatenated together.
     *               The chunks must be concatenated in the same sequence the arguments are provided.
     * @param opContext Context for the given operation.
     * @return A CompletableFuture that, when completed, will contain number of bytes concatenated.
     * @throws UnsupportedOperationException If this operation is not supported by this provider.
     * @throws CompletionException If the operation failed, it will be completed with the appropriate exception. Notable Exceptions:
     * {@link ChunkStorageException} In case of I/O related exceptions.
     */
    abstract protected CompletableFuture<Integer> doConcatAsync(ConcatArgument[] chunks, OperationContext opContext);

    /**
     * Sets readonly attribute for the chunk.
     *
     * @param handle     ChunkHandle of the chunk.
     * @param isReadOnly True if chunk is set to be readonly.
     * @param opContext Context for the given operation.
     * @return A CompletableFuture that, when completed, will indicate that the operation completed.
     *          If the operation failed, it will contain the cause of the failure.
     * @throws UnsupportedOperationException If this operation is not supported by this provider.
     * @throws CompletionException If the operation failed, it will be completed with the appropriate exception. Notable Exceptions:
     * {@link ChunkStorageException} In case of I/O related exceptions.
     */
    abstract protected CompletableFuture<Void> doSetReadOnlyAsync(ChunkHandle handle, boolean isReadOnly, OperationContext opContext);

    /**
     * Truncates a given chunk.
     *
     * @param handle ChunkHandle of the chunk to truncate.
     * @param offset Offset to truncate to.
     * @param opContext Context for the given operation.
     * @return True if the object was truncated, false otherwise.
     * @throws UnsupportedOperationException If this operation is not supported by this provider.
     * @throws CompletionException If the operation failed, it will be completed with the appropriate exception. Notable Exceptions:
     * {@link ChunkStorageException} In case of I/O related exceptions.
     */
    protected CompletableFuture<Boolean> doTruncateAsync(ChunkHandle handle, long offset, OperationContext opContext) {
        throw new UnsupportedOperationException();
    }

    /**
     * Get used space in bytes.
     *
     * @param opContext Context for the given operation.
     * @return A CompletableFuture that, when completed, will return the total size of storage used in bytes.
     *         0 will be returned if not supported.
     * If the operation failed, it will contain the cause of the failure.
     * @throws CompletionException           If the operation failed, it will be completed with the appropriate exception. Notable Exceptions:
     *                                       {@link ChunkStorageException} In case of I/O related exceptions.
     */
    abstract protected CompletableFuture<Long> doGetUsedSpaceAsync(OperationContext opContext);

    /**
     * Validate chunk name.
     * @param chunkName Chunk name.
     */
    protected void checkChunkName(String chunkName) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(chunkName), "chunk name must not be null or empty");
    }

    /**
     * Executes the given Callable and returns its result, while translating any Exceptions bubbling out of it into
     * StreamSegmentExceptions.
     *
     * @param operation The function to execute.
     * @param opContext Context for the given operation.
     * @param <R>       Return type of the operation.
     * @return CompletableFuture<R> of the return type of the operation.
     */
    protected <R> CompletableFuture<R> execute(Callable<R> operation, OperationContext opContext) {
        return CompletableFuture.supplyAsync(() -> {
            Exceptions.checkNotClosed(this.closed.get(), this);
            try {
                val timer = new Timer();
                val ret = operation.call();
                opContext.setInclusiveLatency(timer.getElapsed());
                return ret;
            } catch (Exception e) {
                throw new CompletionException(e);
            }
        }, executor);
    }

    /**
     * Context data used to capture operation runtime information.
     */
    protected static class OperationContext {
        /**
         * End to end latency of the operation.
         */
        @Getter
        @Setter
        private volatile Duration inclusiveLatency = Duration.ZERO;
    }
}
