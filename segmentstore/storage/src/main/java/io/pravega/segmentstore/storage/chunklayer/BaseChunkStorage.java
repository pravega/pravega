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
import lombok.extern.slf4j.Slf4j;

import java.io.InputStream;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/**
 * Base implementation of {@link ChunkStorage} that has all synchronized methods.
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
public abstract class BaseChunkStorage extends AsyncBaseChunkStorage {
    /**
     * Constructor.
     *
     * @param executor  An Executor for async operations.
     */
    public BaseChunkStorage(Executor executor) {
        super(executor);
    }

    @Override
    protected CompletableFuture<ChunkInfo> doGetInfoAsync(String chunkName, OperationContext opContext) {
        return execute(() -> doGetInfo(chunkName), opContext);
    }

    @Override
    protected CompletableFuture<ChunkHandle> doCreateAsync(String chunkName, OperationContext opContext) {
        return execute(() -> doCreate(chunkName), opContext);
    }

    @Override
    protected CompletableFuture<ChunkHandle> doCreateWithContentAsync(String chunkName, int length, InputStream data, OperationContext opContext) {
        return execute(() -> doCreateWithContent(chunkName, length, data), opContext);
    }

    @Override
    protected CompletableFuture<Boolean> checkExistsAsync(String chunkName, OperationContext opContext) {
        return execute(() -> checkExists(chunkName), opContext);
    }

    @Override
    protected CompletableFuture<Void> doDeleteAsync(ChunkHandle handle, OperationContext opContext) {
        return execute(() -> {
            doDelete(handle);
            return null;
        }, opContext);
    }

    @Override
    protected CompletableFuture<ChunkHandle> doOpenReadAsync(String chunkName, OperationContext opContext) {
        return execute(() -> doOpenRead(chunkName), opContext);
    }

    @Override
    protected CompletableFuture<ChunkHandle> doOpenWriteAsync(String chunkName, OperationContext opContext) {
        return execute(() -> doOpenWrite(chunkName), opContext);
    }

    @Override
    protected CompletableFuture<Integer> doReadAsync(ChunkHandle handle, long fromOffset, int length, byte[] buffer, int bufferOffset, OperationContext opContext) {
        return execute(() -> doRead(handle, fromOffset, length, buffer, bufferOffset), opContext);
    }

    @Override
    protected CompletableFuture<Integer> doWriteAsync(ChunkHandle handle, long offset, int length, InputStream data, OperationContext opContext) {
        return execute(() -> doWrite(handle, offset, length, data), opContext);
    }

    @Override
    protected CompletableFuture<Integer> doConcatAsync(ConcatArgument[] chunks, OperationContext opContext) {
        return execute(() -> doConcat(chunks), opContext);
    }

    @Override
    protected CompletableFuture<Void> doSetReadOnlyAsync(ChunkHandle handle, boolean isReadOnly, OperationContext opContext) {
        return execute(() -> {
            doSetReadOnly(handle, isReadOnly);
            return null;
        }, opContext);
    }

    @Override
    protected CompletableFuture<Boolean> doTruncateAsync(ChunkHandle handle, long offset, OperationContext opContext) {
        return execute(() -> doTruncate(handle, offset), opContext);
    }

    /**
     * Retrieves the ChunkInfo for given name.
     *
     * @param chunkName String name of the chunk to read from.
     * @return ChunkInfo Information about the given chunk.
     * @throws ChunkStorageException    Throws ChunkStorageException in case of I/O related exceptions.
     * @throws IllegalArgumentException If argument is invalid.
     */
    abstract protected ChunkInfo doGetInfo(String chunkName) throws ChunkStorageException;

    /**
     * Creates a new chunk.
     *
     * @param chunkName String name of the chunk to create.
     * @return ChunkHandle A writable handle for the recently created chunk.
     * @throws ChunkStorageException    Throws ChunkStorageException in case of I/O related exceptions.
     * @throws IllegalArgumentException If argument is invalid.
     */
    abstract protected ChunkHandle doCreate(String chunkName) throws ChunkStorageException;

    /**
     * Creates a new chunk with provided content.
     *
     * @param chunkName String name of the chunk to create.
     * @param length Number of bytes to write.
     * @param data   An InputStream representing the data to write.
     * @return ChunkHandle A writable handle for the recently created chunk.
     * @throws ChunkStorageException    Throws ChunkStorageException in case of I/O related exceptions.
     * @throws IllegalArgumentException If argument is invalid.
     */
    protected ChunkHandle doCreateWithContent(String chunkName, int length, InputStream data) throws ChunkStorageException {
        ChunkHandle handle = doCreate(chunkName);
        int bytesWritten = doWrite(handle, 0, length, data);
        if (bytesWritten < length) {
            doDelete(ChunkHandle.writeHandle(chunkName));
            throw new ChunkStorageException(chunkName, "doCreateWithContent - invalid length returned");
        }
        return handle;
    }

    @Override
    protected CompletableFuture<Long> doGetUsedSpaceAsync(OperationContext opContext) {
        return execute(() -> doGetUsedSpace(opContext), opContext);
    }

    /**
     * Determines whether named chunk exists in underlying storage.
     *
     * @param chunkName Name of the chunk to check.
     * @return True if the object exists, false otherwise.
     * @throws ChunkStorageException    Throws ChunkStorageException in case of I/O related exceptions.
     * @throws IllegalArgumentException If argument is invalid.
     */
    abstract protected boolean checkExists(String chunkName) throws ChunkStorageException;

    /**
     * Deletes a chunk.
     *
     * @param handle ChunkHandle of the chunk to delete.
     * @throws ChunkStorageException    Throws ChunkStorageException in case of I/O related exceptions.
     * @throws IllegalArgumentException If argument is invalid.
     */
    abstract protected void doDelete(ChunkHandle handle) throws ChunkStorageException;

    /**
     * Opens chunk for Read.
     *
     * @param chunkName String name of the chunk to read from.
     * @return ChunkHandle A readable handle for the given chunk.
     * @throws ChunkStorageException    Throws ChunkStorageException in case of I/O related exceptions.
     * @throws IllegalArgumentException If argument is invalid.
     */
    abstract protected ChunkHandle doOpenRead(String chunkName) throws ChunkStorageException;

    /**
     * Opens chunk for Write (or modifications).
     *
     * @param chunkName String name of the chunk to write to or modify.
     * @return ChunkHandle A writable handle for the given chunk.
     * @throws ChunkStorageException    Throws ChunkStorageException in case of I/O related exceptions.
     * @throws IllegalArgumentException If argument is invalid.
     */
    abstract protected ChunkHandle doOpenWrite(String chunkName) throws ChunkStorageException;

    /**
     * Reads a range of bytes from the underlying chunk.
     *
     * @param handle       ChunkHandle of the chunk to read from.
     * @param fromOffset   Offset in the chunk from which to start reading.
     * @param length       Number of bytes to read.
     * @param buffer       Byte buffer to which data is copied.
     * @param bufferOffset Offset in the buffer at which to start copying read data.
     * @return int Number of bytes read.
     * @throws ChunkStorageException     Throws ChunkStorageException in case of I/O related exceptions.
     * @throws IllegalArgumentException  If argument is invalid.
     * @throws NullPointerException      If the parameter is null.
     * @throws IndexOutOfBoundsException If the index is out of bounds.
     */
    abstract protected int doRead(ChunkHandle handle, long fromOffset, int length, byte[] buffer, int bufferOffset) throws ChunkStorageException;

    /**
     * Writes the given data to the chunk.
     *
     * @param handle ChunkHandle of the chunk to write to.
     * @param offset Offset in the chunk to start writing.
     * @param length Number of bytes to write.
     * @param data   An InputStream representing the data to write.
     * @return int Number of bytes written.
     * @throws ChunkStorageException     Throws ChunkStorageException in case of I/O related exceptions.
     * @throws IndexOutOfBoundsException If the index is out of bounds.
     * @throws IllegalArgumentException Throws IllegalArgumentException in case of invalid index.
     */
    abstract protected int doWrite(ChunkHandle handle, long offset, int length, InputStream data) throws ChunkStorageException;

    /**
     * Concatenates two or more chunks using storage native functionality. (Eg. Multipart upload.)
     *
     * @param chunks Array of ConcatArgument objects containing info about existing chunks to be concatenated together.
     *               The chunks must be concatenated in the same sequence the arguments are provided.
     * @return int Number of bytes concatenated.
     * @throws ChunkStorageException         Throws ChunkStorageException in case of I/O related exceptions.
     * @throws UnsupportedOperationException If this operation is not supported by this provider.
     */
    abstract protected int doConcat(ConcatArgument[] chunks) throws ChunkStorageException, UnsupportedOperationException;

    /**
     * Sets readonly attribute for the chunk.
     *
     * @param handle     ChunkHandle of the chunk.
     * @param isReadOnly True if chunk is set to be readonly.
     * @throws ChunkStorageException         Throws ChunkStorageException in case of I/O related exceptions.
     * @throws UnsupportedOperationException If this operation is not supported by this provider.
     */
    abstract protected void doSetReadOnly(ChunkHandle handle, boolean isReadOnly) throws ChunkStorageException, UnsupportedOperationException;

    /**
     * Truncates a given chunk.
     *
     * @param handle ChunkHandle of the chunk to truncate.
     * @param offset Offset to truncate to.
     * @return True if the object was truncated, false otherwise.
     * @throws ChunkStorageException         Throws ChunkStorageException in case of I/O related exceptions.
     * @throws UnsupportedOperationException If this operation is not supported by this provider.
     */
    protected boolean doTruncate(ChunkHandle handle, long offset) throws ChunkStorageException, UnsupportedOperationException {
        throw new UnsupportedOperationException();
    }

    /**
     * Get used space.
     *
     * @param opContext Context for the given operation.
     * @return Return the total size of storage used in bytes. 0 should be returned if not supported.
     * @throws ChunkStorageException         Throws ChunkStorageException in case of I/O related exceptions.
     */
    protected long doGetUsedSpace(OperationContext opContext) throws ChunkStorageException {
        return 0;
    }
}
