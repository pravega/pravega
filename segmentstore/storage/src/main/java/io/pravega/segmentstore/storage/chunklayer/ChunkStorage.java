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

import java.io.InputStream;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

/**
 * Defines an abstraction for Permanent Storage.
 * Note: not all operations defined here are needed.
 * Below are minimum requirements that any implementation must provide.
 * Note that it is the responsibility of storage provider specific implementation to make sure following guarantees are
 * provided even though underlying storage may not provide all primitives or guarantees.
 * <ul>
 * <li>Once an operation is executed and acknowledged as successful then the effects must be permanent and consistent
 * (as opposed to eventually consistent)</li>
 * <li>{@link ChunkStorage#create(String)}  and {@link ChunkStorage#delete(ChunkHandle)} are not idempotent.</li>
 * <li>{@link ChunkStorage#exists(String)} and {@link ChunkStorage#getInfo(String)} must reflect effects of most recent
 * operation performed.</li>
 * </ul>
 * There is no need to implement any special logic to handle concurrent access to the underlying objects/files.
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
 * For concats, {@link ChunkStorage} supports both native and append, ChunkedSegmentStorage will invoke appropriate method depending
 * on size of target and source chunks. (Eg. ECS)
 * <p>
 * To implement custom {@link ChunkStorage} implementation:
 * <ol>
 *     <li> Implement {@link ChunkStorage}. It is recommended that the implementations should extend {@link AsyncBaseChunkStorage}
 *     or {@link BaseChunkStorage}.</li>
 *     <li> Implement {@link io.pravega.segmentstore.storage.SimpleStorageFactory}.  </li>
 *     <li> Implement {@link io.pravega.segmentstore.storage.StorageFactoryCreator}.
 *     <ul>
 *          <li>Return {@link io.pravega.segmentstore.storage.StorageFactoryInfo} object containing name to use to identify.
 *          This identifier is used in config.properties file to identify implementation to use.</li>
 *          <li> The bindings are loaded using ServiceLoader (https://docs.oracle.com/javase/7/docs/api/java/util/ServiceLoader.html)</li>
 *          <li> Add resource file named "io.pravega.segmentstore.storage.StorageFactoryCreator" to the implementation jar.
 *          That file contains name of the class implementing {@link io.pravega.segmentstore.storage.StorageFactoryCreator}.</li>
 *     </ul>
 *     </li>
 * </ol>
 */
@Beta
public interface ChunkStorage extends AutoCloseable, StatsReporter {
    /**
     * Gets a value indicating whether this Storage implementation supports {@link ChunkStorage#truncate(ChunkHandle, long)} operation on underlying storage object.
     *
     * @return True or false.
     */
    boolean supportsTruncation();

    /**
     * Gets a value indicating whether this Storage implementation supports append operation on underlying storage object.
     *
     * @return True or false.
     */
    boolean supportsAppend();

    /**
     * Gets a value indicating whether this Storage implementation supports native merge operation on underlying storage object.
     *
     * @return True or false.
     */
    boolean supportsConcat();

    /**
     * Gets a value indicating whether this Storage implementation actually stores data durably and supports reading it back during data validation.
     * Generally almost always storage will support durable and stable data writes and subsequent retrieval and therefore should return true.
     * However, In certain special cases like mock storages used for test/performance, implementation may ignore or throw away data
     * and in such cases should return false.
     *
     * @return True if data integrity checks are supported. False otherwise.
     */
    default boolean supportsDataIntegrityCheck() {
        return true;
    }


    /**
     * Determines whether named file/object exists in underlying storage.
     *
     * @param chunkName Name of the storage object to check.
     * @return A CompletableFuture that, when completed, will contain True if the object exists, false otherwise.
     * @throws CompletionException If the operation failed, it will be completed with the appropriate exception. Notable Exceptions:
     *                             {@link ChunkStorageException} In case of I/O related exceptions.
     */
    CompletableFuture<Boolean> exists(String chunkName);

    /**
     * Creates a new chunk.
     *
     * @param chunkName String name of the storage object to create.
     * @return A CompletableFuture that, when completed, will contain a writable handle for the recently created chunk.
     * If the operation failed, it will be completed with the appropriate exception. Notable Exceptions:
     * {@link ChunkStorageException} In case of I/O related exceptions.
     */
    CompletableFuture<ChunkHandle> create(String chunkName);

    /**
     * Creates a new chunk with provided content.
     *
     * @param chunkName String name of the storage object to create.
     * @param length Number of bytes to write.
     * @param data   An InputStream representing the data to write.
     * @return A CompletableFuture that, when completed, will contain a writable handle for the recently created chunk.
     * @throws IndexOutOfBoundsException When data can not be written at given offset.
     * @throws CompletionException       If the operation failed, it will be completed with the appropriate exception. Notable Exceptions:
     *                                   {@link ChunkStorageException} In case of I/O related exceptions.
     */
    CompletableFuture<ChunkHandle> createWithContent(String chunkName, int length, InputStream data);

    /**
     * Deletes a chunk.
     *
     * @param handle ChunkHandle of the storage object to delete.
     * @return A CompletableFuture that, when completed, will indicate that the operation completed.
     * If the operation failed, it will be completed with the appropriate exception. Notable Exceptions:
     * {@link ChunkStorageException} In case of I/O related exceptions.
     */
    CompletableFuture<Void> delete(ChunkHandle handle);

    /**
     * Opens storage object for Read.
     *
     * @param chunkName String name of the storage object to read from.
     * @return A CompletableFuture that, when completed, will contain readable handle for the given chunk.
     * If the operation failed, it will be completed with the appropriate exception.
     * @throws IllegalArgumentException If argument is invalid.
     * @throws CompletionException      If the operation failed, it will be completed with the appropriate exception. Notable Exceptions:
     *                                  {@link ChunkStorageException} In case of I/O related exceptions.
     */
    CompletableFuture<ChunkHandle> openRead(String chunkName);

    /**
     * Opens storage object for Write (or modifications).
     *
     * @param chunkName String name of the storage object to write to or modify.
     * @return A CompletableFuture that, when completed, will contain a writable handle for the given chunk.
     * If the operation failed, it will be completed with the appropriate exception.
     * @throws IllegalArgumentException If argument is invalid.
     * @throws CompletionException      If the operation failed, it will be completed with the appropriate exception. Notable Exceptions:
     *                                  {@link ChunkStorageException} In case of I/O related exceptions.
     */
    CompletableFuture<ChunkHandle> openWrite(String chunkName);

    /**
     * Retrieves the ChunkInfo for given name.
     *
     * @param chunkName String name of the storage object to read from.
     * @return A CompletableFuture that, when completed, will contain information about the given chunk.
     * If the operation failed, it will be completed with the appropriate exception.
     * @throws IllegalArgumentException If argument is invalid.
     * @throws CompletionException      If the operation failed, it will be completed with the appropriate exception. Notable Exceptions:
     *                                  {@link ChunkStorageException} In case of I/O related exceptions.
     */
    CompletableFuture<ChunkInfo> getInfo(String chunkName);

    /**
     * Reads a range of bytes from the underlying storage object.
     *
     * @param handle       ChunkHandle of the storage object to read from.
     * @param fromOffset   Offset in the chunk from which to start reading.
     * @param length       Number of bytes to read.
     * @param buffer       Byte buffer to which data is copied.
     * @param bufferOffset Offset in the buffer at which to start copying read data.
     * @return A CompletableFuture that, when completed, will contain number of bytes read.
     * If the operation failed, it will be completed with the appropriate exception.
     * @throws IllegalArgumentException  If argument is invalid.
     * @throws NullPointerException      If the parameter is null.
     * @throws IndexOutOfBoundsException If the index is out of bounds or offset is not a valid offset in the underlying file/object.
     * @throws CompletionException       If the operation failed, it will be completed with the appropriate exception. Notable Exceptions:
     *                                   {@link ChunkStorageException} In case of I/O related exceptions.
     */
    CompletableFuture<Integer> read(ChunkHandle handle, long fromOffset, int length, byte[] buffer, int bufferOffset);

    /**
     * Writes the given data to the underlying storage object.
     *
     * <ul>
     * <li>It is expected that in cases where it can not overwrite the existing data at given offset, the implementation should throw IndexOutOfBoundsException.</li>
     * For storage where underlying files/objects are immutable once written, the implementation should return false on {@link ChunkStorage#supportsAppend()}.
     * <li>In such cases only valid offset is 0.</li>
     * <li>For storages where underlying files/objects can only be appended but not overwritten, it must match actual current length of underlying file/object.</li>
     * <li>In all cases the offset can not be greater that actual current length of underlying file/object. </li>
     * </ul>
     *
     * @param handle ChunkHandle of the storage object to write to.
     * @param offset Offset in the chunk to start writing.
     * @param length Number of bytes to write.
     * @param data   An InputStream representing the data to write.
     * @return A CompletableFuture that, when completed, will contain number of bytes written.
     * @throws IndexOutOfBoundsException When data can not be written at given offset.
     * @throws CompletionException       If the operation failed, it will be completed with the appropriate exception. Notable Exceptions:
     *                                   {@link ChunkStorageException} In case of I/O related exceptions.
     */
    CompletableFuture<Integer> write(ChunkHandle handle, long offset, int length, InputStream data);

    /**
     * Concatenates two or more chunks using native facility. The first chunk is concatenated to.
     *
     * @param chunks Array of ConcatArgument objects containing info about existing chunks to be appended together.
     *               The chunks must be concatenated in the same sequence the arguments are provided.
     * @return A CompletableFuture that, when completed, will contain number of bytes concatenated.
     * @throws UnsupportedOperationException If this operation is not supported by this provider.
     * @throws CompletionException           If the operation failed, it will be completed with the appropriate exception. Notable Exceptions:
     *                                       {@link ChunkStorageException} In case of I/O related exceptions.
     */
    CompletableFuture<Integer> concat(ConcatArgument[] chunks);

    /**
     * Truncates a given chunk.
     *
     * @param handle ChunkHandle of the storage object to truncate.
     * @param offset Offset to truncate to.
     * @return A CompletableFuture that, when completed, will contain True if the object was truncated, false otherwise.
     * @throws UnsupportedOperationException If this operation is not supported by this provider.
     * @throws CompletionException           If the operation failed, it will be completed with the appropriate exception. Notable Exceptions:
     *                                       {@link ChunkStorageException} In case of I/O related exceptions.
     */
    CompletableFuture<Boolean> truncate(ChunkHandle handle, long offset);

    /**
     * Makes chunk as either readonly or writable.
     *
     * @param handle     ChunkHandle of the storage object.
     * @param isReadonly True if chunk is set to be readonly.
     * @return A CompletableFuture that, when completed, will indicate that the operation completed.
     * If the operation failed, it will contain the cause of the failure.
     * @throws UnsupportedOperationException If this operation is not supported by this provider.
     * @throws CompletionException           If the operation failed, it will be completed with the appropriate exception. Notable Exceptions:
     *                                       {@link ChunkStorageException} In case of I/O related exceptions.
     */
    CompletableFuture<Void> setReadOnly(ChunkHandle handle, boolean isReadonly);

    /**
     * Get used space in bytes.
     *
     * @return A CompletableFuture that, when completed, will return the total size of storage used in bytes.
     * If the operation failed, it will contain the cause of the failure.
     * @throws UnsupportedOperationException If this operation is not supported by this provider.
     * @throws CompletionException           If the operation failed, it will be completed with the appropriate exception. Notable Exceptions:
     *                                       {@link ChunkStorageException} In case of I/O related exceptions.
     */
    CompletableFuture<Long> getUsedSpace();
}
