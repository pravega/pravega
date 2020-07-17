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

import com.google.common.annotations.Beta;

import java.io.InputStream;

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
 *
 * It is recommended that the implementations should extend {@link BaseChunkStorage}.
 */
@Beta
public interface ChunkStorage extends AutoCloseable {
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
     * Determines whether named file/object exists in underlying storage.
     *
     * @param chunkName Name of the storage object to check.
     * @return True if the object exists, false otherwise.
     * @throws ChunkStorageException Throws ChunkStorageException in case of I/O related exceptions.
     */
    boolean exists(String chunkName) throws ChunkStorageException;

    /**
     * Creates a new file.
     *
     * @param chunkName String name of the storage object to create.
     * @return ChunkHandle A writable handle for the recently created chunk.
     * @throws ChunkStorageException Throws ChunkStorageException in case of I/O related exceptions.
     */
    ChunkHandle create(String chunkName) throws ChunkStorageException;

    /**
     * Deletes a file.
     *
     * @param handle ChunkHandle of the storage object to delete.
     * @throws ChunkStorageException Throws ChunkStorageException in case of I/O related exceptions.
     */
    void delete(ChunkHandle handle) throws ChunkStorageException;

    /**
     * Opens storage object for Read.
     *
     * @param chunkName String name of the storage object to read from.
     * @return ChunkHandle A readable handle for the given chunk.
     * @throws ChunkStorageException    Throws ChunkStorageException in case of I/O related exceptions.
     * @throws IllegalArgumentException If argument is invalid.
     */
    ChunkHandle openRead(String chunkName) throws ChunkStorageException, IllegalArgumentException;

    /**
     * Opens storage object for Write (or modifications).
     *
     * @param chunkName String name of the storage object to write to or modify.
     * @return ChunkHandle A writable handle for the given chunk.
     * @throws ChunkStorageException    Throws ChunkStorageException in case of I/O related exceptions.
     * @throws IllegalArgumentException If argument is invalid.
     */
    ChunkHandle openWrite(String chunkName) throws ChunkStorageException, IllegalArgumentException;

    /**
     * Retrieves the ChunkInfo for given name.
     *
     * @param chunkName String name of the storage object to read from.
     * @return ChunkInfo Information about the given chunk.
     * @throws ChunkStorageException    Throws ChunkStorageException in case of I/O related exceptions.
     * @throws IllegalArgumentException If argument is invalid.
     */
    ChunkInfo getInfo(String chunkName) throws ChunkStorageException, IllegalArgumentException;

    /**
     * Reads a range of bytes from the underlying storage object.
     *
     * @param handle       ChunkHandle of the storage object to read from.
     * @param fromOffset   Offset in the file from which to start reading.
     * @param length       Number of bytes to read.
     * @param buffer       Byte buffer to which data is copied.
     * @param bufferOffset Offset in the buffer at which to start copying read data.
     * @return int Number of bytes read.
     * @throws ChunkStorageException     Throws ChunkStorageException in case of I/O related exceptions.
     * @throws IllegalArgumentException  If argument is invalid.
     * @throws NullPointerException      If the parameter is null.
     * @throws IndexOutOfBoundsException If the index is out of bounds or offset is not a valid offset in the underlying file/object.
     */
    int read(ChunkHandle handle, long fromOffset, int length, byte[] buffer, int bufferOffset) throws ChunkStorageException, NullPointerException, IndexOutOfBoundsException;

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
     * @param offset Offset in the file to start writing.
     * @param length Number of bytes to write.
     * @param data   An InputStream representing the data to write.
     * @return int Number of bytes written.
     * @throws ChunkStorageException Throws ChunkStorageException in case of I/O related exceptions.
     * @throws IndexOutOfBoundsException When data can not be written at given offset.
     */
    int write(ChunkHandle handle, long offset, int length, InputStream data) throws ChunkStorageException, IndexOutOfBoundsException;

    /**
     * Concatenates two or more chunks using native facility. The first chunk is concatenated to.
     *
     * @param chunks Array of ConcatArgument objects containing info about existing chunks to be appended together.
     *               The chunks must be concatenated in the same sequence the arguments are provided.
     * @return int Number of bytes concatenated.
     * @throws ChunkStorageException         Throws ChunkStorageException in case of I/O related exceptions.
     * @throws UnsupportedOperationException If this operation is not supported by this provider.
     */
    int concat(ConcatArgument[] chunks) throws ChunkStorageException, UnsupportedOperationException;

    /**
     * Truncates a given chunk.
     *
     * @param handle ChunkHandle of the storage object to truncate.
     * @param offset Offset to truncate to.
     * @return True if the object was truncated, false otherwise.
     * @throws ChunkStorageException         Throws ChunkStorageException in case of I/O related exceptions.
     * @throws UnsupportedOperationException If this operation is not supported by this provider.
     */
    boolean truncate(ChunkHandle handle, long offset) throws ChunkStorageException, UnsupportedOperationException;

    /**
     * Makes chunk as either readonly or writable.
     *
     * @param handle     ChunkHandle of the storage object.
     * @param isReadonly True if chunk is set to be readonly.
     * @throws ChunkStorageException         Throws ChunkStorageException in case of I/O related exceptions.
     * @throws UnsupportedOperationException If this operation is not supported by this provider.
     */
    void setReadOnly(ChunkHandle handle, boolean isReadonly) throws ChunkStorageException, UnsupportedOperationException;
}
