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

import java.io.InputStream;

/**
 * Defines an abstraction for Permanent Storage.
 * Note: not all operations defined here are needed.
 * <div>
 * Below are minimum requirements that any implementation must provide.
 * Note that it is the responsibility of storage provider specific implementation to make sure following guarantees are provided even
 * though underlying storage may not provide all primitives or guarantees.
 * <ul>
 *     <li>Once an operation is executed and acknowledged as successful then the effects must be permanent and consistent (as opposed to eventually consistent)</li>
 *     <li>{@link ChunkStorageProvider#create(String)}  and {@link ChunkStorageProvider#delete(ChunkHandle)} are not idempotent.</li>
 *     <li>{@link ChunkStorageProvider#exists(String)} and {@link ChunkStorageProvider#getInfo(String)} must reflect effects of most recent operation performed.</li>
 * </ul>
 *</div>
 * <div>
 * There are a few different capabilities that ChunkStorageProvider needs to provide.
 * <ul>
 * <li> Does {@link ChunkStorageProvider}  support appending to existing chunks? For vanilla S3 compatible this would return false. This is indicated by {@link ChunkStorageProvider#supportsAppend()}. </li>
 * <li> Does {@link ChunkStorageProvider}  support for concatenating chunks natively? This is indicated by {@link ChunkStorageProvider#supportsConcat()}.
 * If this is true then native concat operation concat will be invoked otherwise concatWithAppend is invoked.</li>
 * <li>In addition {@link ChunkStorageProvider} may provide ability to truncate chunks at given offsets (either at front end or at tail end). This is indicated by {@link ChunkStorageProvider#supportsTruncation()}. </li>
 * </ul>
 * There are some obvious constraints - If ChunkStorageProvider supports concat but not natively then it must support append .
 *
 * For concats, {@link ChunkStorageProvider} supports both native and append, ChunkStorageManager will invoke appropriate method depending on size of target and source chunks. (Eg. ECS)
 * </div>
 */
public interface ChunkStorageProvider extends AutoCloseable {
    /**
     * Gets a value indicating whether this Storage implementation supports {@link ChunkStorageProvider#truncate(ChunkHandle, long)} operation on underlying storage object.
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
     * @throws ChunkStorageException Throws ChunkStorageException in case of I/O related exceptions.
     * @throws IllegalArgumentException If argument is invalid.
     */
    ChunkHandle openRead(String chunkName) throws ChunkStorageException, IllegalArgumentException;

    /**
     * Opens storage object for Write (or modifications).
     *
     * @param chunkName String name of the storage object to write to or modify.
     * @return ChunkHandle A writable handle for the given chunk.
     * @throws ChunkStorageException Throws ChunkStorageException in case of I/O related exceptions.
     * @throws IllegalArgumentException If argument is invalid.
     */
    ChunkHandle openWrite(String chunkName) throws ChunkStorageException, IllegalArgumentException;

    /**
     * Retrieves the ChunkInfo for given name.
     *
     * @param chunkName String name of the storage object to read from.
     * @return ChunkInfo Information about the given chunk.
     * @throws ChunkStorageException Throws ChunkStorageException in case of I/O related exceptions.
     * @throws IllegalArgumentException If argument is invalid.
     */
    ChunkInfo getInfo(String chunkName) throws ChunkStorageException, IllegalArgumentException;

    /**
     * Reads a range of bytes from the underlying storage object.
     *
     * @param handle ChunkHandle of the storage object to read from.
     * @param fromOffset Offset in the file from which to start reading.
     * @param length Number of bytes to read.
     * @param buffer Byte buffer to which data is copied.
     * @param bufferOffset Offset in the buffer at which to start copying read data.
     * @return int Number of bytes read.
     * @throws ChunkStorageException Throws ChunkStorageException in case of I/O related exceptions.
     * @throws IllegalArgumentException If argument is invalid.
     * @throws NullPointerException  If the parameter is null.
     * @throws IndexOutOfBoundsException If the index is out of bounds.
     */
    int read(ChunkHandle handle, long fromOffset, int length, byte[] buffer, int bufferOffset) throws ChunkStorageException, NullPointerException, IndexOutOfBoundsException;

    /**
     * Writes the given data to the underlying storage object.
     *
     * @param handle ChunkHandle of the storage object to write to.
     * @param offset Offset in the file to start writing.
     * @param length Number of bytes to write.
     * @param data An InputStream representing the data to write.
     * @return int Number of bytes written.
     * @throws ChunkStorageException Throws ChunkStorageException in case of I/O related exceptions.
     */
    int write(ChunkHandle handle, long offset, int length, InputStream data) throws ChunkStorageException;

    /**
     * Concatenates two or more chunks using native facility. The first chunk is concatenated to.
     *
     * @param chunks Array of ConcatArgument objects containing info about existing chunks to be appended together.
     *               The chunks must be concatenated in the same sequence the arguments are provided.
     * @return int Number of bytes concatenated.
     * @throws ChunkStorageException Throws ChunkStorageException in case of I/O related exceptions.
     * @throws UnsupportedOperationException If this operation is not supported by this provider.
     */
    int concat(ConcatArgument[] chunks) throws ChunkStorageException, UnsupportedOperationException;

    /**
     * Truncates a given chunk.
     *
     * @param handle ChunkHandle of the storage object to truncate.
     * @param offset Offset to truncate to.
     * @return True if the object was truncated, false otherwise.
     * @throws ChunkStorageException Throws ChunkStorageException in case of I/O related exceptions.
     * @throws UnsupportedOperationException If this operation is not supported by this provider.
     */
    boolean truncate(ChunkHandle handle, long offset) throws ChunkStorageException, UnsupportedOperationException;

    /**
     * Makes chunk as either readonly or writable.
     *
     * @param handle ChunkHandle of the storage object.
     * @param isReadonly True if chunk is set to be readonly.
     * @return True if the operation was successful, false otherwise.
     * @throws ChunkStorageException Throws ChunkStorageException in case of I/O related exceptions.
     * @throws UnsupportedOperationException If this operation is not supported by this provider.
     */
    boolean setReadOnly(ChunkHandle handle, boolean isReadonly) throws ChunkStorageException, UnsupportedOperationException;
}
