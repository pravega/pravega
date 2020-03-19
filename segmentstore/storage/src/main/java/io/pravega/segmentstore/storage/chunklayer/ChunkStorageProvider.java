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

import java.io.IOException;
import java.io.InputStream;

/**
 * Defines an abstraction for Permanent Storage.
 * Note: not all operations defined here are needed.
 */
public interface ChunkStorageProvider extends AutoCloseable {
    /**
     * Gets a value indicating whether this Storage implementation supports truncate operation on underlying storage object.
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
     * Gets a value indicating whether this Storage implementation supports merge operation on underlying storage object.
     *
     * @return True or false.
     */
    boolean supportsConcat();

    /**
     * Determines whether named file/object exists in underlying storage.
     *
     * @param chunkName Name of the storage object to check.
     * @return True if the object exists, false otherwise.
     * @throws IOException Throws IOException in case of I/O related exceptions.
     */
    boolean exists(String chunkName) throws IOException;

    /**
     * Creates a new file.
     *
     * @param chunkName String name of the storage object to create.
     * @return ChunkHandle A writable handle for the recently created chunk.
     * @throws IOException Throws IOException in case of I/O related exceptions.
     */
    ChunkHandle create(String chunkName) throws IOException;

    /**
     * Deletes a file.
     *
     * @param handle ChunkHandle of the storage object to delete.
     * @return True if the object was deleted, false otherwise.
     * @throws IOException Throws IOException in case of I/O related exceptions.
     */
    boolean delete(ChunkHandle handle) throws IOException;

    /**
     * Opens storage object for Read.
     *
     * @param chunkName String name of the storage object to read from.
     * @return ChunkHandle A readable handle for the given chunk.
     * @throws IOException Throws IOException in case of I/O related exceptions.
     * @throws IllegalArgumentException If argument is invalid.
     */
    ChunkHandle openRead(String chunkName) throws IOException, IllegalArgumentException;

    /**
     * Opens storage object for Write (or modifications).
     *
     * @param chunkName String name of the storage object to write to or modify.
     * @return ChunkHandle A writable handle for the given chunk.
     * @throws IOException Throws IOException in case of I/O related exceptions.
     * @throws IllegalArgumentException If argument is invalid.
     */
    ChunkHandle openWrite(String chunkName) throws IOException, IllegalArgumentException;

    /**
     * Retrieves the ChunkInfo for given name.
     *
     * @param chunkName String name of the storage object to read from.
     * @return ChunkInfo Information about the given chunk.
     * @throws IOException Throws IOException in case of I/O related exceptions.
     * @throws IllegalArgumentException If argument is invalid.
     */
    ChunkInfo getInfo(String chunkName) throws IOException, IllegalArgumentException;

    /**
     * Reads a range of bytes from the underlying storage object.
     *
     * @param handle ChunkHandle of the storage object to read from.
     * @param fromOffset Offset in the file from which to start reading.
     * @param length Number of bytes to read.
     * @param buffer Byte buffer to which data is copied.
     * @param bufferOffset Offset in the buffer at which to start copying read data.
     * @return int Number of bytes read.
     * @throws IOException Throws IOException in case of I/O related exceptions.
     * @throws IllegalArgumentException If argument is invalid.
     * @throws NullPointerException  If the parameter is null.
     * @throws IndexOutOfBoundsException If the index is out of bounds.
     */
    int read(ChunkHandle handle, long fromOffset, int length, byte[] buffer, int bufferOffset) throws IOException, NullPointerException, IndexOutOfBoundsException;


    /**
     * Writes the given data to the underlying storage object.
     *
     * @param handle ChunkHandle of the storage object to write to.
     * @param offset Offset in the file to start writing.
     * @param length Number of bytes to write.
     * @param data An InputStream representing the data to write.
     * @return int Number of bytes written.
     * @throws IOException Throws IOException in case of I/O related exceptions.
     */
    int write(ChunkHandle handle, long offset, int length, InputStream data) throws IOException;

    /**
     * Concatenates two or more chunks.
     *
     * @param target String Name of the storage object to concat to.
     * @param sources Array of ChunkHandle to existing chunks to be appended to the target. The chunks are appended in the same sequence the names are provided.
     * @return int Number of bytes concatenated.
     * @throws IOException Throws IOException in case of I/O related exceptions.
     * @throws UnsupportedOperationException If this operation is not supported by this provider.
     */
    int concat(ChunkHandle target, ChunkHandle... sources) throws IOException, UnsupportedOperationException;

    /**
     * Truncates a given chunk.
     *
     * @param handle ChunkHandle of the storage object to truncate.
     * @param offset Offset to truncate to.
     * @return True if the object was truncated, false otherwise.
     * @throws IOException Throws IOException in case of I/O related exceptions.
     * @throws UnsupportedOperationException If this operation is not supported by this provider.
     */
    boolean truncate(ChunkHandle handle, long offset) throws IOException, UnsupportedOperationException;

    /**
     * Sets readonly attribute for the chunk.
     *
     * @param handle ChunkHandle of the storage object.
     * @param isReadonly True if chunk is set to be readonly.
     * @return True if the operation was successful, false otherwise.
     * @throws IOException Throws IOException in case of I/O related exceptions.
     * @throws UnsupportedOperationException If this operation is not supported by this provider.
     */
    boolean setReadonly(ChunkHandle handle, boolean isReadonly) throws IOException, UnsupportedOperationException;
}
