package com.emc.logservice.storageabstraction;

import com.emc.logservice.contracts.SegmentProperties;

import java.io.InputStream;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;

/**
 * Defines an abstraction for Permanent Storage.
 */
public interface Storage extends AutoCloseable {
    /**
     * Creates a new StreamSegment in this Storage Layer.
     *
     * @param streamSegmentName The full name of the StreamSegment.
     * @param timeout           Timeout for the operation.
     * @return A CompletableFuture that, when completed, will indicate that the StreamSegment has been created (it will
     * contain a StreamSegmentInformation for a blank stream). If the operation failed, it will contain the cause of the
     * failure.
     */
    CompletableFuture<SegmentProperties> create(String streamSegmentName, Duration timeout);

    /**
     * Writes the given data to the StreamSegment.
     *
     * @param streamSegmentName The full name of the StreamSegment.
     * @param offset            The offset in the StreamSegment to write data at.
     * @param data              An InputStream representing the data to write.
     * @param length            The length of the InputStream.
     * @param timeout           Timeout for the operation.
     * @return A CompletableFuture that, when completed, will indicate the operation succeeded. If the operation failed,
     * it will contain the cause of the failure.
     */
    CompletableFuture<Void> write(String streamSegmentName, long offset, InputStream data, int length, Duration timeout);

    /**
     * Reads a range of bytes from the StreamSegment.
     *
     * @param streamSegmentName The full name of the StreamSegment.
     * @param offset            The offset in the StreamSegment to read data from.
     * @param buffer            A buffer to use for reading data.
     * @param bufferOffset      The offset in the buffer to start writing data to.
     * @param length            The number of bytes to read.
     * @param timeout           Timeout for the operation.
     * @return A CompletableFuture that, when completed, will contain the number of bytes read. If the operation failed,
     * it will contain the cause of the failure.
     * @throws ArrayIndexOutOfBoundsException If bufferOffset is invalid for the buffer.
     * @throws ArrayIndexOutOfBoundsException If bufferOffset + length is invalid for the buffer.
     */
    CompletableFuture<Integer> read(String streamSegmentName, long offset, byte[] buffer, int bufferOffset, int length, Duration timeout);

    /**
     * Seals a StreamSegment. No further modifications are allowed on the StreamSegment after this operation completes.
     *
     * @param streamSegmentName The full name of the StreamSegment.
     * @param timeout           Timeout for the operation.
     * @return A CompletableFuture that, when completed, will indicate that the operation completed (it will contain a
     * StreamSegmentInformation with the final state of the StreamSegment). If the operation failed, it will contain the
     * cause of the failure.
     */
    CompletableFuture<SegmentProperties> seal(String streamSegmentName, Duration timeout);

    /**
     * Gets current information about a StreamSegment.
     *
     * @param streamSegmentName The full name of the StreamSegment.
     * @param timeout           Timeout for the operation.
     * @return A CompletableFuture that, when completed, will contain the information requested about the StreamSegment.
     * If the operation failed, it will contain the cause of the failure.
     */
    CompletableFuture<SegmentProperties> getStreamSegmentInfo(String streamSegmentName, Duration timeout);

    /**
     * Concatenates two StreamSegments together.
     *
     * @param targetStreamSegmentName The full name of the Target StreamSegment. The Source StreamSegment will be
     *                                concatenated to this StreamSegment.
     * @param sourceStreamSegmentName The full name of the Source StreamSegment. This StreamSegment will be concatenated
     *                                to the Target StreamSegment.
     * @param timeout                 Timeout for the operation.
     * @return A CompletableFuture that, when completed, will indicate the operation succeeded. If the operation failed,
     * it will contain the cause of the failure.
     */
    CompletableFuture<Void> concat(String targetStreamSegmentName, String sourceStreamSegmentName, Duration timeout);

    /**
     * Deletes a StreamSegment.
     *
     * @param streamSegmentName The full name of the StreamSegment.
     * @param timeout           Timeout for the operation.
     * @return A CompletableFuture that, when completed, will indicate the operation succeeded. If the operation failed,
     * it will contain the cause of the failure.
     */
    CompletableFuture<Void> delete(String streamSegmentName, Duration timeout);

    @Override
    void close();
}
