/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.stream.impl.segment;

import com.emc.pravega.stream.Serializer;

import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;

/**
 * Defines an OutputStream for a segment.
 * Allows data to be appended to the end of the segment by calling {@link #write(ByteBuffer, CompletableFuture)}
 */
public interface SegmentOutputStream extends AutoCloseable {
    public static final int MAX_WRITE_SIZE = Serializer.MAX_EVENT_SIZE;

    /**
     * Writes the data from the given ByteBuffer to this SegmentOutputStream.
     *
     * @param buff Data to be written. Note this is limited to {@value #MAX_WRITE_SIZE} bytes.
     * @param onComplete future to be completed when data has been replicated and stored durably.
     * @throws SegmentSealedException If the segment is closed for modifications.
     */
    public abstract void write(ByteBuffer buff, CompletableFuture<Boolean> onComplete) throws SegmentSealedException;

    /**
     * Writes the provided data to the SegmentOutputStream if and only if the SegmentOutputStream is
     * currently of expectedLength.
     * 
     * @param expectedLength The length of all data written to the SegmentOutputStream for the write
     *            to be applied
     * @param buff the data to be written. Note this is limited to {@value #MAX_WRITE_SIZE} bytes.
     * @param onComplete future to be completed when the operation is complete. True if the data was
     *            written and false if it was not.
     * @throws SegmentSealedException If the segment is closed for modifications.
     */
    public abstract void conditionalWrite(long expectedLength, ByteBuffer buff, CompletableFuture<Boolean> onComplete)
            throws SegmentSealedException;

    /**
     * Flushes and then closes the output stream.
     * Frees any resources associated with it.
     *
     * @throws SegmentSealedException If the segment is closed for modifications.
     */
    @Override
    public abstract void close() throws SegmentSealedException;

    /**
     * Block on all writes that have not yet completed.
     *
     * @throws SegmentSealedException If the segment is closed for modifications.
     */
    public abstract void flush() throws SegmentSealedException;
}