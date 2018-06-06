/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.segment.impl;

import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;

/**
 * Allows for decoding the events from a Segment asynchronously.
 */
public interface AsyncSegmentEventReader {

    /**
     * Gets the associated segment information.
     */
    Segment getSegmentId();

    /**
     * Sets the offset for reading from the segment and cancels any outstanding read.
     *
     * @param offset The offset to set.
     */
    void setOffset(long offset);

    /**
     * Gets the current offset.
     *
     * @return The current offset.
     */
    long getOffset();

    /**
     * Sets the end offset for reading from the segment (exclusive).
     *
     * @param offset The offset to use.
     */
    void setEndOffset(long offset);

    /**
     * Gets the end offset.
     *
     * @return The end offset.
     */
    long getEndOffset();

    /**
     * Reads the next event from the segment.
     *
     * The offset is advanced iff the future completes successfully.  If called while a read is outstanding,
     * cancels that read and re-reads the same data.
     *
     * NOTE: a thread-safe implementation of {@link AsyncSegmentEventReader} may acquire a lock that is taken
     * upon calls to {@code readAsync}, and may re-acquire the lock upon completion of the future.  To avoid
     * a deadlock, be careful about acquiring locks in the completion handler of the returned future.
     *
     * @return a future containing the event data.
     * @throws EndOfSegmentException if the configured end offset was reached.
     */
    CompletableFuture<ByteBuffer> readAsync() throws EndOfSegmentException;

    /**
     * Closes this reader. No further methods may be called after close.
     * This will free any resources associated with the reader.
     */
    void close();

    /**
     * Indicates whether the reader is closed.
     */
    boolean isClosed();
}
