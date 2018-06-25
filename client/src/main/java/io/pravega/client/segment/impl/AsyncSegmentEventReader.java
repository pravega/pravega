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
     * Reads the next event from the segment.
     *
     * If called while a read is outstanding, the corresponding future is cancelled.
     *
     * @param offset the offset to read the next event from.
     * @return a future containing the event data.
     */
    CompletableFuture<ByteBuffer> readAsync(long offset);

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
