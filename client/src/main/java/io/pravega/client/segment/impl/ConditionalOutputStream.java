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

import io.pravega.client.stream.impl.PendingEvent;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;

/**
 * Defines an ConditionalOutputStream for a segment.
 * Allows data to be conditionally appended to the end of the segment
 */
public interface ConditionalOutputStream extends AutoCloseable {

    /**
     * Returns the name of the segment associated to this output stream.
     *
     * @return The name of the segment associated to this output stream.
     */
    public abstract String getSegmentName();
    
    /**
     * Writes the provided data to the SegmentOutputStream. If
     * {@link PendingEvent#getExpectedOffset()} the data will be written only if the
     * SegmentOutputStream is currently of expectedLength.
     * 
     * The associated callback will be invoked when the operation is complete.
     * 
     * @param event The event to be added to the segment.
     */
    public abstract CompletableFuture<Boolean> write(ByteBuffer data, long expectedOffset);

    /**
     * Flushes and then closes the output stream.
     * Frees any resources associated with it.
     * @throws SegmentSealedException If the segment is closed for modifications.
     */
    @Override
    public abstract void close() throws SegmentSealedException;

}