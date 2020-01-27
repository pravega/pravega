/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.segment.impl;

import java.nio.ByteBuffer;

/**
 * Defines a ConditionalOutputStream for a segment.
 * Allows data to be conditionally appended to the end of the segment
 */
public interface ConditionalOutputStream extends AutoCloseable {

    /**
     * Returns the scoped name of the segment associated to this output stream.
     *
     * @return The name of the segment associated to this output stream.
     */
    public String getScopedSegmentName();
    
    /**
     * Writes the provided data to the SegmentOutputStream. The data will be written only if the
     * SegmentOutputStream's writeOffset is currently expectedOffset.
     * 
     * This is a synchronous and blocking method.
     * 
     * @param data The data to be added to the segment.
     * @param expectedOffset The location in the segment that the data will be written at.
     * @return true if the data was appended, false if the offset was not the expected value.
     * @throws SegmentSealedException If the segment is closed for modifications.
     */
    public boolean write(ByteBuffer data, long expectedOffset) throws SegmentSealedException;

    /**
     * Flushes and then closes the output stream.
     * Frees any resources associated with it.
     */
    @Override
    public void close();

}