/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.connection.impl;

import com.google.common.annotations.VisibleForTesting;
import io.pravega.common.MathHelpers;
import io.pravega.common.util.BufferView;
import io.pravega.common.util.ByteArraySegment;
import io.pravega.shared.protocol.WireCommands;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import javax.annotation.concurrent.NotThreadSafe;
import lombok.AccessLevel;
import lombok.Getter;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * This is a utility class for repeatedly reading data from an input stream, that tries to buffer data in a way that minimizes allocations.
 * It is intended that {@link #getBuffOfSize(InputStream, int)} is called in a loop to read chunks of data from the provided input stream.
 * This class does not support reading chunks larger than {@link WireCommands#MAX_WIRECOMMAND_SIZE}.
 */
@NotThreadSafe
class IoBuffer {
    private final int maxBufferSize = WireCommands.MAX_WIRECOMMAND_SIZE;
    @VisibleForTesting
    @Getter(AccessLevel.PACKAGE)
    private ByteArraySegment buffer = null;
    
    private ByteArraySegment sliceOut(int size) {
        ByteArraySegment result = buffer.slice(0, size);
        if (buffer.getLength() > size) {
            buffer = new ByteArraySegment(buffer.array(), buffer.arrayOffset() + size, buffer.getLength());            
        } else if (buffer.getLength() == size) {
            buffer = null;
        } else {
            throw new IllegalStateException("Attempted to read more from the buffer than was available.");
        }
        return result;
    }
    
    /**
     * Obtain a ByteBuff of size `size` by reading data from the provided input stream or from this buffer if the data is already available.
     */
    public BufferView getBuffOfSize(InputStream in, int size) throws IOException {
        checkArgument(size <= maxBufferSize, "Requested buffer size {} is larger than max allowd {}", size, maxBufferSize);
        if (size == 0) {
            // Technically this should not need to be special cased per the Javadoc of InputStrem, 
            // but ByteArrayInputStream has a bug that makes this needed.
            return BufferView.empty();
        }
        if (buffer == null) {
            int bufferSize = MathHelpers.minMax(in.available(), size, maxBufferSize);
            byte[] newBuffer = new byte[bufferSize];
            int read = in.read(newBuffer);
            if (read <= -1) {
                throw new EOFException();
            }
            buffer = new ByteArraySegment(newBuffer, 0, read); 
        } 

        if (buffer.getLength() >= size) {
            return sliceOut(size);
        } else {
            int firstSize = buffer.getLength();
            ByteArraySegment first = sliceOut(firstSize);
            assert buffer == null; //Should have been fully sliced out
            byte[] remaining = new byte[size - firstSize];
            for (int offset = 0; offset < remaining.length;) {
                int read = in.read(remaining, offset, remaining.length - offset);
                if (read <= -1) {
                    throw new EOFException();
                }
                offset += read;
            }
            return BufferView.wrap(first, new ByteArraySegment(remaining));
        }
    }
    
}