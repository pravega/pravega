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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.pravega.common.MathHelpers;
import io.pravega.shared.protocol.netty.WireCommands;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

class IoBuffer {
    private int maxBufferSize = WireCommands.MAX_WIRECOMMAND_SIZE;
    private ByteBuffer buffer = null;
    
    private ByteBuf sliceOut(int size) {
        ByteBuf result = Unpooled.wrappedBuffer(buffer.array(), buffer.arrayOffset() + buffer.position(), size);
        buffer.position(buffer.position() + size);
        if (!buffer.hasRemaining()) {
            buffer = null;
        }
        return result;
    }
    
    public ByteBuf getBuffOfSize(InputStream in, int size) throws IOException {
        if (size > maxBufferSize) {
            throw new IllegalArgumentException("Requested buffer size " + size + " is larger than maximum allowed"
                    + maxBufferSize);
        }
        if (buffer == null) {
            int bufferSize = MathHelpers.minMax(in.available(), size, maxBufferSize);
            byte[] newBuffer = new byte[bufferSize];
            int read = in.read(newBuffer);
            if (read <= -1) {
                throw new EOFException();
            }
            buffer = ByteBuffer.wrap(newBuffer, 0, read); 
        } 

        if (buffer.remaining() >= size) {
            return sliceOut(size);
        } else {
            int firstSize = buffer.remaining();
            ByteBuf first = sliceOut(firstSize);
            assert buffer == null; //Should have been fully sliced out
            byte[] remaining = new byte[size - firstSize];
            for (int offset = 0; offset < remaining.length;) {
                int read = in.read(remaining, offset, remaining.length - offset);
                if (read <= -1) {
                    throw new EOFException();
                }
                offset += read;
            }
            return Unpooled.wrappedBuffer(first, Unpooled.wrappedBuffer(remaining));
        }
    }
    
}