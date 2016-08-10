/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.emc.pravega.stream.impl;

import java.nio.ByteBuffer;

import com.emc.pravega.stream.SegmentId;
import com.emc.pravega.stream.Serializer;
import com.emc.pravega.stream.segment.EndOfSegmentException;
import com.emc.pravega.stream.segment.SegmentInputStream;

public class SegmentConsumerImpl<Type> implements SegmentConsumer<Type> {

    private final SegmentId segmentId;
    private final SegmentInputStream in;
    private final Serializer<Type> deserializer;

    SegmentConsumerImpl(SegmentId segmentId, SegmentInputStream in, Serializer<Type> deserializer) {
        this.segmentId = segmentId;
        this.in = in;
        this.deserializer = deserializer;
    }

    @Override
    public Type getNextEvent(long timeout) throws EndOfSegmentException {
        ByteBuffer buffer;
        synchronized (in) { // TODO: This implementation sucks. It is unnecessary allocating 2 byte
                            // arrays. This could be made less redundant with lower level code.
            buffer = ByteBuffer.allocate(4);
            in.read(buffer);
            int length = buffer.getInt();
            buffer = ByteBuffer.allocate(length);
            in.read(buffer);
        }
        return deserializer.deserialize(buffer);
    }

    @Override
    public long getOffset() {
        synchronized (in) {
            return in.getOffset();
        }
    }

    @Override
    public void setOffset(long offset) {
        synchronized (in) {
            in.setOffset(offset);
        }
    }

    @Override
    public void close() {
        synchronized (in) {
            in.close();
        }
    }

    @Override
    public SegmentId getLogId() {
        return segmentId;
    }
}
