/**
 *  Copyright (c) 2017 Dell Inc. or its subsidiaries. All Rights Reserved
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *          http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package com.emc.pravega.stream.impl;

import java.nio.ByteBuffer;

import javax.annotation.concurrent.GuardedBy;

import com.emc.pravega.stream.Segment;
import com.emc.pravega.stream.Serializer;
import com.emc.pravega.stream.impl.segment.EndOfSegmentException;
import com.emc.pravega.stream.impl.segment.SegmentInputStream;

/**
 * Reads items from the segmentInputStream, deserializes them with the Serializer and returns them to the caller.
 */
public class SegmentReaderImpl<Type> implements SegmentReader<Type> {

    private final Segment segmentId;
    @GuardedBy("in")
    private final SegmentInputStream in;
    private final Serializer<Type> deserializer;

    SegmentReaderImpl(Segment segmentId, SegmentInputStream in, Serializer<Type> deserializer) {
        this.segmentId = segmentId;
        this.in = in;
        this.deserializer = deserializer;
    }

    @Override
    public Type getNextEvent(long timeout) throws EndOfSegmentException {
        ByteBuffer buffer;
        synchronized (in) { 
            buffer = in.read();
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
    public Segment getSegmentId() {
        return segmentId;
    }
    
    @Override
    public boolean canReadWithoutBlocking() {
        synchronized (in) {
            return in.canReadWithoutBlocking();
        }
    }
}
