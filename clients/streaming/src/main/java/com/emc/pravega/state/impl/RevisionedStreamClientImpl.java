/**
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
package com.emc.pravega.state.impl;

import com.emc.pravega.common.concurrent.FutureHelpers;
import com.emc.pravega.common.netty.WireCommands;
import com.emc.pravega.state.Revision;
import com.emc.pravega.state.RevisionedStreamClient;
import com.emc.pravega.stream.Segment;
import com.emc.pravega.stream.Serializer;
import com.emc.pravega.stream.impl.segment.EndOfSegmentException;
import com.emc.pravega.stream.impl.segment.SegmentInputStream;
import com.emc.pravega.stream.impl.segment.SegmentOutputStream;
import com.emc.pravega.stream.impl.segment.SegmentSealedException;

import java.nio.ByteBuffer;
import java.util.AbstractMap;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.concurrent.CompletableFuture;

import javax.annotation.concurrent.GuardedBy;

import lombok.AllArgsConstructor;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class RevisionedStreamClientImpl<T> implements RevisionedStreamClient<T> {

    private final Segment segment;
    @GuardedBy("lock")
    private final SegmentInputStream in;
    @GuardedBy("lock")
    private final SegmentOutputStream out;
    private final Serializer<T> serializer;
    private final Object lock = new Object();

    @Override
    public Revision conditionallyWrite(Revision latestRevision, T value) {
        CompletableFuture<Boolean> wasWritten = new CompletableFuture<>();
        long offset = latestRevision.asImpl().getOffsetInSegment();
        ByteBuffer serialized = serializer.serialize(value);
        int size = serialized.remaining();
        try {
            synchronized (lock) {
                out.conditionalWrite(offset, serialized, wasWritten);
            }
        } catch (SegmentSealedException e) {
            throw new CorruptedStateException("Unexpected end of segment ", e);
        }
        if (FutureHelpers.getAndHandleExceptions(wasWritten, RuntimeException::new)) {
            return new RevisionImpl(segment, getNewOffset(offset, size), 0);
        } else {
            return null;
        }
    }

    private static final long getNewOffset(long initial, int size) {
        return initial + size + WireCommands.TYPE_PLUS_LENGTH_SIZE;
    }

    @Override
    public void unconditionallyWrite(T value) {
        CompletableFuture<Boolean> wasWritten = new CompletableFuture<>();
        ByteBuffer serialized = serializer.serialize(value);
        try {
            synchronized (lock) {
                out.write(serialized, wasWritten);
            }
        } catch (SegmentSealedException e) {
            throw new CorruptedStateException("Unexpected end of segment ", e);
        }
        FutureHelpers.getAndHandleExceptions(wasWritten, RuntimeException::new);
    }

    @Override
    public Iterator<Entry<Revision, T>> readFrom(Revision start) {
        synchronized (lock) {
            long startOffset = start.asImpl().getOffsetInSegment();
            long endOffset = in.fetchCurrentStreamLength();
            return new StreamIterator(startOffset, endOffset);
        }
    }

    @AllArgsConstructor
    private class StreamIterator implements Iterator<Entry<Revision, T>> {
        long offset;
        final long endOffset;

        @Override
        public boolean hasNext() {
            return offset < endOffset;
        }

        @Override
        public Entry<Revision, T> next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            Revision revision;
            ByteBuffer data;
            synchronized (lock) {
                in.setOffset(offset);
                try {
                    data = in.read();
                } catch (EndOfSegmentException e) {
                    throw new IllegalStateException(
                            "SegmentInputStream: " + in + " shrunk from its origional length: " + endOffset);
                }
                offset = in.getOffset();
                revision = new RevisionImpl(segment, offset, 0);
            }
            return new AbstractMap.SimpleImmutableEntry<>(revision, serializer.deserialize(data));
        }
    }

}
