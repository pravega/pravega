/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package com.emc.pravega.state.impl;

import com.emc.pravega.common.util.ReusableLatch;
import com.emc.pravega.state.InitialUpdate;
import com.emc.pravega.state.Revision;
import com.emc.pravega.state.Revisioned;
import com.emc.pravega.state.Update;
import com.emc.pravega.stream.Serializer;
import com.emc.pravega.stream.Stream;
import com.emc.pravega.stream.impl.StreamImpl;
import com.emc.pravega.stream.impl.segment.EndOfSegmentException;
import com.emc.pravega.stream.impl.segment.SegmentInputStream;
import com.emc.pravega.testcommon.Async;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

import lombok.Data;

public class SynchronizerTest {

    @Data
    private static class RevisionedImpl implements Revisioned {
        private final String scopedStreamName;
        private final Revision revision;
    }

    @Data
    private static class BlockingUpdate implements Update<RevisionedImpl>, InitialUpdate<RevisionedImpl> {
        private final ReusableLatch latch = new ReusableLatch(false);
        private final int num;

        @Override
        public RevisionedImpl create(Revision revision) {
            latch.awaitUninterruptibly();
            return new RevisionedImpl(null, new RevisionImpl(num, num));
        }

        @Override
        public RevisionedImpl applyTo(RevisionedImpl oldState, Revision newRevision) {
            latch.awaitUninterruptibly();
            return new RevisionedImpl(oldState.scopedStreamName, new RevisionImpl(num, num));
        }
    }

    private static class MockSegmentInputStream extends SegmentInputStream {
        private ByteBuffer[] results;
        private int pos = 0;
        private int visableLength = 0;

        @Override
        public void setOffset(long offset) {
            pos = (int) offset;
        }

        @Override
        public long getOffset() {
            return pos;
        }

        @Override
        public long fetchCurrentStreamLength() {
            return visableLength;
        }

        @Override
        public ByteBuffer read() throws EndOfSegmentException {
            if (pos >= visableLength) {
                throw new IllegalArgumentException();
            }
            return results[pos++].slice();
        }

        @Override
        public void close() {
        }
    }

    @Test(timeout = 20000)
    public void testLocking() throws EndOfSegmentException {
        String streamName = "streamName";
        Stream stream = new StreamImpl("scope", streamName, null);

        BlockingUpdate[] blocking = new BlockingUpdate[] { new BlockingUpdate(1), new BlockingUpdate(2),
                new BlockingUpdate(3), new BlockingUpdate(4) };
        Serializer<BlockingUpdate> serializer = new Serializer<BlockingUpdate>() {
            AtomicInteger count = new AtomicInteger(0);

            @Override
            public ByteBuffer serialize(BlockingUpdate value) {
                ByteBuffer result = ByteBuffer.allocate(4).putInt(count.getAndIncrement());
                result.rewind();
                return result;
            }

            @Override
            public BlockingUpdate deserialize(ByteBuffer serializedValue) {
                int index = serializedValue.getInt();
                return blocking[index];
            }
        };
        MockSegmentInputStream in = new MockSegmentInputStream();
        SynchronizerImpl<RevisionedImpl, BlockingUpdate, BlockingUpdate> sync = new SynchronizerImpl<>(stream,
                in,
                null,
                serializer,
                serializer);
        in.results = new ByteBuffer[] { sync.encodeInit(blocking[0]),
                sync.encodeUpdate(Collections.singletonList(blocking[1])),
                sync.encodeUpdate(Collections.singletonList(blocking[2])),
                sync.encodeUpdate(Collections.singletonList(blocking[3])), };
        in.visableLength = 2;
        blocking[0].latch.release();
        blocking[1].latch.release();
        RevisionedImpl state1 = sync.getLatestState();
        assertEquals(new RevisionImpl(2, 2), state1.getRevision());

        in.visableLength = 3;
        RevisionedImpl state2 = Async.testBlocking(() -> {
            return sync.getLatestState(state1);
        }, () -> blocking[2].latch.release());
        assertEquals(new RevisionImpl(3, 3), state2.getRevision());

        in.visableLength = 4;
        RevisionedImpl state3 = Async.testBlocking(() -> {
            return sync.getLatestState(state2);
        }, () -> {
            in.visableLength = 3;
            sync.getLatestState();
            blocking[3].latch.release();
        });
        assertEquals(new RevisionImpl(4, 4), state3.getRevision());
    }
}
