/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import com.emc.pravega.common.util.ReusableLatch;
import com.emc.pravega.state.InitialUpdate;
import com.emc.pravega.state.Revision;
import com.emc.pravega.state.Revisioned;
import com.emc.pravega.state.RevisionedStreamClient;
import com.emc.pravega.state.StateSynchronizer;
import com.emc.pravega.state.SynchronizerConfig;
import com.emc.pravega.state.Update;
import com.emc.pravega.stream.Segment;
import com.emc.pravega.stream.impl.JavaSerializer;
import com.emc.pravega.stream.impl.segment.EndOfSegmentException;
import com.emc.pravega.stream.mock.MockClientFactory;
import com.emc.pravega.stream.mock.MockSegmentStreamFactory;
import com.emc.pravega.testcommon.Async;

import java.io.Serializable;
import java.util.AbstractMap;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang.NotImplementedException;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

import lombok.Cleanup;
import lombok.Data;

public class SynchronizerTest {

    @Data
    private static class RevisionedImpl implements Revisioned {
        private final String scopedStreamName;
        private final Revision revision;
    }
    
    @Data
    private static class NormalUpdate implements Update<RevisionedImpl>, InitialUpdate<RevisionedImpl>, Serializable {
        @Override
        public RevisionedImpl create(String scopedStreamName, Revision revision) {
            return new RevisionedImpl(scopedStreamName, revision);
        }

        @Override
        public RevisionedImpl applyTo(RevisionedImpl oldState, Revision newRevision) {
            return new RevisionedImpl(oldState.getScopedStreamName(), newRevision);
        }
    }

    @Data
    private static class BlockingUpdate implements Update<RevisionedImpl>, InitialUpdate<RevisionedImpl> {
        private final ReusableLatch latch = new ReusableLatch(false);
        private final int num;

        @Override
        public RevisionedImpl create(String scopedSteamName, Revision revision) {
            latch.awaitUninterruptibly();
            return new RevisionedImpl(scopedSteamName, new RevisionImpl(revision.asImpl().getSegment(), num, num));
        }

        @Override
        public RevisionedImpl applyTo(RevisionedImpl oldState, Revision newRevision) {
            latch.awaitUninterruptibly();
            return new RevisionedImpl(oldState.scopedStreamName,
                    new RevisionImpl(newRevision.asImpl().getSegment(), num, num));
        }
    }

    private static class MockRevisionedStreamClient implements RevisionedStreamClient<UpdateOrInit<RevisionedImpl>> {
        private Segment segment;
        private BlockingUpdate init;
        private BlockingUpdate[] updates;
        private int visableLength = 0;

        @Override
        public Iterator<Entry<Revision, UpdateOrInit<RevisionedImpl>>> readFrom(
                Revision start) {
            return new Iterator<Entry<Revision, UpdateOrInit<RevisionedImpl>>>() {
                private int pos = start.asImpl().getEventAtOffset();

                @Override
                public Entry<Revision, UpdateOrInit<RevisionedImpl>> next() {
                    UpdateOrInit<RevisionedImpl> value;
                    RevisionImpl revision = new RevisionImpl(segment, pos, pos);
                    if (pos == 0) {
                        value = new UpdateOrInit<>(init);
                    } else {
                        value = new UpdateOrInit<>(Collections.singletonList(updates[pos - 1]));
                    }
                    pos++;
                    return new AbstractMap.SimpleImmutableEntry<>(revision, value);
                }

                @Override
                public boolean hasNext() {
                    return pos <= visableLength;
                }
            };
        }

        @Override
        public Revision writeConditionally(Revision latestRevision, UpdateOrInit<RevisionedImpl> value) {
            throw new NotImplementedException();
        }

        @Override
        public void writeUnconditionally(UpdateOrInit<RevisionedImpl> value) {
            throw new NotImplementedException();
        }

        @Override
        public Revision fetchRevision() {
            return new RevisionImpl(segment, visableLength, visableLength);
        }

    }

    @Test(timeout = 20000)
    public void testLocking() throws EndOfSegmentException {
        String streamName = "streamName";
        String scope = "scope";
        Segment segment = new Segment(scope, streamName, 0);

        BlockingUpdate[] updates = new BlockingUpdate[] { new BlockingUpdate(1), new BlockingUpdate(2),
                new BlockingUpdate(3), new BlockingUpdate(4) };

        MockRevisionedStreamClient client = new MockRevisionedStreamClient();
        client.segment = segment;
        StateSynchronizerImpl<RevisionedImpl> sync = new StateSynchronizerImpl<RevisionedImpl>(segment, client);
        client.init = new BlockingUpdate(0);
        client.updates = updates;
        client.visableLength = 2;
        client.init.latch.release();
        updates[0].latch.release();
        updates[1].latch.release();
        sync.fetchUpdates();
        RevisionedImpl state1 = sync.getState();
        assertEquals(new RevisionImpl(segment, 2, 2), state1.getRevision());

        client.visableLength = 3;
        Async.testBlocking(() -> {
            sync.fetchUpdates();
        }, () -> updates[2].latch.release());
        RevisionedImpl state2 = sync.getState();
        assertEquals(new RevisionImpl(segment, 3, 3), state2.getRevision());

        client.visableLength = 4;
        Async.testBlocking(() -> {
            sync.fetchUpdates();
        }, () -> {
            client.visableLength = 3;
            sync.getState();
            updates[3].latch.release();
        });
        RevisionedImpl state3 = sync.getState();
        assertEquals(new RevisionImpl(segment, 4, 4), state3.getRevision());
    }
    
    @Test(timeout = 20000)
    public void testCompaction() throws EndOfSegmentException {
        String streamName = "streamName";
        String scope = "scope";
        
        MockSegmentStreamFactory ioFactory = new MockSegmentStreamFactory();
        @Cleanup
        MockClientFactory clientFactory = new MockClientFactory(scope, ioFactory);

        StateSynchronizer<RevisionedImpl> sync = clientFactory.createStateSynchronizer(streamName,
                                                                                       new JavaSerializer<>(),
                                                                                       new JavaSerializer<>(),
                                                                                       new SynchronizerConfig(null, null));
        AtomicInteger callCount = new AtomicInteger(0);
        sync.initialize(new NormalUpdate());
        sync.updateState(state -> {
            callCount.incrementAndGet();
            return Collections.singletonList(new NormalUpdate());
        });
        assertEquals(1, callCount.get());
        sync.updateState(state -> {
            callCount.incrementAndGet();
            return Collections.singletonList(new NormalUpdate());
        });
        assertEquals(2, callCount.get());
        sync.compact(state -> {
            callCount.incrementAndGet();
            return new NormalUpdate();
        });
        assertEquals(3, callCount.get());
        sync.updateState(s -> {
            callCount.incrementAndGet();
            return Collections.singletonList(new NormalUpdate());
        });
        assertEquals(5, callCount.get());
        sync.compact(state -> {
            callCount.incrementAndGet();
            return new NormalUpdate();
        });
        assertEquals(6, callCount.get());
    }

}
