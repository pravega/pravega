/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.state.impl;

import io.pravega.client.segment.impl.EndOfSegmentException;
import io.pravega.client.segment.impl.Segment;
import io.pravega.client.segment.impl.SegmentAttribute;
import io.pravega.client.state.InitialUpdate;
import io.pravega.client.state.Revision;
import io.pravega.client.state.Revisioned;
import io.pravega.client.state.RevisionedStreamClient;
import io.pravega.client.state.StateSynchronizer;
import io.pravega.client.state.SynchronizerConfig;
import io.pravega.client.state.Update;
import io.pravega.client.state.examples.SetSynchronizer;
import io.pravega.client.stream.impl.ByteArraySerializer;
import io.pravega.client.stream.impl.JavaSerializer;
import io.pravega.client.stream.mock.MockClientFactory;
import io.pravega.client.stream.mock.MockSegmentStreamFactory;
import io.pravega.common.util.ReusableLatch;
import io.pravega.test.common.AssertExtensions;
import java.io.Serializable;
import java.util.AbstractMap;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import lombok.Cleanup;
import lombok.Data;
import org.apache.commons.lang3.NotImplementedException;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class SynchronizerTest {

    @Data
    private static class RevisionedImpl implements Revisioned {
        private final String scopedStreamName;
        private final Revision revision;
        private final String value;
    }
    
    @Data
    private static class RegularUpdate implements Update<RevisionedImpl>, InitialUpdate<RevisionedImpl>, Serializable {
        private static final long serialVersionUID = 1L;
        private final String value;
        @Override
        public RevisionedImpl create(String scopedStreamName, Revision revision) {
            return new RevisionedImpl(scopedStreamName, revision, value);
        }

        @Override
        public RevisionedImpl applyTo(RevisionedImpl oldState, Revision newRevision) {
            return new RevisionedImpl(oldState.getScopedStreamName(), newRevision, value);
        }
    }

    @Data
    private static class BlockingUpdate implements Update<RevisionedImpl>, InitialUpdate<RevisionedImpl> {
        private final ReusableLatch latch = new ReusableLatch(false);
        private final int num;

        @Override
        public RevisionedImpl create(String scopedSteamName, Revision revision) {
            latch.awaitUninterruptibly();
            return new RevisionedImpl(scopedSteamName, new RevisionImpl(revision.asImpl().getSegment(), num, num),
                                      String.valueOf(num));
        }

        @Override
        public RevisionedImpl applyTo(RevisionedImpl oldState, Revision newRevision) {
            latch.awaitUninterruptibly();
            return new RevisionedImpl(oldState.scopedStreamName,
                                      new RevisionImpl(newRevision.asImpl().getSegment(), num, num), "" + num);
        }
    }

    private static class MockRevisionedStreamClient implements RevisionedStreamClient<UpdateOrInit<RevisionedImpl>> {
        private Segment segment;
        private BlockingUpdate init;
        private BlockingUpdate[] updates;
        private int visableLength = 0;
        private final AtomicLong mark = new AtomicLong(SegmentAttribute.NULL_VALUE);

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
            throw new NotImplementedException("writeConditionally");
        }

        @Override
        public void writeUnconditionally(UpdateOrInit<RevisionedImpl> value) {
            throw new NotImplementedException("writeUnconditionally");
        }

        @Override
        public Revision fetchLatestRevision() {
            return new RevisionImpl(segment, visableLength, visableLength);
        }

        @Override
        public Revision getMark() {
            long location = mark.get();
            return location == SegmentAttribute.NULL_VALUE ? null : new RevisionImpl(segment, location, 0);
        }

        @Override
        public boolean compareAndSetMark(Revision expected, Revision newLocation) {
            long exp = expected == null ? SegmentAttribute.NULL_VALUE : expected.asImpl().getOffsetInSegment();
            long value = newLocation == null ? SegmentAttribute.NULL_VALUE : newLocation.asImpl().getOffsetInSegment();
            return mark.compareAndSet(exp, value);
        }

        @Override
        public Revision fetchOldestRevision() {
            return new RevisionImpl(segment, 0, 0);
        }

        @Override
        public void close() { 
        }

        @Override
        public void truncateToRevision(Revision newStart) {
            throw new NotImplementedException("truncateToRevision");
        }
    }

    @Test(timeout = 20000)
    public void testLocking() {
        String streamName = "streamName";
        String scope = "scope";
        Segment segment = new Segment(scope, streamName, 0);

        BlockingUpdate[] updates = new BlockingUpdate[] { new BlockingUpdate(1), new BlockingUpdate(2),
                new BlockingUpdate(3), new BlockingUpdate(4) };

        MockRevisionedStreamClient client = new MockRevisionedStreamClient();
        client.segment = segment;
        @Cleanup
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
        AssertExtensions.assertBlocks(() -> {
            sync.fetchUpdates();
        }, () -> updates[2].latch.release());
        RevisionedImpl state2 = sync.getState();
        assertEquals(new RevisionImpl(segment, 3, 3), state2.getRevision());

        client.visableLength = 4;
        AssertExtensions.assertBlocks(() -> {
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
                                                                                       SynchronizerConfig.builder().build());
        assertEquals(0, sync.bytesWrittenSinceCompaction());
        AtomicInteger callCount = new AtomicInteger(0);
        sync.initialize(new RegularUpdate("a"));
        sync.updateState((state, updates) -> {
            callCount.incrementAndGet();
            updates.add(new RegularUpdate("b"));
        });
        assertEquals(sync.getState().getValue(), "b");
        assertEquals(1, callCount.get());
        long size = sync.bytesWrittenSinceCompaction();
        assertTrue(size > 0);
        sync.updateState((state, updates) -> {
            callCount.incrementAndGet();
            updates.add(new RegularUpdate("c"));
        });
        assertEquals(sync.getState().getValue(), "c");
        assertEquals(2, callCount.get());
        assertTrue(sync.bytesWrittenSinceCompaction() > size);
        sync.compact(state -> {
            callCount.incrementAndGet();
            return new RegularUpdate("c");
        });
        assertEquals(sync.getState().getValue(), "c");
        assertEquals(3, callCount.get());
        assertEquals(0, sync.bytesWrittenSinceCompaction());
        sync.updateState((state, updates) -> {
            callCount.incrementAndGet();
            updates.add(new RegularUpdate("e"));
        });
        assertEquals(sync.getState().getValue(), "e");
        assertEquals(5, callCount.get());
        assertEquals(size, sync.bytesWrittenSinceCompaction());
        sync.compact(state -> {
            callCount.incrementAndGet();
            return new RegularUpdate("e");
        });
        assertEquals(sync.getState().getValue(), "e");
        assertEquals(6, callCount.get());
        assertEquals(0, sync.bytesWrittenSinceCompaction());
    }

    @Test(timeout = 20000)
    public void testConsistency() {
        String streamName = "streamName";
        String scope = "scope";
        
        MockSegmentStreamFactory ioFactory = new MockSegmentStreamFactory();
        @Cleanup
        MockClientFactory clientFactory = new MockClientFactory(scope, ioFactory);
        @Cleanup
        StateSynchronizer<RevisionedImpl> syncA = clientFactory.createStateSynchronizer(streamName,
                                                                                        new JavaSerializer<>(),
                                                                                        new JavaSerializer<>(),
                                                                                        SynchronizerConfig.builder().build());
        @Cleanup
        StateSynchronizer<RevisionedImpl> syncB = clientFactory.createStateSynchronizer(streamName,
                                                                                        new JavaSerializer<>(),
                                                                                        new JavaSerializer<>(),
                                                                                        SynchronizerConfig.builder().build());
        syncA.initialize(new RegularUpdate("Foo"));
        AtomicInteger callCount = new AtomicInteger(0);
        syncB.updateState((state, updates) -> {
            callCount.incrementAndGet();
            updates.add(new RegularUpdate("Bar"));
        });
        assertEquals(1, callCount.get());
        assertEquals("Foo", syncA.getState().value);
        syncA.updateState((state, updates) -> {
            callCount.incrementAndGet();
            updates.add(new RegularUpdate("Baz"));
        });
        assertEquals(3, callCount.get());
        assertEquals("Baz", syncA.getState().value);
        syncB.fetchUpdates();
        assertEquals("Baz", syncA.getState().value);
        syncB.updateState((state, updates) -> {
            callCount.incrementAndGet();
            updates.add(new RegularUpdate("Bat"));
        });
        assertEquals(4, callCount.get());
        assertEquals("Baz", syncA.getState().value);
        syncA.fetchUpdates();
        assertEquals("Bat", syncA.getState().value);
    }
    
    @Test(timeout = 20000)
    public void testReturnValue() throws EndOfSegmentException {
        String streamName = "streamName";
        String scope = "scope";
        
        MockSegmentStreamFactory ioFactory = new MockSegmentStreamFactory();
        @Cleanup
        MockClientFactory clientFactory = new MockClientFactory(scope, ioFactory);
        StateSynchronizer<RevisionedImpl> sync = clientFactory.createStateSynchronizer(streamName,
                                                                                       new JavaSerializer<>(),
                                                                                       new JavaSerializer<>(),
                                                                                       SynchronizerConfig.builder().build());
        StateSynchronizer<RevisionedImpl> syncA = clientFactory.createStateSynchronizer(streamName,
                                                                                        new JavaSerializer<>(),
                                                                                        new JavaSerializer<>(),
                                                                                        SynchronizerConfig.builder().build());
        StateSynchronizer<RevisionedImpl> syncB = clientFactory.createStateSynchronizer(streamName,
                                                                                        new JavaSerializer<>(),
                                                                                        new JavaSerializer<>(),
                                                                                        SynchronizerConfig.builder().build());
        syncA.initialize(new RegularUpdate("Foo"));
        AtomicInteger callCount = new AtomicInteger(0);
        String previous = syncB.updateState((state, updates) -> {
            callCount.incrementAndGet();
            updates.add(new RegularUpdate("Bar"));
            return state.getValue();
        });
        assertEquals(previous, "Foo");
        assertEquals(1, callCount.get());
        assertEquals("Foo", syncA.getState().value);
        previous = syncA.updateState((state, updates) -> {
            callCount.incrementAndGet();
            updates.add(new RegularUpdate("Baz"));
            return state.getValue();
        });
        assertEquals(previous, "Bar");
        assertEquals(3, callCount.get());
        assertEquals("Baz", syncA.getState().value);
        syncB.fetchUpdates();
        assertEquals("Baz", syncA.getState().value);
        previous = syncB.updateState((state, updates) -> {
            callCount.incrementAndGet();
            updates.add(new RegularUpdate("Bat"));
            return state.getValue();
        });
        assertEquals(previous, "Baz");
        assertEquals(4, callCount.get());
        assertEquals("Baz", syncA.getState().value);
        syncA.fetchUpdates();
        assertEquals("Bat", syncA.getState().value);
    }
    
    @Test(timeout = 20000)
    public void testCompactionShrinksSet() throws EndOfSegmentException {
        String streamName = "testCompactionShrinksSet";
        String scope = "scope";
        
        MockSegmentStreamFactory ioFactory = new MockSegmentStreamFactory();
        @Cleanup
        MockClientFactory clientFactory = new MockClientFactory(scope, ioFactory);
        SetSynchronizer<String> set = SetSynchronizer.createNewSet(streamName, clientFactory);
        RevisionedStreamClient<byte[]> rsc = clientFactory.createRevisionedStreamClient(streamName, new ByteArraySerializer(),
                                                                                   SynchronizerConfig.builder().build());
        set.add("Foo");
        assertNull(rsc.getMark());
        set.add("Bar");
        assertNull(rsc.getMark());
        set.clear();
        assertNotNull(rsc.getMark());
        Iterator<?> iter = rsc.readFrom(rsc.getMark());
        assertTrue(iter.hasNext());
        iter.next();
        assertFalse(iter.hasNext());
        set.add("Foo2");
        assertNotNull(rsc.getMark());
        assertEquals(1, set.getCurrentSize());
    }

    @Test(timeout = 20000)
    public void testSetOperations() throws EndOfSegmentException {
        String streamName = "testCompactionShrinksSet";
        String scope = "scope";
        
        MockSegmentStreamFactory ioFactory = new MockSegmentStreamFactory();
        @Cleanup
        MockClientFactory clientFactory = new MockClientFactory(scope, ioFactory);
        SetSynchronizer<String> set = SetSynchronizer.createNewSet(streamName, clientFactory);
        SetSynchronizer<String> set2 = SetSynchronizer.createNewSet(streamName, clientFactory);
        assertEquals(0, set.getCurrentSize());
        set.add("Foo");
        set2.add("Bar");
        set.update();
        assertEquals(2, set.getCurrentSize());
        set.clear();
        assertEquals(0, set.getCurrentSize());
        set.add("Baz");
        assertEquals(2, set2.getCurrentSize());
        set2.remove("Bar");
        assertEquals(1, set2.getCurrentSize());
        set2.remove("Baz");
        assertEquals(0, set2.getCurrentSize());
        set.update();
        assertEquals(0, set.getCurrentSize());
        SetSynchronizer<String> set3 = SetSynchronizer.createNewSet(streamName, clientFactory);
        set3.update();
        assertEquals(0, set3.getCurrentSize());
    }

    @Test(timeout = 20000)
    public void testCompactWithTruncation() throws EndOfSegmentException {
        String streamName = "streamName";
        String scope = "scope";

        MockSegmentStreamFactory ioFactory = new MockSegmentStreamFactory();
        @Cleanup
        MockClientFactory clientFactoryA = new MockClientFactory(scope, ioFactory);
        @Cleanup
        MockClientFactory clientFactoryB = new MockClientFactory(scope, ioFactory);

        StateSynchronizer<RevisionedImpl> syncA = clientFactoryA.createStateSynchronizer(streamName,
                new JavaSerializer<>(),
                new JavaSerializer<>(),
                SynchronizerConfig.builder().build());

        StateSynchronizer<RevisionedImpl> syncB = clientFactoryB.createStateSynchronizer(streamName,
                new JavaSerializer<>(),
                new JavaSerializer<>(),
                SynchronizerConfig.builder().build());

        assertEquals(0, syncA.bytesWrittenSinceCompaction());
        assertEquals(0, syncB.bytesWrittenSinceCompaction());

        AtomicInteger callCount = new AtomicInteger(0);

        syncA.initialize(new RegularUpdate("a"));
        syncB.initialize(new RegularUpdate("b"));
        assertEquals("a", syncA.getState().getValue());
        assertEquals("a", syncB.getState().getValue());

        syncA.updateState((state, updates) -> {
            callCount.incrementAndGet();
            updates.add(new RegularUpdate("b"));
        });
        assertEquals(1, callCount.get());
        assertEquals("b", syncA.getState().getValue());
        syncB.fetchUpdates();
        assertEquals("b", syncB.getState().getValue());

        long size = syncA.bytesWrittenSinceCompaction();
        assertTrue(size > 0);

        syncA.updateState((state, updates) -> {
            callCount.incrementAndGet();
            updates.add(new RegularUpdate("c"));
        });
        assertEquals(2, callCount.get());
        assertEquals("c", syncA.getState().getValue());
        syncB.fetchUpdates();
        assertEquals("c", syncB.getState().getValue());
        assertTrue(syncA.bytesWrittenSinceCompaction() > size);

        syncA.updateState((state, updates) -> {
            callCount.incrementAndGet();
            updates.add(new RegularUpdate("d"));
        });
        assertEquals(3, callCount.get());
        assertEquals("d", syncA.getState().getValue());
        assertEquals("c", syncB.getState().getValue());
        syncB.fetchUpdates();
        assertEquals("d", syncB.getState().getValue());
        assertTrue(syncA.bytesWrittenSinceCompaction() > size);

        syncA.updateState((state, updates) -> {
            callCount.incrementAndGet();
            updates.add(new RegularUpdate("e"));
        });
        assertEquals(4, callCount.get());
        assertEquals("e", syncA.getState().getValue());
        syncB.fetchUpdates();
        assertEquals("e", syncB.getState().getValue());
        assertTrue(syncA.bytesWrittenSinceCompaction() > size);

        syncA.updateState((state, updates) -> {
            callCount.incrementAndGet();
            updates.add(new RegularUpdate("f"));
        });
        assertEquals(5, callCount.get());
        assertEquals("f", syncA.getState().getValue());
        assertTrue(syncA.bytesWrittenSinceCompaction() > size);

        assertEquals("e", syncB.getState().getValue());
        syncB.compact(state -> {
            callCount.incrementAndGet();
            return new RegularUpdate(state.getValue());
        });
        assertEquals(7, callCount.get());
        assertEquals(0, syncB.bytesWrittenSinceCompaction());
        assertEquals("f", syncA.getState().getValue());
        assertEquals("f", syncB.getState().getValue());

        syncA.updateState((state, updates) -> {
            callCount.incrementAndGet();
            updates.add(new RegularUpdate("g"));
        });
        assertEquals(9, callCount.get());
        assertEquals("g", syncA.getState().getValue());
        assertEquals(syncA.bytesWrittenSinceCompaction(), size);

        syncA.updateState((state, updates) -> {
            callCount.incrementAndGet();
            updates.add(new RegularUpdate("h"));
        });
        assertEquals(10, callCount.get());
        assertEquals("h", syncA.getState().getValue());

        syncA.compact(state -> {
            callCount.incrementAndGet();
            return new RegularUpdate("h");
        });
        assertEquals(11, callCount.get());
        assertEquals("h", syncA.getState().getValue());
        syncB.fetchUpdates();
        assertEquals("h", syncB.getState().getValue());
        assertEquals(0, syncA.bytesWrittenSinceCompaction());
    }
}
