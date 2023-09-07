/**
 * Copyright Pravega Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.pravega.client.state.impl;

import io.pravega.client.ClientConfig;
import io.pravega.client.SynchronizerClientFactory;
import io.pravega.client.connection.impl.ConnectionPoolImpl;
import io.pravega.client.control.impl.Controller;
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
import io.pravega.client.stream.InvalidStreamException;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.TruncatedDataException;
import io.pravega.client.stream.impl.ByteArraySerializer;
import io.pravega.client.stream.impl.ClientFactoryImpl;
import io.pravega.client.stream.impl.JavaSerializer;
import io.pravega.client.stream.impl.StreamSegments;
import io.pravega.client.stream.mock.MockClientFactory;
import io.pravega.client.stream.mock.MockSegmentStreamFactory;
import io.pravega.common.concurrent.ExecutorServiceHelpers;
import io.pravega.common.util.ReusableLatch;
import io.pravega.test.common.AssertExtensions;
import java.io.Serializable;
import java.net.URI;
import java.util.AbstractMap;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledExecutorService;
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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SynchronizerTest {

    private final StreamConfiguration config = StreamConfiguration.builder()
                                                                  .scalingPolicy(ScalingPolicy.fixed(1))
                                                                  .build();

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
        public Iterator<Entry<Revision, UpdateOrInit<RevisionedImpl>>> readRange(Revision startRevision, Revision endRevision) {
            return new Iterator<Entry<Revision, UpdateOrInit<RevisionedImpl>>>() {
                private int startPos = startRevision.asImpl().getEventAtOffset();
                private int endPos = endRevision.asImpl().getEventAtOffset();
                @Override
                public Entry<Revision, UpdateOrInit<RevisionedImpl>> next() {
                    if (!hasNext()) {
                        throw new NoSuchElementException();
                    }
                    UpdateOrInit<RevisionedImpl> value;
                    RevisionImpl revision = new RevisionImpl(segment, startPos, startPos);
                    if (startPos == 0) {
                        value = new UpdateOrInit<>(init);
                    } else {
                        value = new UpdateOrInit<>(Collections.singletonList(updates[startPos - 1]));
                    }
                    startPos++;
                    return new AbstractMap.SimpleImmutableEntry<>(revision, value);
                }

                @Override
                public boolean hasNext() {
                    return startPos <= endPos;
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
    public void testCompaction() {
        String streamName = "streamName";
        String scope = "scope";

        MockSegmentStreamFactory ioFactory = new MockSegmentStreamFactory();
        @Cleanup
        MockClientFactory clientFactory = new MockClientFactory(scope, ioFactory);
        createScopeAndStream(streamName, scope, clientFactory.getController());

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
        createScopeAndStream(streamName, scope, clientFactory.getController());
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
    public void testReturnValue() {
        String streamName = "streamName";
        String scope = "scope";
        
        MockSegmentStreamFactory ioFactory = new MockSegmentStreamFactory();
        @Cleanup
        MockClientFactory clientFactory = new MockClientFactory(scope, ioFactory);
        createScopeAndStream(streamName, scope, clientFactory.getController());

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
    public void testCompactionShrinksSet() {
        String streamName = "testCompactionShrinksSet";
        String scope = "scope";
        
        MockSegmentStreamFactory ioFactory = new MockSegmentStreamFactory();
        @Cleanup
        MockClientFactory clientFactory = new MockClientFactory(scope, ioFactory);
        createScopeAndStream(streamName, scope, clientFactory.getController());

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
    public void testSetOperations() {
        String streamName = "testCompactionShrinksSet";
        String scope = "scope";
        
        MockSegmentStreamFactory ioFactory = new MockSegmentStreamFactory();
        @Cleanup
        MockClientFactory clientFactory = new MockClientFactory(scope, ioFactory);
        createScopeAndStream(streamName, scope, clientFactory.getController());

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
    public void testCompactWithTruncation() {
        String streamName = "streamName";
        String scope = "scope";

        MockSegmentStreamFactory ioFactory = new MockSegmentStreamFactory();
        @Cleanup
        MockClientFactory clientFactoryA = new MockClientFactory(scope, ioFactory);
        createScopeAndStream(streamName, scope, clientFactoryA.getController());
        @Cleanup
        MockClientFactory clientFactoryB = new MockClientFactory(scope, ioFactory);
        createScopeAndStream(streamName, scope, clientFactoryB.getController());

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

    @Test(timeout = 20000)
    public void testCreateStateSynchronizerError() {
        String streamName = "streamName";
        String scope = "scope";

        Controller controller = mock(Controller.class);
        @Cleanup
        ClientFactoryImpl clientFactory = new ClientFactoryImpl(scope, controller, ClientConfig.builder().build());

        // Simulate a sealed stream.
        CompletableFuture<StreamSegments> result = new CompletableFuture<>();
        result.complete(new StreamSegments(new TreeMap<>()));
        when(controller.getCurrentSegments(scope, streamName)).thenReturn(result);

        AssertExtensions.assertThrows(InvalidStreamException.class, () -> clientFactory.createStateSynchronizer(streamName,
                                                                                                                new JavaSerializer<>(),
                                                                                                                new JavaSerializer<>(),
                                                                                                                SynchronizerConfig.builder().build()));

        result = new CompletableFuture<>();
        result.completeExceptionally(new RuntimeException("Controller exception"));
        when(controller.getCurrentSegments(scope, streamName)).thenReturn(result);

        AssertExtensions.assertThrows(InvalidStreamException.class, () -> clientFactory.createStateSynchronizer(streamName,
                                                                                                                new JavaSerializer<>(),
                                                                                                                new JavaSerializer<>(),
                                                                                                                SynchronizerConfig.builder().build()));
    }

    @Test(timeout = 20000)
    @SuppressWarnings("unchecked")
    public void testFetchUpdatesWithMultipleTruncation() {
        String streamName = "streamName";
        String scope = "scope";

        RevisionedStreamClient<UpdateOrInit<RevisionedImpl>> revisionedClient = mock(RevisionedStreamClient.class);
        final Segment segment = new Segment(scope, streamName, 0L);
        @Cleanup
        StateSynchronizerImpl<RevisionedImpl> syncA = new StateSynchronizerImpl<>(segment, revisionedClient);

        Revision firstMark = new RevisionImpl(segment, 10L, 1);
        Revision secondMark = new RevisionImpl(segment, 20L, 2);
        final AbstractMap.SimpleImmutableEntry<Revision, UpdateOrInit<RevisionedImpl>> entry =
                new AbstractMap.SimpleImmutableEntry<>(secondMark, new UpdateOrInit<>(new RegularUpdate("x")));
        Iterator<Entry<Revision, UpdateOrInit<RevisionedImpl>>> iterator =
                Collections.<Entry<Revision, UpdateOrInit<RevisionedImpl>>>singletonList(entry).iterator();

        // Setup Mock
        when(revisionedClient.getMark()).thenReturn(firstMark);
        when(revisionedClient.readFrom(firstMark))
                // simulate multiple TruncatedDataExceptions.
                .thenThrow(new TruncatedDataException())
                .thenThrow(new TruncatedDataException())
                .thenReturn(iterator);
        when(revisionedClient.readFrom(secondMark)).thenReturn(iterator);

        syncA.fetchUpdates(); // invoke fetchUpdates which will encounter TruncatedDataException from RevisionedStreamClient.
        assertEquals("x", syncA.getState().getValue());
    }

    @Test(timeout = 20000)
    @SuppressWarnings("unchecked")
    public void testConcurrentFetchUpdatesAfterTruncation() {
        String streamName = "streamName";
        String scope = "scope";

        // Mock of the RevisionedStreamClient.
        RevisionedStreamClient<UpdateOrInit<RevisionedImpl>> revisionedStreamClient = mock(RevisionedStreamClient.class);

        final Segment segment = new Segment(scope, streamName, 0L);
        @Cleanup
        StateSynchronizerImpl<RevisionedImpl> syncA = new StateSynchronizerImpl<>(segment, revisionedStreamClient);

        Revision firstMark = new RevisionImpl(segment, 10L, 1);
        Revision secondMark = new RevisionImpl(segment, 20L, 2);
        final AbstractMap.SimpleImmutableEntry<Revision, UpdateOrInit<RevisionedImpl>> entry =
                new AbstractMap.SimpleImmutableEntry<>(secondMark, new UpdateOrInit<>(new RegularUpdate("x")));

        // Mock iterators to simulate concurrent revisionedStreamClient.readFrom(firstMark) call.
        Iterator<Entry<Revision, UpdateOrInit<RevisionedImpl>>> iterator1 =
                Collections.<Entry<Revision, UpdateOrInit<RevisionedImpl>>>singletonList(entry).iterator();
        Iterator<Entry<Revision, UpdateOrInit<RevisionedImpl>>> iterator2 =
                Collections.<Entry<Revision, UpdateOrInit<RevisionedImpl>>>singletonList(entry).iterator();
        // Latch to ensure both the thread encounter truncation exception.
        CountDownLatch truncationLatch = new CountDownLatch(2);
        // Latch to ensure both the threads invoke read attempt reading from same revision.
        // This will simulate the race condition where the in-memory state is newer than the state returned by RevisionedStreamClient.
        CountDownLatch raceLatch = new CountDownLatch(2);

        // Setup Mock
        when(revisionedStreamClient.getMark()).thenReturn(firstMark);
        when(revisionedStreamClient.readFrom(firstMark))
                // simulate multiple TruncatedDataExceptions.
                .thenAnswer(invocation -> {
                    truncationLatch.countDown();
                    truncationLatch.await(); // wait until the other thread encounters the TruncationDataException.
                    throw new TruncatedDataException();
                })
                .thenAnswer(invocation -> {
                    throw new TruncatedDataException();
                })
                .thenAnswer(invocation -> {
                    truncationLatch.countDown();
                    raceLatch.await(); // wait until the other thread attempts to fetch updates from SSS post truncation and updates internal state.
                    return iterator1;
                }).thenAnswer(invocation -> {
                    raceLatch.countDown();
                    return iterator2;
                });

        // Return an iterator whose hasNext is false.
        when(revisionedStreamClient.readFrom(secondMark)).thenAnswer(invocation -> {
            raceLatch.countDown(); // release the waiting thread which is fetching updates from SSS when the internal state is already updated.
            return iterator2;
        });

        // Simulate concurrent invocations of fetchUpdates API.
        @Cleanup("shutdownNow")
        ScheduledExecutorService exec = ExecutorServiceHelpers.newScheduledThreadPool(2, "test-pool");
        CompletableFuture<Void> cf1 = CompletableFuture.supplyAsync(() -> {
            syncA.fetchUpdates();
            return null;
        }, exec);
        CompletableFuture<Void> cf2 = CompletableFuture.supplyAsync(() -> {
            syncA.fetchUpdates();
            return null;
        }, exec);
        // Wait until the completion of both the fetchUpdates() API.
        CompletableFuture.allOf(cf1, cf2).join();
        assertEquals("x", syncA.getState().getValue());
    }

    @Test(timeout = 5000)
    public void testSynchronizerClientFactory() {
        ClientConfig config = ClientConfig.builder().controllerURI(URI.create("tls://localhost:9090")).build();
        @Cleanup
        ClientFactoryImpl factory = (ClientFactoryImpl) SynchronizerClientFactory.withScope("scope", config);
        ConnectionPoolImpl cp = (ConnectionPoolImpl) factory.getConnectionPool();
        assertEquals(1, cp.getClientConfig().getMaxConnectionsPerSegmentStore());
        assertEquals(config.isEnableTls(), cp.getClientConfig().isEnableTls());
    }

    private void createScopeAndStream(String streamName, String scope, Controller controller) {
        controller.createScope(scope).join();
        controller.createStream(scope, streamName, config);
    }
}
