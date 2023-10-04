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
package io.pravega.client.batch.impl;

import com.google.common.collect.ImmutableSet;
import io.pravega.auth.AuthenticationException;
import io.pravega.client.ClientConfig;
import io.pravega.client.batch.SegmentRange;
import io.pravega.client.connection.impl.ClientConnection;
import io.pravega.client.connection.impl.ConnectionPool;
import io.pravega.client.segment.impl.Segment;
import io.pravega.client.segment.impl.SegmentTruncatedException;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.StreamCut;
import io.pravega.client.stream.impl.SegmentWithRange;
import io.pravega.client.stream.impl.StreamCutImpl;
import io.pravega.client.stream.impl.StreamImpl;
import io.pravega.client.stream.impl.StreamSegmentsWithPredecessors;
import io.pravega.client.stream.impl.StreamSegmentSuccessors;
import io.pravega.client.stream.mock.MockConnectionFactoryImpl;
import io.pravega.client.stream.mock.MockController;
import io.pravega.common.util.RetriesExhaustedException;
import io.pravega.common.util.Retry;
import io.pravega.shared.protocol.netty.ConnectionFailedException;
import io.pravega.shared.protocol.netty.PravegaNodeUri;
import io.pravega.shared.protocol.netty.ReplyProcessor;
import io.pravega.shared.protocol.netty.WireCommands;
import io.pravega.shared.protocol.netty.WireCommands.GetStreamSegmentInfo;
import io.pravega.shared.protocol.netty.WireCommands.StreamSegmentInfo;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import io.pravega.test.common.AssertExtensions;
import lombok.Cleanup;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.times;

public class BatchClientImplTest {

    private static final String SCOPE = "scope";
    private static final String STREAM = "stream";
    private static final Retry.RetryWithBackoff RETRY_WITH_BACKOFF = Retry.withExpBackoff(1, 1, 2, Duration.ofSeconds(30).toMillis());

    private static class MockControllerWithSuccessors extends MockController {
        private StreamSegmentsWithPredecessors successors;

        public MockControllerWithSuccessors(String endpoint, int port, ConnectionPool connectionPool, StreamSegmentsWithPredecessors successors) {
            super(endpoint, port, connectionPool, false);
            this.successors = successors;
        }

        @Override
        public CompletableFuture<StreamSegmentsWithPredecessors> getSuccessors(Segment segment) {
            return completedFuture(successors);
        }
    }

    @Test(timeout = 7000)
    public void testGetSegmentsWithUnboundedStreamCut() throws Exception {

        PravegaNodeUri location = new PravegaNodeUri("localhost", 0);
        @Cleanup
        MockConnectionFactoryImpl connectionFactory = getMockConnectionFactory(location);
        MockController mockController = new MockController(location.getEndpoint(), location.getPort(), connectionFactory, false);
        Stream stream = createStream(SCOPE, STREAM, 3, mockController);
        @Cleanup
        BatchClientFactoryImpl client = new BatchClientFactoryImpl(mockController, ClientConfig.builder().maxConnectionsPerSegmentStore(1).build(), connectionFactory);

        Iterator<SegmentRange> unBoundedSegments = client.getSegments(stream, StreamCut.UNBOUNDED, StreamCut.UNBOUNDED).getIterator();
        assertTrue(unBoundedSegments.hasNext());
        assertEquals(0L, unBoundedSegments.next().asImpl().getSegment().getSegmentId());
        assertTrue(unBoundedSegments.hasNext());
        assertEquals(1L, unBoundedSegments.next().asImpl().getSegment().getSegmentId());
        assertTrue(unBoundedSegments.hasNext());
        assertEquals(2L, unBoundedSegments.next().asImpl().getSegment().getSegmentId());
        assertFalse(unBoundedSegments.hasNext());
    }

    @Test(timeout = 5000)
    public void testGetSegmentsWithStreamCut() throws Exception {

        PravegaNodeUri location = new PravegaNodeUri("localhost", 0);
        MockConnectionFactoryImpl connectionFactory = getMockConnectionFactory(location);
        MockController mockController = new MockController(location.getEndpoint(), location.getPort(), connectionFactory, false);
        Stream stream = createStream(SCOPE, STREAM, 3, mockController);
        @Cleanup
        BatchClientFactoryImpl client = new BatchClientFactoryImpl(mockController, ClientConfig.builder().maxConnectionsPerSegmentStore(1).build(), connectionFactory);

        Iterator<SegmentRange> boundedSegments = client.getSegments(stream, getStreamCut(5L, 0, 1, 2), getStreamCut(15L, 0, 1, 2)).getIterator();
        assertTrue(boundedSegments.hasNext());
        assertEquals(0L, boundedSegments.next().asImpl().getSegment().getSegmentId());
        assertTrue(boundedSegments.hasNext());
        assertEquals(1L, boundedSegments.next().asImpl().getSegment().getSegmentId());
        assertTrue(boundedSegments.hasNext());
        assertEquals(2L, boundedSegments.next().asImpl().getSegment().getSegmentId());
        assertFalse(boundedSegments.hasNext());
    }

    @Test(timeout = 5000)
    public void testGetSegmentsWithNullStreamCut() throws Exception {

        PravegaNodeUri location = new PravegaNodeUri("localhost", 0);
        @Cleanup
        MockConnectionFactoryImpl connectionFactory = getMockConnectionFactory(location);
        MockController mockController = new MockController(location.getEndpoint(), location.getPort(), connectionFactory, false);
        Stream stream = createStream(SCOPE, STREAM, 3, mockController);
        @Cleanup
        BatchClientFactoryImpl client = new BatchClientFactoryImpl(mockController, ClientConfig.builder().maxConnectionsPerSegmentStore(1).build(), connectionFactory);

        Iterator<SegmentRange> segments = client.getSegments(stream, null, null).getIterator();
        assertTrue(segments.hasNext());
        assertEquals(0L, segments.next().asImpl().getSegment().getSegmentId());
        assertTrue(segments.hasNext());
        assertEquals(1L, segments.next().asImpl().getSegment().getSegmentId());
        assertTrue(segments.hasNext());
        assertEquals(2L, segments.next().asImpl().getSegment().getSegmentId());
        assertFalse(segments.hasNext());
    }

    @Test(timeout = 5000)
    public void testGetSegmentsWithMultipleSegments() throws Exception {

        PravegaNodeUri location = new PravegaNodeUri("localhost", 0);
        @Cleanup
        MockConnectionFactoryImpl connectionFactory = getMockConnectionFactory(location);
        MockController mockController = new MockController(location.getEndpoint(), location
                .getPort(), connectionFactory, false);
        MockController stubbedController = spy(mockController);
        Stream stream = createStream(SCOPE, STREAM, 2, stubbedController);
        Set<Segment> segments = ImmutableSet.<Segment>builder().add(new Segment(SCOPE, STREAM, 0L),
                new Segment(SCOPE, STREAM, 1L), new Segment(SCOPE, STREAM, 2L)).build();
        // Setup mock.
        doReturn(CompletableFuture.completedFuture(new StreamSegmentSuccessors(segments, "")))
                .when(stubbedController).getSegments(any(StreamCut.class), any(StreamCut.class));
        @Cleanup
        BatchClientFactoryImpl client = new BatchClientFactoryImpl(stubbedController, ClientConfig.builder().maxConnectionsPerSegmentStore(1).build(), connectionFactory);

        Iterator<SegmentRange> segmentIterator = client.getSegments(stream, null, null).getIterator();
        assertTrue(segmentIterator.hasNext());
        assertEquals(0L, segmentIterator.next().asImpl().getSegment().getSegmentId());
        assertTrue(segmentIterator.hasNext());
        assertEquals(1L, segmentIterator.next().asImpl().getSegment().getSegmentId());
        assertTrue(segmentIterator.hasNext());
        assertEquals(2L, segmentIterator.next().asImpl().getSegment().getSegmentId());
        assertFalse(segmentIterator.hasNext());
    }

    @Test(timeout = 5000)
    public void testGetSegmentRangeBetweenStreamCuts() throws Exception {

        PravegaNodeUri location = new PravegaNodeUri("localhost", 0);
        @Cleanup
        MockConnectionFactoryImpl connectionFactory = getMockConnectionFactory(location);
        MockController mockController = new MockController(location.getEndpoint(), location.getPort(), connectionFactory, false);
        @Cleanup
        BatchClientFactoryImpl client = new BatchClientFactoryImpl(mockController, ClientConfig.builder().maxConnectionsPerSegmentStore(1).build(), connectionFactory);
        List<SegmentRange> segRanges = client.getSegmentRangeBetweenStreamCuts(getStreamCut(5L, 0, 1, 2), getStreamCut(15L, 0, 1, 2));

        assertNotNull(segRanges);
        assertEquals(3, segRanges.size());
        assertEquals("stream", segRanges.get(2).getSegment().getStream().getStreamName());
    }

    @Test(timeout = 5000)
    public void testGetNextStreamCut() throws Exception {
        Segment segment1 = new Segment("scope", "stream", 1L);
        Segment segment2 = new Segment("scope", "stream", 2L);
        Map<Segment, Long> positionMap = new HashMap<>();
        positionMap.put(segment1, 20L);
        positionMap.put(segment2, 30L);
        StreamCut startingSC = new StreamCutImpl(Stream.of("scope", "stream"), positionMap);
        PravegaNodeUri endpoint = new PravegaNodeUri("localhost", 0);
        @Cleanup
        MockConnectionFactoryImpl cf = new MockConnectionFactoryImpl();
        @Cleanup
        MockControllerWithSuccessors controller = new MockControllerWithSuccessors(endpoint.getEndpoint(), endpoint.getPort(), cf, getEmptyReplacement().join());
        ClientConnection connection = mock(ClientConnection.class);
        cf.provideConnection(endpoint, connection);
        @Cleanup
        BatchClientFactoryImpl client = new BatchClientFactoryImpl(controller, ClientConfig.builder().maxConnectionsPerSegmentStore(1).build(), cf);

        Mockito.doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) {
                WireCommands.LocateOffset locateOffset = invocation.getArgument(0);
                ReplyProcessor processor = cf.getProcessor(endpoint);
                processor.process(new WireCommands.OffsetLocated(locateOffset.getRequestId(), locateOffset.getSegment(), 90L));
                return null;
            }
        }).when(connection).send(any(WireCommands.LocateOffset.class));
        StreamCut nextSC = client.getNextStreamCut(startingSC, 50L);
        assertNotNull(nextSC);
        assertEquals(2, nextSC.asImpl().getPositions().size());
        assertTrue(nextSC.asImpl().getPositions().containsKey(segment1));
        assertTrue(nextSC.asImpl().getPositions().containsKey(segment2));
    }

    @Test(timeout = 5000)
    public void testGetNextStreamCutSegmentScaleUp() throws Exception {
        Segment segment1 = new Segment("scope", "stream", 1L);
        Segment segment2 = new Segment("scope", "stream", 2L);
        Segment segment3 = new Segment("scope", "stream", 3L);
        Segment segment4 = new Segment("scope", "stream", 4L);
        Segment segment5 = new Segment("scope", "stream", 5L);
        Map<Segment, Long> positionMap = new HashMap<>();
        positionMap.put(segment1, 20L);
        positionMap.put(segment2, 90L);
        positionMap.put(segment3, 50L);
        StreamCut startingSC = new StreamCutImpl(Stream.of("scope", "stream"), positionMap);
        PravegaNodeUri endpoint = new PravegaNodeUri("localhost", 0);
        @Cleanup
        MockConnectionFactoryImpl cf = new MockConnectionFactoryImpl();
        @Cleanup
        MockControllerWithSuccessors controller = new MockControllerWithSuccessors(endpoint.getEndpoint(), endpoint.getPort(), cf, getScaleUpReplacement(segment2, segment4, segment5).join());
        ClientConnection connection = mock(ClientConnection.class);
        cf.provideConnection(endpoint, connection);
        @Cleanup
        BatchClientFactoryImpl client = new BatchClientFactoryImpl(controller, ClientConfig.builder().maxConnectionsPerSegmentStore(1).build(), cf);

        Mockito.doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) {
                WireCommands.LocateOffset locateOffset = invocation.getArgument(0);
                ReplyProcessor processor = cf.getProcessor(endpoint);
                processor.process(new WireCommands.OffsetLocated(locateOffset.getRequestId(), locateOffset.getSegment(), 90L));
                return null;
            }
        }).when(connection).send(any(WireCommands.LocateOffset.class));
        StreamCut nextSC = client.getNextStreamCut(startingSC, 50L);
        assertNotNull(nextSC);
        assertEquals(4, nextSC.asImpl().getPositions().size());
        assertTrue(nextSC.asImpl().getPositions().containsKey(segment1)
                && nextSC.asImpl().getPositions().containsKey(segment3)
                && nextSC.asImpl().getPositions().containsKey(segment4)
                && nextSC.asImpl().getPositions().containsKey(segment5));
    }

    @Test(timeout = 5000)
    public void testGetNextStreamCutSegmentsScaleDownNotForwarded() throws Exception {
        Segment segment1 = new Segment("scope", "stream", 1L);
        Segment segment2 = new Segment("scope", "stream", 2L);
        Segment segment3 = new Segment("scope", "stream", 3L);
        Segment segment4 = new Segment("scope", "stream", 4L);

        Map<Segment, Long> positionMap = new HashMap<>();
        positionMap.put(segment1, 20L);
        positionMap.put(segment2, 90L);
        positionMap.put(segment3, 40L);
        StreamCut startingSC = new StreamCutImpl(Stream.of("scope", "stream"), positionMap);
        PravegaNodeUri endpoint = new PravegaNodeUri("localhost", 0);
        @Cleanup
        MockConnectionFactoryImpl cf = new MockConnectionFactoryImpl();
        @Cleanup
        MockControllerWithSuccessors controller = new MockControllerWithSuccessors(endpoint.getEndpoint(), endpoint.getPort(), cf, getScaleDownReplacement(segment2, segment3, segment4).join());
        ClientConnection connection = mock(ClientConnection.class);
        cf.provideConnection(endpoint, connection);
        @Cleanup
        BatchClientFactoryImpl client = new BatchClientFactoryImpl(controller, ClientConfig.builder().maxConnectionsPerSegmentStore(1).build(), cf);

        Mockito.doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) {
                WireCommands.LocateOffset locateOffset = invocation.getArgument(0);
                ReplyProcessor processor = cf.getProcessor(endpoint);
                processor.process(new WireCommands.OffsetLocated(locateOffset.getRequestId(), locateOffset.getSegment(), 90L));
                return null;
            }
        }).when(connection).send(any(WireCommands.LocateOffset.class));
        StreamCut nextSC = client.getNextStreamCut(startingSC, 50L);
        assertNotNull(nextSC);
        assertEquals(3, nextSC.asImpl().getPositions().size());
        assertTrue(nextSC.asImpl().getPositions().containsKey(segment1)
                && nextSC.asImpl().getPositions().containsKey(segment2)
                && nextSC.asImpl().getPositions().containsKey(segment3));
    }

    @Test(timeout = 5000)
    public void testGetNextStreamCutSegmentScaleDown() throws Exception {
        Segment segment1 = new Segment("scope", "stream", 1L);
        Segment segment2 = new Segment("scope", "stream", 2L);
        Segment segment3 = new Segment("scope", "stream", 3L);
        Segment segment4 = new Segment("scope", "stream", 4L);

        Map<Segment, Long> positionMap = new HashMap<>();
        positionMap.put(segment1, 30L);
        positionMap.put(segment2, 90L);
        positionMap.put(segment3, 90L);
        StreamCut startingSC = new StreamCutImpl(Stream.of("scope", "stream"), positionMap);
        PravegaNodeUri endpoint = new PravegaNodeUri("localhost", 0);
        @Cleanup
        MockConnectionFactoryImpl cf = new MockConnectionFactoryImpl();
        @Cleanup
        MockControllerWithSuccessors controller = new MockControllerWithSuccessors(endpoint.getEndpoint(), endpoint.getPort(), cf, getScaleDownReplacement(segment2, segment3, segment4).join());
        ClientConnection connection = mock(ClientConnection.class);
        cf.provideConnection(endpoint, connection);
        @Cleanup
        BatchClientFactoryImpl client = new BatchClientFactoryImpl(controller, ClientConfig.builder().maxConnectionsPerSegmentStore(1).build(), cf);

        Mockito.doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) {
                WireCommands.LocateOffset locateOffset = invocation.getArgument(0);
                ReplyProcessor processor = cf.getProcessor(endpoint);
                processor.process(new WireCommands.OffsetLocated(locateOffset.getRequestId(), locateOffset.getSegment(), 90L));
                return null;
            }
        }).when(connection).send(any(WireCommands.LocateOffset.class));
        StreamCut nextSC = client.getNextStreamCut(startingSC, 50L);
        assertNotNull(nextSC);
        assertEquals(2, nextSC.asImpl().getPositions().size());
        assertTrue(nextSC.asImpl().getPositions().containsKey(segment1)
                && nextSC.asImpl().getPositions().containsKey(segment4));
    }

    @Test(timeout = 5000)
    public void testGetNextStreamCutNoScaling() throws Exception {
        Segment segment1 = new Segment("scope", "stream", 1L);
        Segment segment2 = new Segment("scope", "stream", 2L);
        Segment segment3 = new Segment("scope", "stream", 3L);

        Map<Segment, Long> positionMap = new HashMap<>();
        positionMap.put(segment1, 30L);
        positionMap.put(segment2, 90L);
        positionMap.put(segment3, 40L);
        StreamCut startingSC = new StreamCutImpl(Stream.of("scope", "stream"), positionMap);
        PravegaNodeUri endpoint = new PravegaNodeUri("localhost", 0);
        @Cleanup
        MockConnectionFactoryImpl cf = new MockConnectionFactoryImpl();
        @Cleanup
        MockControllerWithSuccessors controller = new MockControllerWithSuccessors(endpoint.getEndpoint(), endpoint.getPort(), cf, getEmptyReplacement().join());
        ClientConnection connection = mock(ClientConnection.class);
        cf.provideConnection(endpoint, connection);
        @Cleanup
        BatchClientFactoryImpl client = new BatchClientFactoryImpl(controller, ClientConfig.builder().maxConnectionsPerSegmentStore(1).build(), cf);

        Mockito.doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) {
                WireCommands.LocateOffset locateOffset = invocation.getArgument(0);
                ReplyProcessor processor = cf.getProcessor(endpoint);
                processor.process(new WireCommands.OffsetLocated(locateOffset.getRequestId(), locateOffset.getSegment(), 90L));
                return null;
            }
        }).when(connection).send(any(WireCommands.LocateOffset.class));
        StreamCut nextSC = client.getNextStreamCut(startingSC, 50L);
        assertNotNull(nextSC);
        assertEquals(3, nextSC.asImpl().getPositions().size());
        assertTrue(nextSC.asImpl().getPositions().containsKey(segment1)
                && nextSC.asImpl().getPositions().containsKey(segment2)
                && nextSC.asImpl().getPositions().containsKey(segment3));
    }

    @Test(timeout = 10000)
    public void testGetNextStreamCutWithNoSuchSegmentException() throws Exception {
        Segment segment1 = new Segment("scope", "stream", 1L);
        Map<Segment, Long> positionMap = new HashMap<>();
        positionMap.put(segment1, 30L);
        StreamCut startingSC = new StreamCutImpl(Stream.of("scope", "stream"), positionMap);
        PravegaNodeUri endpoint = new PravegaNodeUri("localhost", 0);
        @Cleanup
        MockConnectionFactoryImpl cf = new MockConnectionFactoryImpl();
        @Cleanup
        MockControllerWithSuccessors controller = new MockControllerWithSuccessors(endpoint.getEndpoint(), endpoint.getPort(), cf, getEmptyReplacement().join());
        ClientConnection connection = mock(ClientConnection.class);
        cf.provideConnection(endpoint, connection);
        @Cleanup
        BatchClientFactoryImpl client = new BatchClientFactoryImpl(controller, ClientConfig.builder().maxConnectionsPerSegmentStore(1).build(), cf, RETRY_WITH_BACKOFF);

        Mockito.doAnswer((Answer<Void>) invocation -> {
            WireCommands.LocateOffset locateOffset = invocation.getArgument(0);
            ReplyProcessor processor = cf.getProcessor(endpoint);
            processor.process(new WireCommands.NoSuchSegment(locateOffset.getRequestId(), locateOffset.getSegment(), "", 30L));
            return null;
        }).when(connection).send(any(WireCommands.LocateOffset.class));
        AssertExtensions.assertThrows(SegmentTruncatedException.class, () -> client.getNextStreamCut(startingSC, 50L));
        Mockito.doAnswer((Answer<Void>) invocation -> {
            WireCommands.LocateOffset locateOffset = invocation.getArgument(0);
            ReplyProcessor processor = cf.getProcessor(endpoint);
            processor.process(new WireCommands.AuthTokenCheckFailed(locateOffset.getRequestId(), "Token expired", WireCommands.AuthTokenCheckFailed.ErrorCode.TOKEN_EXPIRED));
            return null;
        }).when(connection).send(any(WireCommands.LocateOffset.class));
        AssertExtensions.assertThrows(RetriesExhaustedException.class, () -> client.getNextStreamCut(startingSC, 50L));
        Mockito.doAnswer((Answer<Void>) invocation -> {
            WireCommands.LocateOffset locateOffset = invocation.getArgument(0);
            ReplyProcessor processor = cf.getProcessor(endpoint);
            processor.process(new WireCommands.AuthTokenCheckFailed(locateOffset.getRequestId(), "Token expired", WireCommands.AuthTokenCheckFailed.ErrorCode.TOKEN_CHECK_FAILED));
            return null;
        }).when(connection).send(any(WireCommands.LocateOffset.class));
        AssertExtensions.assertThrows(AuthenticationException.class, () -> client.getNextStreamCut(startingSC, 50L));
    }

    @Test(timeout = 7000)
    public void testGetNextStreamCutWithTokenException() throws Exception {
        Segment segment1 = new Segment("scope", "stream", 1L);
        Map<Segment, Long> positionMap = new HashMap<>();
        positionMap.put(segment1, 30L);
        StreamCut startingSC = new StreamCutImpl(Stream.of("scope", "stream"), positionMap);
        PravegaNodeUri endpoint = new PravegaNodeUri("localhost", 0);
        @Cleanup
        MockConnectionFactoryImpl cf = new MockConnectionFactoryImpl();
        @Cleanup
        MockControllerWithSuccessors controller = new MockControllerWithSuccessors(endpoint.getEndpoint(), endpoint.getPort(), cf, getEmptyReplacement().join());
        ClientConnection connection = mock(ClientConnection.class);
        cf.provideConnection(endpoint, connection);
        @Cleanup
        BatchClientFactoryImpl client = new BatchClientFactoryImpl(controller, ClientConfig.builder().maxConnectionsPerSegmentStore(1).build(), cf, RETRY_WITH_BACKOFF);

        Mockito.doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) {
                WireCommands.LocateOffset locateOffset = invocation.getArgument(0);
                ReplyProcessor processor = cf.getProcessor(endpoint);
                processor.process(new WireCommands.AuthTokenCheckFailed(locateOffset.getRequestId(), "server-stacktrace",
                        WireCommands.AuthTokenCheckFailed.ErrorCode.TOKEN_EXPIRED));
                return null;
            }
        }).when(connection).send(any(WireCommands.LocateOffset.class));
        AssertExtensions.assertThrows(RetriesExhaustedException.class, () -> client.getNextStreamCut(startingSC, 50L));
    }

    @Test(timeout = 7000)
    public void testGetNextStreamCutRetryWithTokenException() throws Exception {
        Segment segment1 = new Segment("scope", "stream", 1L);
        Map<Segment, Long> positionMap = new HashMap<>();
        positionMap.put(segment1, 30L);
        StreamCut startingSC = new StreamCutImpl(Stream.of("scope", "stream"), positionMap);
        PravegaNodeUri endpoint = new PravegaNodeUri("localhost", 0);
        @Cleanup
        MockConnectionFactoryImpl cf = new MockConnectionFactoryImpl();
        @Cleanup
        MockControllerWithSuccessors controller = new MockControllerWithSuccessors(endpoint.getEndpoint(), endpoint.getPort(), cf, getEmptyReplacement().join());
        ClientConnection connection = mock(ClientConnection.class);
        cf.provideConnection(endpoint, connection);
        @Cleanup
        BatchClientFactoryImpl client = new BatchClientFactoryImpl(controller, ClientConfig.builder().maxConnectionsPerSegmentStore(1).build(), cf, RETRY_WITH_BACKOFF);
        Mockito.doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) {

                WireCommands.LocateOffset locateOffset = invocation.getArgument(0);
                ReplyProcessor processor = cf.getProcessor(endpoint);
                processor.process(new WireCommands.AuthTokenCheckFailed(locateOffset.getRequestId(), "server-stacktrace",
                        WireCommands.AuthTokenCheckFailed.ErrorCode.TOKEN_EXPIRED));
                return null;
            }
        }).when(connection).send(any(WireCommands.LocateOffset.class));

        AssertExtensions.assertThrows(RetriesExhaustedException.class, () -> client.getNextStreamCut(startingSC, 50L));
        //Verify retry Logic
        verify(connection, times(2)).send(any(WireCommands.LocateOffset.class));
    }

    @Test(timeout = 5000)
    public void testGetNextStreamCutWithApproxOffsetGreaterThanZero() {
        Segment segment1 = new Segment("scope", "stream", 1L);
        Map<Segment, Long> positionMap = new HashMap<>();
        positionMap.put(segment1, 30L);
        StreamCut startingSC = new StreamCutImpl(Stream.of("scope", "stream"), positionMap);
        PravegaNodeUri endpoint = new PravegaNodeUri("localhost", 0);
        @Cleanup
        MockConnectionFactoryImpl cf = new MockConnectionFactoryImpl();
        @Cleanup
        MockControllerWithSuccessors controller = new MockControllerWithSuccessors(endpoint.getEndpoint(), endpoint.getPort(), cf, getEmptyReplacement().join());
        ClientConnection connection = mock(ClientConnection.class);
        cf.provideConnection(endpoint, connection);
        @Cleanup
        BatchClientFactoryImpl client = new BatchClientFactoryImpl(controller, ClientConfig.builder().maxConnectionsPerSegmentStore(1).build(), cf);
        AssertExtensions.assertThrows(IllegalArgumentException.class, () -> client.getNextStreamCut(startingSC, 0L));
    }

    private CompletableFuture<StreamSegmentsWithPredecessors> getEmptyReplacement() {
        Map<SegmentWithRange, List<Long>> segments = new HashMap<>();
        return CompletableFuture.completedFuture(new StreamSegmentsWithPredecessors(segments, ""));
    }

    private CompletableFuture<StreamSegmentsWithPredecessors> getScaleDownReplacement(Segment old1, Segment old2, Segment repacement1) {
        Map<SegmentWithRange, List<Long>> segments = new HashMap<>();
        segments.put(new SegmentWithRange(repacement1, 0, 0.35), Arrays.asList(old1.getSegmentId(), old2.getSegmentId()));
        return CompletableFuture.completedFuture(new StreamSegmentsWithPredecessors(segments, ""));
    }

    private CompletableFuture<StreamSegmentsWithPredecessors> getScaleUpReplacement(Segment old, Segment repacement1, Segment repacement2) {
        Map<SegmentWithRange, List<Long>> segments = new HashMap<>();
        segments.put(new SegmentWithRange(repacement1, 0, 0.25), Collections.singletonList(old.getSegmentId()));
        segments.put(new SegmentWithRange(repacement2, 0.25, 0.5), Collections.singletonList(old.getSegmentId()));
        return CompletableFuture.completedFuture(new StreamSegmentsWithPredecessors(segments, ""));
    }

    private Stream createStream(String scope, String streamName, int numSegments, MockController mockController) {
        Stream stream = new StreamImpl(scope, streamName);
        mockController.createScope(scope);
        mockController.createStream(scope, streamName, StreamConfiguration.builder()
                                                       .scalingPolicy(ScalingPolicy.fixed(numSegments))
                                                       .build())
                      .join();
        return stream;
    }

    private MockConnectionFactoryImpl getMockConnectionFactory(PravegaNodeUri location) throws ConnectionFailedException {
        MockConnectionFactoryImpl connectionFactory = new MockConnectionFactoryImpl();
        ClientConnection connection = mock(ClientConnection.class);
        Mockito.doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                GetStreamSegmentInfo request = (GetStreamSegmentInfo) invocation.getArgument(0);
                connectionFactory.getProcessor(location)
                                 .process(new StreamSegmentInfo(request.getRequestId(), request.getSegmentName(), true,
                                                                false, false, 0, 0, 0));
                return null;
            }
        }).when(connection).send(Mockito.any(GetStreamSegmentInfo.class));
        connectionFactory.provideConnection(location, connection);
        return connectionFactory;
    }

    private StreamCut getStreamCut(long offset, int... segments) {
        final Map<Segment, Long> positionMap = Arrays.stream(segments).boxed()
                                                     .collect(Collectors.toMap(s -> new Segment("scope", STREAM, s),
                                                             s -> offset));

        return new StreamCutImpl(Stream.of("scope", STREAM), positionMap);
    }
}
