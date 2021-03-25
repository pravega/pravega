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
import io.pravega.client.ClientConfig;
import io.pravega.client.batch.SegmentRange;
import io.pravega.client.connection.impl.ClientConnection;
import io.pravega.client.segment.impl.Segment;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.StreamCut;
import io.pravega.client.stream.impl.StreamCutImpl;
import io.pravega.client.stream.impl.StreamImpl;
import io.pravega.client.stream.impl.StreamSegmentSuccessors;
import io.pravega.client.stream.mock.MockConnectionFactoryImpl;
import io.pravega.client.stream.mock.MockController;
import io.pravega.shared.protocol.netty.ConnectionFailedException;
import io.pravega.shared.protocol.netty.PravegaNodeUri;
import io.pravega.shared.protocol.netty.WireCommands.GetStreamSegmentInfo;
import io.pravega.shared.protocol.netty.WireCommands.StreamSegmentInfo;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import lombok.Cleanup;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

public class BatchClientImplTest {

    private static final String SCOPE = "scope";
    private static final String STREAM = "stream";

    @Test(timeout = 5000)
    public void testGetSegmentsWithUnboundedStreamCut() throws Exception {

        PravegaNodeUri location = new PravegaNodeUri("localhost", 0);
        @Cleanup
        MockConnectionFactoryImpl connectionFactory = getMockConnectionFactory(location);
        MockController mockController = new MockController(location.getEndpoint(), location.getPort(), connectionFactory, false);
        Stream stream = createStream(SCOPE, STREAM, 3, mockController);
        @Cleanup
        BatchClientFactoryImpl client = new BatchClientFactoryImpl(mockController, ClientConfig.builder().build(), connectionFactory);

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
        BatchClientFactoryImpl client = new BatchClientFactoryImpl(mockController, ClientConfig.builder().build(), connectionFactory);

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
        BatchClientFactoryImpl client = new BatchClientFactoryImpl(mockController, ClientConfig.builder().build(), connectionFactory);

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
        BatchClientFactoryImpl client = new BatchClientFactoryImpl(stubbedController, ClientConfig.builder().build(), connectionFactory);

        Iterator<SegmentRange> segmentIterator = client.getSegments(stream, null, null).getIterator();
        assertTrue(segmentIterator.hasNext());
        assertEquals(0L, segmentIterator.next().asImpl().getSegment().getSegmentId());
        assertTrue(segmentIterator.hasNext());
        assertEquals(1L, segmentIterator.next().asImpl().getSegment().getSegmentId());
        assertTrue(segmentIterator.hasNext());
        assertEquals(2L, segmentIterator.next().asImpl().getSegment().getSegmentId());
        assertFalse(segmentIterator.hasNext());
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
