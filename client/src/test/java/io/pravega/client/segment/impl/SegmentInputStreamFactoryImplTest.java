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
package io.pravega.client.segment.impl;

import io.pravega.client.connection.impl.ClientConnection;
import io.pravega.client.connection.impl.ConnectionPool;
import io.pravega.client.connection.impl.Flow;
import io.pravega.client.control.impl.Controller;
import io.pravega.client.security.auth.EmptyTokenProviderImpl;
import io.pravega.client.stream.mock.MockConnectionFactoryImpl;
import io.pravega.client.stream.mock.MockController;
import io.pravega.shared.protocol.netty.PravegaNodeUri;
import io.pravega.shared.protocol.netty.ReplyProcessor;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;

import lombok.Cleanup;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class SegmentInputStreamFactoryImplTest {

    @Mock
    private Controller controller;
    @Mock
    private ConnectionPool cp;
    @Mock
    private ClientConnection connection;
    @Mock
    private ScheduledExecutorService executor;

    @Before
    public void setup() {
        when(controller.getEndpointForSegment(anyString()))
                .thenReturn(CompletableFuture.completedFuture(new PravegaNodeUri("localhost", 9090)));
        when(cp.getClientConnection(any(Flow.class), any(PravegaNodeUri.class), any(ReplyProcessor.class)))
                .thenReturn(CompletableFuture.completedFuture(connection));
        when(cp.getInternalExecutor()).thenReturn(executor);
    }

    @Test
    public void createInputStreamForSegment() {
        SegmentInputStreamFactoryImpl factory = new SegmentInputStreamFactoryImpl(controller, cp);
        SegmentInputStream segmentInputStream = factory
                .createInputStreamForSegment(Segment.fromScopedName("scope/stream/0"), new EmptyTokenProviderImpl());
        assertEquals(0, segmentInputStream.getOffset());
    }

    @Test
    public void testCreateInputStreamForSegmentWithOffset() {
        SegmentInputStreamFactoryImpl factory = new SegmentInputStreamFactoryImpl(controller, cp);
        SegmentInputStream segmentInputStream = factory
                .createInputStreamForSegment(Segment
                        .fromScopedName("scope/stream/0"), new EmptyTokenProviderImpl(), 100);
        assertEquals(100, segmentInputStream.getOffset());
        assertEquals(Segment.fromScopedName("scope/stream/0"), segmentInputStream.getSegmentId());

    }

    @Test
    public void testCreateInputStreamForSegmentWithOffsetAndBufferSize() {
        MockConnectionFactoryImpl connectionFactory = new MockConnectionFactoryImpl();
        MockController mockController = new MockController("localhost", -1, connectionFactory, false);
        SegmentInputStreamFactoryImpl streamFactory = new SegmentInputStreamFactoryImpl(mockController, connectionFactory);
        @Cleanup
        EventSegmentReader reader = streamFactory.createEventReaderForSegment(Segment.fromScopedName("scope/stream/0"), 100L, 10);
        assertEquals(100, reader.getOffset());
        assertEquals(Segment.fromScopedName("scope/stream/0"), reader.getSegmentId());

    }
}