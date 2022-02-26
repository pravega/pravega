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
package io.pravega.client.byteStream.impl;

import com.google.common.base.Preconditions;
import io.pravega.client.connection.impl.ClientConnection;
import io.pravega.client.security.auth.DelegationTokenProvider;
import io.pravega.client.security.auth.DelegationTokenProviderFactory;
import io.pravega.client.segment.impl.Segment;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.impl.StreamSegments;
import io.pravega.client.stream.mock.MockConnectionFactoryImpl;
import io.pravega.client.stream.mock.MockController;
import io.pravega.client.stream.mock.MockSegmentStreamFactory;
import io.pravega.common.concurrent.Futures;
import io.pravega.shared.protocol.netty.PravegaNodeUri;
import io.pravega.shared.security.auth.AccessOperation;
import lombok.Cleanup;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.concurrent.CompletableFuture;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

public class ByteStreanWriterImplTest {

    private static final String SCOPE = "scope";
    private static final String STREAM = "stream";
    private MockConnectionFactoryImpl connectionFactory;
    private MockController controller;
    private ByteStreamWriterImpl mockWriter;

    @Before
    public void setup() {
        PravegaNodeUri endpoint = new PravegaNodeUri("localhost", 0);
        connectionFactory = new MockConnectionFactoryImpl();
        ClientConnection connection = mock(ClientConnection.class);
        connectionFactory.provideConnection(endpoint, connection);
        controller = new MockController(endpoint.getEndpoint(), endpoint.getPort(), connectionFactory, false);
        controller.createScope(SCOPE);
        controller.createStream(SCOPE, STREAM, StreamConfiguration.builder()
                .scalingPolicy(ScalingPolicy.fixed(1))
                .build());
        MockSegmentStreamFactory streamFactory = new MockSegmentStreamFactory();

        StreamSegments segments = Futures.getThrowingException(controller.getCurrentSegments(SCOPE, STREAM));
        Preconditions.checkState(segments.getNumberOfSegments() > 0, "Stream is sealed");
        Preconditions.checkState(segments.getNumberOfSegments() == 1, "Stream is configured with more than one segment");

        Segment segment = segments.getSegments().iterator().next();
        EventWriterConfig config = EventWriterConfig.builder().retryAttempts(Integer.MAX_VALUE).build();

        DelegationTokenProvider tokenProvider =
                DelegationTokenProviderFactory.create(controller, segment, AccessOperation.WRITE);

        mockWriter = new ByteStreamWriterImpl(streamFactory.createOutputStreamForSegment(segment, config, tokenProvider),
                streamFactory.createSegmentMetadataClient(segment, tokenProvider));
    }

    @After
    public void teardown() {
        controller.close();
        connectionFactory.close();
    }

    @Test(timeout = 5000)
    public void testAsyncFlush() throws Exception {
        @Cleanup
        ByteStreamWriterImpl writer = Mockito.spy(mockWriter);
        byte[] value = new byte[] { 1, 2, 3, 4, 5, 6, 7 };
        int headoffset = 0;
        writer.write(value);

        CompletableFuture<Void> firstFuture = new CompletableFuture<>();

        doReturn(firstFuture)
                .when(writer).updateLastEventFuture();
        CompletableFuture<Void> firstFlushAsync = writer.flushAsync();
        writer.write(value);
        writer.write(value);
        writer.write(value);
        firstFuture.complete(null);
        firstFlushAsync.join();

        reset(writer);
        CompletableFuture<Void> secondFlushAsync = writer.flushAsync();

        assertEquals(headoffset, writer.fetchHeadOffset());
        assertEquals(value.length * 4, writer.fetchTailOffset());
        secondFlushAsync.join();
        assertEquals(headoffset, writer.fetchHeadOffset());
        assertEquals(value.length * 4, writer.fetchTailOffset());
    }

    @Test(timeout = 5000)
    public void testAsyncAndSyncFlush() throws Exception {
        @Cleanup
        ByteStreamWriterImpl writer = Mockito.spy(mockWriter);
        byte[] value = new byte[] { 1, 2, 3, 4, 5, 6, 7 };
        writer.write(value);

        for (int i = 0; i < 1000; i++) {
            writer.write(value);
        }
        writer.flush();

        writer.write(value);
        writer.flushAsync();
        assertEquals(value.length * 1002, writer.fetchTailOffset());
    }
}
