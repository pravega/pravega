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
package io.pravega.client.byteStream;

import com.google.common.base.Preconditions;
import io.pravega.client.ByteStreamClientFactory;
import io.pravega.client.byteStream.impl.BufferedByteStreamWriterImpl;
import io.pravega.client.byteStream.impl.ByteStreamClientImpl;
import io.pravega.client.connection.impl.ClientConnection;
import io.pravega.client.segment.impl.SegmentTruncatedException;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.impl.PendingEvent;
import io.pravega.client.stream.impl.StreamSegments;
import io.pravega.client.stream.mock.MockConnectionFactoryImpl;
import io.pravega.client.stream.mock.MockController;
import io.pravega.client.stream.mock.MockSegmentStreamFactory;
import io.pravega.common.concurrent.Futures;
import io.pravega.shared.protocol.netty.PravegaNodeUri;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

import lombok.Cleanup;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static io.pravega.test.common.AssertExtensions.assertThrows;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

public class ByteStreamWriterTest {

    private static final String SCOPE = "scope";
    private static final String STREAM = "stream";
    private MockConnectionFactoryImpl connectionFactory;
    private MockController controller;
    private ByteStreamClientFactory clientFactory;

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
        clientFactory = new ByteStreamClientImpl(SCOPE, controller, connectionFactory, streamFactory, streamFactory,
                                                 streamFactory);

        StreamSegments segments = Futures.getThrowingException(controller.getCurrentSegments(SCOPE, STREAM));
        Preconditions.checkState(segments.getNumberOfSegments() > 0, "Stream is sealed");
        Preconditions.checkState(segments.getNumberOfSegments() == 1, "Stream is configured with more than one segment");
    }

    @After
    public void teardown() {
        clientFactory.close();
        controller.close();
        connectionFactory.close();
    }

    @Test(timeout = 5000)
    public void testWrite() throws Exception {
        @Cleanup
        ByteStreamWriter writer = clientFactory.createByteStreamWriter(STREAM);
        byte[] value = new byte[] { 1, 2, 3, 4, 5, 6, 7 };
        int headoffset = 0;
        writer.write(value);
        writer.flush();
        assertEquals(headoffset, writer.fetchHeadOffset());
        assertEquals(value.length, writer.fetchTailOffset());
        writer.write(value);
        writer.write(value);
        headoffset = 5;
        writer.truncateDataBefore(headoffset);
        writer.flush();
        assertEquals(headoffset, writer.fetchHeadOffset());
        assertEquals(value.length * 3, writer.fetchTailOffset());

        headoffset = 10;
        writer.write(value, 0, value.length);
        writer.truncateDataBefore(headoffset);
        writer.flushAsync().join();
        assertEquals(headoffset, writer.fetchHeadOffset());
        assertEquals(value.length * 4, writer.fetchTailOffset());
    }

    @Test(timeout = 5000)
    public void testSingleByteWrite() throws Exception {
        @Cleanup
        ByteStreamWriter writer = clientFactory.createByteStreamWriter(STREAM);
        int numBytes = BufferedByteStreamWriterImpl.BUFFER_SIZE * 2 + 1;
        for (int i = 0; i < numBytes; i++) {
            writer.write(i);
        }
        writer.flush();
        assertEquals(numBytes, writer.fetchTailOffset());
    }

    @Test(timeout = 5000)
    public void testLargeWrite() throws Exception {
        @Cleanup
        ByteStreamWriter writer = clientFactory.createByteStreamWriter(STREAM);
        byte[] value = new byte[2 * PendingEvent.MAX_WRITE_SIZE + 10];
        Arrays.fill(value, (byte) 1);
        writer.write(value);
        writer.flush();
        assertEquals(value.length, writer.fetchTailOffset());
        Arrays.fill(value, (byte) 2);
        writer.write(value);
        Arrays.fill(value, (byte) 3);
        writer.write(value);
        writer.flush();
        assertEquals(value.length * 3L, writer.fetchTailOffset());
    }

    @Test
    public void testCloseAndSeal() throws IOException {
        @Cleanup
        ByteStreamWriter writer = clientFactory.createByteStreamWriter(STREAM);
        ByteBuffer toWrite = ByteBuffer.wrap(new byte[] { 0, 1, 2, 3, 4 });
        writer.write(toWrite);
        writer.closeAndSeal();
        assertEquals(5, writer.fetchTailOffset());
    }

    @Test
    public void testTruncate() throws IOException {
        @Cleanup
        ByteStreamWriter writer = clientFactory.createByteStreamWriter(STREAM);
        @Cleanup
        ByteStreamReader reader = clientFactory.createByteStreamReader(STREAM);

        ByteBuffer toWrite = ByteBuffer.wrap(new byte[] { 0, 1, 2, 3, 4 });
        writer.write(toWrite);
        writer.truncateDataBefore(4);

        reader.seekToOffset(3);
        assertThrows(SegmentTruncatedException.class, reader::read);
    }
}
