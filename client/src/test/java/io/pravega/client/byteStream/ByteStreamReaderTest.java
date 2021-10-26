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

import io.pravega.client.ByteStreamClientFactory;
import io.pravega.client.byteStream.impl.ByteStreamClientImpl;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.mock.MockConnectionFactoryImpl;
import io.pravega.client.stream.mock.MockController;
import io.pravega.client.stream.mock.MockSegmentStreamFactory;
import io.pravega.shared.protocol.netty.PravegaNodeUri;
import io.pravega.test.common.AssertExtensions;
import lombok.Cleanup;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class ByteStreamReaderTest {

    private static final String SCOPE = "scope";
    private static final String STREAM = "stream";
    private MockConnectionFactoryImpl connectionFactory;
    private MockController controller;
    private ByteStreamClientFactory clientFactory;

    @Before
    public void setup() {
        PravegaNodeUri endpoint = new PravegaNodeUri("localhost", 0);
        connectionFactory = new MockConnectionFactoryImpl();
        controller = new MockController(endpoint.getEndpoint(), endpoint.getPort(), connectionFactory, false);
        controller.createScope(SCOPE);
        controller.createStream(SCOPE, STREAM, StreamConfiguration.builder()
                                                   .scalingPolicy(ScalingPolicy.fixed(1))
                                                   .build());
        MockSegmentStreamFactory streamFactory = new MockSegmentStreamFactory();
        clientFactory = new ByteStreamClientImpl(SCOPE, controller, connectionFactory, streamFactory, streamFactory,
                                              streamFactory);
    }

    @After
    public void teardown() {
        clientFactory.close();
        controller.close();
        connectionFactory.close();
    }
    
    @Test(timeout = 5000)
    public void testReadWritten() throws Exception {
        @Cleanup
        ByteStreamWriter writer = clientFactory.createByteStreamWriter(STREAM);
        byte[] value = new byte[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 };
        int headOffset = 0;
        writer.write(value);
        writer.flush();
        @Cleanup
        ByteStreamReader reader = clientFactory.createByteStreamReader(STREAM);
        for (int i = 0; i < 10; i++) {
            assertEquals(i, reader.read());
        }
        assertEquals(headOffset, reader.fetchHeadOffset());
        assertEquals(value.length, reader.fetchTailOffset());
        headOffset = 3;
        writer.truncateDataBefore(headOffset);
        writer.write(value);
        writer.flush();
        assertEquals(headOffset, reader.fetchHeadOffset());
        assertEquals(value.length * 2, reader.fetchTailOffset());
        byte[] read = new byte[5];
        assertEquals(5, reader.read(read));
        assertArrayEquals(new byte[] { 0, 1, 2, 3, 4 }, read);
        assertEquals(2, reader.read(read, 2, 2));
        assertArrayEquals(new byte[] { 0, 1, 5, 6, 4 }, read);
        assertEquals(3, reader.read(read, 2, 3));
        assertArrayEquals(new byte[] { 0, 1, 7, 8, 9 }, read);
        assertEquals(-1, reader.read(read, 2, 3));
        assertArrayEquals(new byte[] { 0, 1, 7, 8, 9 }, read);
    }

    @Test(timeout = 5000)
    public void testAvailable() throws Exception {
        @Cleanup
        ByteStreamWriter writer = clientFactory.createByteStreamWriter(STREAM);
        @Cleanup
        ByteStreamReader reader = clientFactory.createByteStreamReader(STREAM);
        assertEquals(0, reader.available());
        byte[] value = new byte[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 };
        writer.write(value);
        writer.flush();
        assertEquals(10, reader.available());
    }

    @Test(timeout = 5000)
    public void testSkip() throws Exception {
        @Cleanup
        ByteStreamWriter writer = clientFactory.createByteStreamWriter(STREAM);
        @Cleanup
        ByteStreamReader reader = clientFactory.createByteStreamReader(STREAM);
        for (int i = 0; i < 5; i++) {
            writer.write(new byte[] { (byte) i });
        }
        assertEquals(1, reader.skip(1));
        assertEquals(1, reader.getOffset());
        byte[] read = new byte[2];
        assertEquals(2, reader.read(read));
        assertEquals(3, reader.getOffset());
        assertArrayEquals(new byte[] { 1, 2 }, read);
        long skipped = reader.skip(10);
        assertEquals(2, skipped);
        assertEquals(-1, reader.read());
        reader.seekToOffset(0);
        assertEquals(0, reader.getOffset());
        assertEquals(5, reader.available());
    }

    @Test
    public void testInvalidStream() {
        String stream1 = "invalidstream1";
        controller.createStream(SCOPE, stream1, StreamConfiguration.builder()
                                                                  .scalingPolicy(ScalingPolicy.fixed(2))
                                                                  .build());
        AssertExtensions.assertThrows(IllegalStateException.class, () -> clientFactory.createByteStreamReader(stream1));
        AssertExtensions.assertThrows(IllegalStateException.class, () -> clientFactory.createByteStreamWriter(stream1));
        String stream2 = "invalidstream2";
        controller.createStream(SCOPE, stream2, StreamConfiguration.builder()
                                                                  .scalingPolicy(ScalingPolicy.fixed(1))
                                                                  .build());
        controller.sealStream(SCOPE, stream2);
        clientFactory.createByteStreamReader(stream2);
        AssertExtensions.assertThrows(IllegalStateException.class, () -> clientFactory.createByteStreamWriter(stream2));
    }
}
