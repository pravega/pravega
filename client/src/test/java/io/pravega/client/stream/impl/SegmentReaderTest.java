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

package io.pravega.client.stream.impl;


import io.pravega.client.ClientConfig;
import io.pravega.client.control.impl.Controller;
import io.pravega.client.security.auth.DelegationTokenProviderFactory;
import io.pravega.client.segment.impl.EndOfSegmentException;
import io.pravega.client.segment.impl.Segment;
import io.pravega.client.segment.impl.SegmentOutputStream;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.SegmentReader;
import io.pravega.client.stream.SegmentReaderSnapshot;
import io.pravega.client.stream.mock.MockConnectionFactoryImpl;
import io.pravega.client.stream.mock.MockController;
import io.pravega.client.stream.mock.MockSegmentStreamFactory;
import io.pravega.shared.protocol.netty.PravegaNodeUri;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.CompletableFuture;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

/**
 * Test case for
 */
public class SegmentReaderTest {

    private final JavaSerializer<String> stringSerializer = new JavaSerializer<>();

    private Controller controller = null;
    private MockConnectionFactoryImpl connectionFactory;


    @Before
    public void setUp() throws Exception {
        PravegaNodeUri uri = new PravegaNodeUri("endpoint", 12345);
        this.connectionFactory = new MockConnectionFactoryImpl();
        this.controller = new MockController(uri.getEndpoint(), uri.getPort(), connectionFactory, true);
    }

    @After
    public void tearDown() throws Exception {
        if (this.controller != null) {
            this.controller.close();
        }
        if (this.connectionFactory != null) {
            this.connectionFactory.close();
        }
    }

    @Test(timeout = 10000)
    public void read() throws EndOfSegmentException {
        long timeout = 1000;
        MockSegmentStreamFactory factory = new MockSegmentStreamFactory();
        Segment segment = new Segment("Scope", "Stream", 1);
        EventWriterConfig config = EventWriterConfig.builder().build();
        SegmentOutputStream outputStream = factory.createOutputStreamForSegment(segment, c -> { }, config, DelegationTokenProviderFactory.createWithEmptyToken());
        sendData("1", outputStream);
        sendData("2", outputStream);
        sendData("3", outputStream);
        SegmentReader<String> segmentReader = new SegmentReaderImpl<>(factory, segment, stringSerializer, 0,
                ClientConfig.builder().build(), controller, factory);

        checkSnapshot(segment, 0, false, segmentReader.getSnapshot());

        String event = segmentReader.read(timeout);
        assertEquals("1", event);

        checkSnapshot(segment, 16, false, segmentReader.getSnapshot());

        event = segmentReader.read(timeout);
        assertEquals("2", event);

        checkSnapshot(segment, 32, false, segmentReader.getSnapshot());

        event = segmentReader.read(timeout);
        assertEquals("3", event);

        assertThrows("Read event", EndOfSegmentException.class, () -> segmentReader.read(timeout));

        checkSnapshot(segment, 48, true, segmentReader.getSnapshot());

    }

    private void sendData(String data, SegmentOutputStream outputStream) {
        outputStream.write(PendingEvent.withHeader("routingKey", stringSerializer.serialize(data), new CompletableFuture<>()));
    }

    private void checkSnapshot(Segment expectedSegment, long expectedPosition, boolean expectedIsEndOfSegment,
                               SegmentReaderSnapshot segmentReaderSnapshot) {
        assertEquals(expectedSegment, segmentReaderSnapshot.getSegment());
        assertEquals(expectedIsEndOfSegment, segmentReaderSnapshot.isEndOfSegment());
        assertEquals(expectedPosition, segmentReaderSnapshot.getPosition());
    }
}