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

import io.pravega.client.ClientConfig;
import io.pravega.client.control.impl.Controller;
import io.pravega.client.security.auth.DelegationTokenProvider;
import io.pravega.client.security.auth.DelegationTokenProviderFactory;
import io.pravega.client.segment.impl.EndOfSegmentException;
import io.pravega.client.segment.impl.EventSegmentReader;
import io.pravega.client.segment.impl.Segment;
import io.pravega.client.segment.impl.SegmentInfo;
import io.pravega.client.segment.impl.SegmentInputStreamFactory;
import io.pravega.client.segment.impl.SegmentMetadataClient;
import io.pravega.client.segment.impl.SegmentMetadataClientFactory;
import io.pravega.client.segment.impl.SegmentOutputStream;
import io.pravega.client.segment.impl.SegmentTruncatedException;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.TruncatedDataException;
import io.pravega.client.stream.impl.JavaSerializer;
import io.pravega.client.stream.impl.PendingEvent;
import io.pravega.client.stream.mock.MockConnectionFactoryImpl;
import io.pravega.client.stream.mock.MockController;
import io.pravega.client.stream.mock.MockSegmentStreamFactory;
import io.pravega.shared.protocol.netty.PravegaNodeUri;
import io.pravega.test.common.AssertExtensions;

import java.util.NoSuchElementException;
import java.util.concurrent.CompletableFuture;
import lombok.Cleanup;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static io.pravega.test.common.AssertExtensions.assertThrows;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doReturn;

public class SegmentIteratorTest {

    private final JavaSerializer<String> stringSerializer = new JavaSerializer<>();

    private Controller controller = null;
    private MockConnectionFactoryImpl connectionFactory;
    private SegmentMetadataClientFactory metaFactory;

    @Before
    public void setUp() {
        PravegaNodeUri uri = new PravegaNodeUri("endpoint", 12345);
        this.connectionFactory = new MockConnectionFactoryImpl();
        this.metaFactory = mock(SegmentMetadataClientFactory.class);
        this.controller = new MockController(uri.getEndpoint(), uri.getPort(), connectionFactory, true);
    }

    @After
    public void tearDown() {
        if (this.controller != null) {
            this.controller.close();
        }
        if (this.connectionFactory != null) {
            this.connectionFactory.close();
        }
    }
    
    @Test(timeout = 5000)
    public void testHasNext() {
        MockSegmentStreamFactory factory = new MockSegmentStreamFactory();
        Segment segment = new Segment("Scope", "Stream", 1);
        EventWriterConfig config = EventWriterConfig.builder().build();
        SegmentOutputStream outputStream = factory.createOutputStreamForSegment(segment, c -> { }, config, DelegationTokenProviderFactory.createWithEmptyToken());
        sendData("1", outputStream);
        sendData("2", outputStream);
        sendData("3", outputStream);
        @Cleanup
        SegmentMetadataClient metadataClient = factory.createSegmentMetadataClient(segment, DelegationTokenProviderFactory.createWithEmptyToken());
        long length = metadataClient.getSegmentInfo().join().getWriteOffset();
        @Cleanup
        SegmentIteratorImpl<String> iter = new SegmentIteratorImpl<>(factory, metaFactory, controller, ClientConfig.builder().build(), segment, stringSerializer, 0, length);
        assertTrue(iter.hasNext());
        assertTrue(iter.hasNext());
        assertEquals("1", iter.next());
        assertEquals("2", iter.next());
        assertTrue(iter.hasNext());
        assertEquals("3", iter.next());
        assertFalse(iter.hasNext());
        assertThrows(NoSuchElementException.class, () -> iter.next());
        assertFalse(iter.hasNext());
    }

    @Test(timeout = 5000)
    public void testOffset() {
        MockSegmentStreamFactory factory = new MockSegmentStreamFactory();
        Segment segment = new Segment("Scope", "Stream", 1);
        EventWriterConfig config = EventWriterConfig.builder().build();
        SegmentOutputStream outputStream = factory.createOutputStreamForSegment(segment, c -> { }, config,
                DelegationTokenProviderFactory.createWithEmptyToken());
        sendData("1", outputStream);
        sendData("2", outputStream);
        sendData("3", outputStream);
        @Cleanup
        SegmentMetadataClient metadataClient = factory.createSegmentMetadataClient(segment,
                DelegationTokenProviderFactory.createWithEmptyToken());
        long length = metadataClient.getSegmentInfo().join().getWriteOffset();
        @Cleanup
        SegmentIteratorImpl<String> iter = new SegmentIteratorImpl<>(factory, metaFactory, controller, ClientConfig.builder().build(), segment, stringSerializer, 0, length);
        assertEquals(0, iter.getOffset());
        assertEquals("1", iter.next());
        assertEquals(length / 3, iter.getOffset());
        assertEquals("2", iter.next());
        assertEquals(length / 3 * 2, iter.getOffset());
        assertTrue(iter.hasNext());
        assertEquals(length / 3 * 2, iter.getOffset());
        assertEquals("3", iter.next());
        assertEquals(length, iter.getOffset());
        assertThrows(NoSuchElementException.class, () -> iter.next());
        assertFalse(iter.hasNext());
        assertEquals(length, iter.getOffset());
    }
    
    @Test(timeout = 5000)
    public void testTruncate() {
        MockSegmentStreamFactory factory = new MockSegmentStreamFactory();
        Segment segment = new Segment("Scope", "Stream", 1);
        EventWriterConfig config = EventWriterConfig.builder().build();
        SegmentOutputStream outputStream = factory.createOutputStreamForSegment(segment, c -> { }, config,
                DelegationTokenProviderFactory.createWithEmptyToken());
        sendData("1", outputStream);
        sendData("2", outputStream);
        sendData("3", outputStream);
        @Cleanup
        SegmentMetadataClient metadataClient = factory.createSegmentMetadataClient(segment,
                DelegationTokenProviderFactory.createWithEmptyToken());
        long length = metadataClient.getSegmentInfo().join().getWriteOffset();

        SegmentMetadataClientFactory metaFactory = mock(SegmentMetadataClientFactory.class);
        SegmentMetadataClient metaClient = mock(SegmentMetadataClient.class);
        when(metaFactory.createSegmentMetadataClient(any(Segment.class), any(DelegationTokenProvider.class))).thenReturn(metaClient);
        doReturn(CompletableFuture.completedFuture(10L)).when(metaClient).fetchCurrentSegmentHeadOffset();
        @Cleanup
        SegmentIteratorImpl<String> iter = new SegmentIteratorImpl<>(factory, metaFactory, controller, ClientConfig.builder().build(), segment, stringSerializer, 0, length);
        assertEquals("1", iter.next());
        long segmentLength = metadataClient.fetchCurrentSegmentLength().join();
        assertEquals(0, segmentLength % 3);
        metadataClient.truncateSegment(segmentLength * 2 / 3).join();
        AssertExtensions.assertThrows(TruncatedDataException.class, () -> iter.next());
        @Cleanup
        SegmentIteratorImpl<String> iter2 = new SegmentIteratorImpl<>(factory, metaFactory, controller, ClientConfig.builder().build(), segment, stringSerializer,
                                                                      segmentLength * 2 / 3, length);
        assertTrue(iter2.hasNext());
        assertEquals("3", iter2.next());
        assertFalse(iter.hasNext());
    }

    @Test(timeout = 5000)
    public void testTimeoutError() throws SegmentTruncatedException, EndOfSegmentException {
        Segment segment = new Segment("Scope", "Stream", 1);
        long endOffset = 10;
        SegmentInputStreamFactory factory = mock(SegmentInputStreamFactory.class);
        EventSegmentReader input = mock(EventSegmentReader.class);
        when(factory.createEventReaderForSegment(segment, 0, endOffset)).thenReturn(input);
        when(input.read()).thenReturn(null).thenReturn(stringSerializer.serialize("s"));

        SegmentMetadataClientFactory metaFactory = mock(SegmentMetadataClientFactory.class);
        SegmentMetadataClient metaClient = mock(SegmentMetadataClient.class);
        when(metaFactory.createSegmentMetadataClient(any(Segment.class), any(DelegationTokenProvider.class))).thenReturn(metaClient);
        doReturn(CompletableFuture.completedFuture(10L)).when(metaClient).fetchCurrentSegmentHeadOffset();
        @Cleanup
        SegmentIteratorImpl<String> iter = new SegmentIteratorImpl<>(factory, metaFactory, controller, ClientConfig.builder().build(), segment, stringSerializer, 0, endOffset);
        assertEquals("s", iter.next());
        verify(input, times(2)).read();
        when(input.read()).thenThrow(SegmentTruncatedException.class);
        assertThrows(TruncatedDataException.class, () -> iter.next());
        // Ensure fetchCurrentSegmentHeadOffset returns incompleteFuture.
        CompletableFuture<SegmentInfo> incompleteFuture = new CompletableFuture();
        doReturn(incompleteFuture).when(metaClient).fetchCurrentSegmentHeadOffset();
        ClientConfig clinetConfig = ClientConfig.builder().connectTimeoutMilliSec(1000).build();
        SegmentIteratorImpl<String> iter1 = new SegmentIteratorImpl<>(factory, metaFactory, controller, clinetConfig, segment, stringSerializer, 0, endOffset);
        assertThrows(TruncatedDataException.class, () -> iter1.next());
    }

    private void sendData(String data, SegmentOutputStream outputStream) {
        outputStream.write(PendingEvent.withHeader("routingKey", stringSerializer.serialize(data), new CompletableFuture<>()));
    }
    
}
