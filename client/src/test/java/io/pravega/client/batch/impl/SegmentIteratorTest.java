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

import io.pravega.client.security.auth.DelegationTokenProviderFactory;
import io.pravega.client.segment.impl.EndOfSegmentException;
import io.pravega.client.segment.impl.EventSegmentReader;
import io.pravega.client.segment.impl.Segment;
import io.pravega.client.segment.impl.SegmentInputStreamFactory;
import io.pravega.client.segment.impl.SegmentMetadataClient;
import io.pravega.client.segment.impl.SegmentOutputStream;
import io.pravega.client.segment.impl.SegmentTruncatedException;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.TruncatedDataException;
import io.pravega.client.stream.impl.JavaSerializer;
import io.pravega.client.stream.impl.PendingEvent;
import io.pravega.client.stream.mock.MockSegmentStreamFactory;
import io.pravega.test.common.AssertExtensions;

import java.util.NoSuchElementException;
import java.util.concurrent.CompletableFuture;
import lombok.Cleanup;
import org.junit.Test;

import static io.pravega.test.common.AssertExtensions.assertThrows;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class SegmentIteratorTest {

    private final JavaSerializer<String> stringSerializer = new JavaSerializer<>();
    
    @Test(timeout = 5000)
    public void testHasNext() {
        MockSegmentStreamFactory factory = new MockSegmentStreamFactory();
        Segment segment = new Segment("Scope", "Stream", 1);
        EventWriterConfig config = EventWriterConfig.builder().build();
        SegmentOutputStream outputStream = factory.createOutputStreamForSegment(segment, c -> { }, config, DelegationTokenProviderFactory.createWithEmptyToken());
        sendData("1", outputStream);
        sendData("2", outputStream);
        sendData("3", outputStream);
        SegmentMetadataClient metadataClient = factory.createSegmentMetadataClient(segment, DelegationTokenProviderFactory.createWithEmptyToken());
        long length = metadataClient.getSegmentInfo().getWriteOffset();
        @Cleanup
        SegmentIteratorImpl<String> iter = new SegmentIteratorImpl<>(factory, segment, stringSerializer, 0, length);
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
        SegmentMetadataClient metadataClient = factory.createSegmentMetadataClient(segment,
                DelegationTokenProviderFactory.createWithEmptyToken());
        long length = metadataClient.getSegmentInfo().getWriteOffset();
        @Cleanup
        SegmentIteratorImpl<String> iter = new SegmentIteratorImpl<>(factory, segment, stringSerializer, 0, length);
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
        SegmentMetadataClient metadataClient = factory.createSegmentMetadataClient(segment,
                DelegationTokenProviderFactory.createWithEmptyToken());
        long length = metadataClient.getSegmentInfo().getWriteOffset();
        @Cleanup
        SegmentIteratorImpl<String> iter = new SegmentIteratorImpl<>(factory, segment, stringSerializer, 0, length);
        assertEquals("1", iter.next());
        long segmentLength = metadataClient.fetchCurrentSegmentLength();
        assertEquals(0, segmentLength % 3);
        metadataClient.truncateSegment(segmentLength * 2 / 3);
        AssertExtensions.assertThrows(TruncatedDataException.class, () -> iter.next());
        @Cleanup
        SegmentIteratorImpl<String> iter2 = new SegmentIteratorImpl<>(factory, segment, stringSerializer,
                                                                      segmentLength * 2 / 3, length);
        assertTrue(iter2.hasNext());
        assertEquals("3", iter2.next());
        assertFalse(iter.hasNext());
    }

    @Test(timeout = 5000)
    public void testTimeoutError() throws SegmentTruncatedException, EndOfSegmentException {
        Segment segment = new Segment("Scope", "Stream", 1);
        int endOffset = 10;
        SegmentInputStreamFactory factory = mock(SegmentInputStreamFactory.class);
        EventSegmentReader input = mock(EventSegmentReader.class);
        when(factory.createEventReaderForSegment(segment, 0, endOffset)).thenReturn(input);
        when(input.read()).thenReturn(null).thenReturn(stringSerializer.serialize("s"));
        @Cleanup
        SegmentIteratorImpl<String> iter = new SegmentIteratorImpl<>(factory, segment, stringSerializer, 0, endOffset);
        assertEquals("s", iter.next());
        verify(input, times(2)).read();
        when(input.read()).thenThrow(SegmentTruncatedException.class);
        assertThrows(TruncatedDataException.class, () -> iter.next());
    }

    private void sendData(String data, SegmentOutputStream outputStream) {
        outputStream.write(PendingEvent.withHeader("routingKey", stringSerializer.serialize(data), new CompletableFuture<>()));
    }
    
}
