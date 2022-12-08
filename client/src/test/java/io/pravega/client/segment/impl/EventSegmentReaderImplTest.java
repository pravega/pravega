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


import io.pravega.shared.protocol.netty.WireCommandType;
import io.pravega.shared.protocol.netty.WireCommands;
import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class EventSegmentReaderImplTest {

    @Rule
    public Timeout globalTimeout = new Timeout(10, TimeUnit.SECONDS);

    @Test
    public void testSuccess() throws SegmentTruncatedException, EndOfSegmentException {
        // Setup Mocks
        SegmentInputStream segmentInputStream = mock(SegmentInputStream.class);
        @Cleanup
        EventSegmentReaderImpl segmentReader = new EventSegmentReaderImpl(segmentInputStream);
        doAnswer(i -> {
            ByteBuffer headerReadingBuffer = i.getArgument(0);
            headerReadingBuffer.putInt(WireCommandType.EVENT.getCode());
            headerReadingBuffer.putInt(8);
            return WireCommands.TYPE_PLUS_LENGTH_SIZE;
        }).when(segmentInputStream).read(any(ByteBuffer.class), eq(1000L));
        doAnswer(i -> {
            ByteBuffer headerReadingBuffer = i.getArgument(0);
            headerReadingBuffer.putLong(12345678);
            return 8;
        }).when(segmentInputStream).read(any(ByteBuffer.class), eq(EventSegmentReaderImpl.PARTIAL_DATA_TIMEOUT));
        when(segmentInputStream.getSegmentId()).thenReturn(new Segment("scope", "stream", 0L));

        // Invoke read.
        ByteBuffer readData = segmentReader.read(1000);
        assertNotNull(readData);
        assertEquals(readData.remaining(), 8);
        assertEquals(12345678, readData.getLong());
    }
    
    @Test
    public void testHeaderTimeout() throws SegmentTruncatedException, EndOfSegmentException {
        // Setup Mocks
        SegmentInputStream segmentInputStream = mock(SegmentInputStream.class);
        @Cleanup
        EventSegmentReaderImpl segmentReader = new EventSegmentReaderImpl(segmentInputStream);
        //return a value less than WireCommands.TYPE_PLUS_LENGTH_SIZE = 8 bytes.
        when(segmentInputStream.read(any(ByteBuffer.class), eq(1000L))).thenReturn(5);
        // simulate a timeout while reading the remaining part of the headers.
        when(segmentInputStream.read(any(ByteBuffer.class), eq(EventSegmentReaderImpl.PARTIAL_DATA_TIMEOUT))).thenReturn(0);
        when(segmentInputStream.getSegmentId()).thenReturn(new Segment("scope", "stream", 0L));
        // Invoke read.
        ByteBuffer readData = segmentReader.read(1000);
        assertNull(readData);
        verify(segmentInputStream, times(1)).setOffset(0L, true);
    }

    @Test
    public void testEventDataTimeout() throws SegmentTruncatedException, EndOfSegmentException {
        // Setup Mocks
        SegmentInputStream segmentInputStream = mock(SegmentInputStream.class);
        @Cleanup
        EventSegmentReaderImpl segmentReader = new EventSegmentReaderImpl(segmentInputStream);
        doAnswer(i -> {
            ByteBuffer headerReadingBuffer = i.getArgument(0);
            headerReadingBuffer.putInt(WireCommandType.EVENT.getCode());
            headerReadingBuffer.putInt(10);
            return WireCommands.TYPE_PLUS_LENGTH_SIZE;
        }).when(segmentInputStream).read(any(ByteBuffer.class), eq(1000L));
        // simulate a timeout while reading the remaining data.
        when(segmentInputStream.read(any(ByteBuffer.class), eq(EventSegmentReaderImpl.PARTIAL_DATA_TIMEOUT))).thenReturn(0);
        when(segmentInputStream.getSegmentId()).thenReturn(new Segment("scope", "stream", 0L));

        // Invoke read.
        ByteBuffer readData = segmentReader.read(1000);
        assertNull(readData);
        verify(segmentInputStream, times(1)).setOffset(0L, true);
        verify(segmentInputStream, times(0)).setOffset(0);
    }

    @Test
    public void testEventDataTimeoutZeroLength() throws SegmentTruncatedException, EndOfSegmentException {
        // Setup Mocks
        SegmentInputStream segmentInputStream = mock(SegmentInputStream.class);
        @Cleanup
        EventSegmentReaderImpl segmentReader = new EventSegmentReaderImpl(segmentInputStream);
        doAnswer(i -> {
            ByteBuffer headerReadingBuffer = i.getArgument(0);
            headerReadingBuffer.putInt(WireCommandType.EVENT.getCode());
            headerReadingBuffer.putInt(0);
            return WireCommands.TYPE_PLUS_LENGTH_SIZE;
        }).when(segmentInputStream).read(any(ByteBuffer.class), eq(1000L));
        // simulate a timeout while reading the remaining data.
        when(segmentInputStream.read(any(ByteBuffer.class), eq(EventSegmentReaderImpl.PARTIAL_DATA_TIMEOUT))).thenReturn(0);
        when(segmentInputStream.getSegmentId()).thenReturn(new Segment("scope", "stream", 0L));

        // Invoke read.
        ByteBuffer readData = segmentReader.read(1000);
        assertEquals("Empty message received from Segment store", 0, readData.capacity());
        verify(segmentInputStream, times(0)).setOffset(0);
    }

    @Test
    public void testEventDataPartialTimeout() throws SegmentTruncatedException, EndOfSegmentException {
        // Setup Mocks
        SegmentInputStream segmentInputStream = mock(SegmentInputStream.class);
        @Cleanup
        EventSegmentReaderImpl segmentReader = new EventSegmentReaderImpl(segmentInputStream);
        doAnswer(i -> {
            ByteBuffer headerReadingBuffer = i.getArgument(0);
            headerReadingBuffer.putInt(WireCommandType.EVENT.getCode());
            headerReadingBuffer.putInt(10);
            return WireCommands.TYPE_PLUS_LENGTH_SIZE;
        }).when(segmentInputStream).read(any(ByteBuffer.class), eq(1000L));
        // simulate a partial read followed by timeout.
        doAnswer(i -> {
            ByteBuffer headerReadingBuffer = i.getArgument(0);
            // append 5 bytes. 5 Bytes are remaining.
            headerReadingBuffer.put((byte) 0x01);
            headerReadingBuffer.put((byte) 0x01);
            headerReadingBuffer.put((byte) 0x01);
            headerReadingBuffer.put((byte) 0x01);
            headerReadingBuffer.put((byte) 0x01);
            return 5;
        }).doReturn(0) // the second invocation should cause a timeout.
          .when(segmentInputStream).read(any(ByteBuffer.class), eq(EventSegmentReaderImpl.PARTIAL_DATA_TIMEOUT));
        when(segmentInputStream.getSegmentId()).thenReturn(new Segment("scope", "stream", 0L));

        // Invoke read.
        ByteBuffer readData = segmentReader.read(1000);
        assertNull(readData);
        verify(segmentInputStream, times(1)).setOffset(0L, true);
        verify(segmentInputStream, times(0)).setOffset(0);
    }
}
