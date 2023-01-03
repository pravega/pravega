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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.pravega.client.connection.impl.ClientConnection;
import io.pravega.client.segment.impl.EndOfSegmentException;
import io.pravega.client.segment.impl.EventSegmentReader;
import io.pravega.client.segment.impl.Segment;
import io.pravega.client.segment.impl.SegmentInputStreamFactoryImpl;
import io.pravega.client.segment.impl.SegmentTruncatedException;
import io.pravega.client.stream.EventPointer;
import io.pravega.client.stream.mock.MockConnectionFactoryImpl;
import io.pravega.client.stream.mock.MockController;
import io.pravega.shared.protocol.netty.ConnectionFailedException;
import io.pravega.shared.protocol.netty.PravegaNodeUri;
import io.pravega.shared.protocol.netty.ReplyProcessor;
import io.pravega.shared.protocol.netty.WireCommands;
import io.pravega.shared.protocol.netty.WireCommands.ReadSegment;
import io.pravega.shared.protocol.netty.WireCommands.SegmentRead;
import io.pravega.test.common.AssertExtensions;
import java.io.IOException;
import java.nio.ByteBuffer;
import lombok.Cleanup;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.times;

public class EventPointerTest {

    /**
     * Simple exercise of event pointers. The test case creates an impl instance with some arbitrary
     * values, serializes the pointer, deserializes it, and asserts that the values obtained are the
     * expected ones.
     * 
     * @throws IOException
     * @throws ClassNotFoundException
     */
    @Test
    public void testEventPointerImpl() throws IOException, ClassNotFoundException {
        String scope = "testScope";
        String stream = "testStream";
        int segmentId = 1;
        Segment segment = new Segment(scope, stream, segmentId);
        EventPointer pointer = new EventPointerImpl(segment, 10L, 10);
        EventPointer pointerRead = EventPointer.fromBytes(pointer.toBytes());
        assertEquals(pointer, pointerRead);

        StringBuilder name = new StringBuilder();
        name.append(scope);
        name.append("/");
        name.append(stream);
        assertEquals(name.toString(), pointerRead.asImpl().getSegment().getScopedStreamName());
        assertEquals(name.toString(), pointer.getStream().getScopedName());

        name.append("/");
        name.append(segmentId);
        name.append(".#epoch.0");
        assertEquals(name.toString(), pointerRead.asImpl().getSegment().getScopedName());

        assertTrue(pointerRead.asImpl().getEventStartOffset() == 10L);
        assertTrue(pointerRead.asImpl().getEventLength() == 10);
    }

    @Test(timeout = 5000)
    public void testReadEventPointer() throws EndOfSegmentException, SegmentTruncatedException,
                                       ConnectionFailedException {
        byte[] event = new byte[100];
        Segment segment = Segment.fromScopedName("scope/stream/0");
        PravegaNodeUri uri = new PravegaNodeUri("localhost", -1);
        @Cleanup
        MockConnectionFactoryImpl connectionFactory = new MockConnectionFactoryImpl();
        ClientConnection connection = Mockito.mock(ClientConnection.class);
        connectionFactory.provideConnection(uri, connection);
        doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                ReadSegment request = (ReadSegment) invocation.getArgument(0);
                assertEquals(123, request.getOffset());
                assertEquals(event.length + WireCommands.TYPE_PLUS_LENGTH_SIZE, request.getSuggestedLength());
                ReplyProcessor processor = connectionFactory.getProcessor(uri);
                ByteBuf data = new WireCommands.Event(Unpooled.wrappedBuffer(event)).getAsByteBuf();
                processor.segmentRead(new SegmentRead(request.getSegment(), 123, false, false,
                                                      data, request.getRequestId()));
                return null;
            }
        }).when(connection).send(any(ReadSegment.class));

        MockController mockController = new MockController("localhost", -1, connectionFactory, false);
        SegmentInputStreamFactoryImpl streamFactory = new SegmentInputStreamFactoryImpl(mockController,
                                                                                        connectionFactory);
        EventSegmentReaderUtility utility = new EventSegmentReaderUtility(streamFactory);
        EventPointerImpl pointer = new EventPointerImpl(segment, 123, event.length + WireCommands.TYPE_PLUS_LENGTH_SIZE);
        EventSegmentReader segmentReader = utility.createEventSegmentReader(pointer);
        ByteBuffer read = segmentReader.read();
        assertEquals(event.length, read.remaining());
        assertEquals(ByteBuffer.wrap(event), read);
        AssertExtensions.assertThrows(EndOfSegmentException.class, () -> segmentReader.read());
        segmentReader.close();
        Mockito.verify(connection, times(1)).send(any(ReadSegment.class));
        Mockito.verify(connection, times(1)).close();
        Mockito.verifyNoMoreInteractions(connection);
    }

}
