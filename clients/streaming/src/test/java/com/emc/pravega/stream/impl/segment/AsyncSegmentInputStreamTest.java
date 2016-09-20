/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.emc.pravega.stream.impl.segment;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import java.nio.ByteBuffer;

import org.junit.Test;

import com.emc.pravega.common.netty.ClientConnection;
import com.emc.pravega.common.netty.ConnectionFailedException;
import com.emc.pravega.common.netty.ReplyProcessor;
import com.emc.pravega.common.netty.WireCommands.ReadSegment;
import com.emc.pravega.common.netty.WireCommands.SegmentRead;
import com.emc.pravega.stream.impl.segment.AsyncSegmentInputStream.ReadFuture;
import com.emc.pravega.testcommon.Async;

import lombok.Cleanup;

public class AsyncSegmentInputStreamTest {

    @Test
    public void testRetry() throws ConnectionFailedException {
        String segment = "testRetry";
        String endpoint = "localhost";
        int port = 1234;
        TestConnectionFactoryImpl connectionFactory = new TestConnectionFactoryImpl(endpoint, port);
        @Cleanup
        AsyncSegmentInputStreamImpl in = new AsyncSegmentInputStreamImpl(connectionFactory, connectionFactory, segment);
        ClientConnection c = mock(ClientConnection.class);
        connectionFactory.provideConnection(endpoint, c);
        ReadFuture readFuture = in.read(1234, 5678);
        ReplyProcessor processor = connectionFactory.getProcessor(endpoint);
        verify(c).sendAsync(new ReadSegment(segment, 1234, 5678));
        processor.connectionDropped();
        verify(c).close();
        assertFalse(readFuture.isSuccess());
        SegmentRead segmentRead = new SegmentRead(segment, 1234, false, false, ByteBuffer.allocate(0));
        SegmentRead result = Async.testBlocking(() -> in.getResult(readFuture), () -> {
            processor.segmentRead(segmentRead);
        });
        verify(c).send(new ReadSegment(segment, 1234, 5678));
        assertTrue(readFuture.isSuccess());
        assertEquals(segmentRead, result);
        verifyNoMoreInteractions(c);
    }

    @Test
    public void testRead() throws ConnectionFailedException {
        String segment = "testRetry";
        String endpoint = "localhost";
        int port = 1234;

        TestConnectionFactoryImpl connectionFactory = new TestConnectionFactoryImpl(endpoint, port);
        @Cleanup
        AsyncSegmentInputStreamImpl in = new AsyncSegmentInputStreamImpl(connectionFactory, connectionFactory, segment);
        ClientConnection c = mock(ClientConnection.class);
        connectionFactory.provideConnection(endpoint, c);
        ReadFuture readFuture = in.read(1234, 5678);
        ReplyProcessor processor = connectionFactory.getProcessor(endpoint);
        verify(c).sendAsync(new ReadSegment(segment, 1234, 5678));
        SegmentRead segmentRead = new SegmentRead(segment, 1234, false, false, ByteBuffer.allocate(0));
        processor.segmentRead(segmentRead);
        assertTrue(readFuture.isSuccess());
        assertEquals(segmentRead, in.getResult(readFuture));
        verifyNoMoreInteractions(c);
    }

    @Test
    public void testWrongOffsetReturned() throws ConnectionFailedException {
        String segment = "testRetry";
        String endpoint = "localhost";
        int port = 1234;
        TestConnectionFactoryImpl connectionFactory = new TestConnectionFactoryImpl(endpoint, port);
        @Cleanup
        AsyncSegmentInputStreamImpl in = new AsyncSegmentInputStreamImpl(connectionFactory, connectionFactory, segment);
        ClientConnection c = mock(ClientConnection.class);
        connectionFactory.provideConnection(endpoint, c);
        ReadFuture readFuture = in.read(1234, 5678);
        ReplyProcessor processor = connectionFactory.getProcessor(endpoint);
        verify(c).sendAsync(new ReadSegment(segment, 1234, 5678));
        processor.segmentRead(new SegmentRead(segment, 1235, false, false, ByteBuffer.allocate(0)));
        assertFalse(readFuture.isSuccess());
        verifyNoMoreInteractions(c);
    }

}
