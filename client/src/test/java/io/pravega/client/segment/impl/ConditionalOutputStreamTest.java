/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.segment.impl;

import io.pravega.client.netty.impl.ClientConnection;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.mock.MockConnectionFactoryImpl;
import io.pravega.client.stream.mock.MockController;
import io.pravega.shared.protocol.netty.ConnectionFailedException;
import io.pravega.shared.protocol.netty.PravegaNodeUri;
import io.pravega.shared.protocol.netty.ReplyProcessor;
import io.pravega.shared.protocol.netty.WireCommands;
import io.pravega.shared.protocol.netty.WireCommands.AppendSetup;
import io.pravega.shared.protocol.netty.WireCommands.ConditionalAppend;
import io.pravega.shared.protocol.netty.WireCommands.SetupAppend;
import io.pravega.test.common.AssertExtensions;
import java.nio.ByteBuffer;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;

public class ConditionalOutputStreamTest {

    @Test(timeout = 5000)
    public void testWrite() throws ConnectionFailedException, SegmentSealedException {
        MockConnectionFactoryImpl connectionFactory = new MockConnectionFactoryImpl();
        MockController controller = new MockController("localhost", 0, connectionFactory, true);
        ConditionalOutputStreamFactory factory = new ConditionalOutputStreamFactoryImpl(controller, connectionFactory);
        Segment segment = new Segment("scope", "testWrite", 1);       
        ConditionalOutputStream cOut = factory.createConditionalOutputStream(segment, "token", EventWriterConfig.builder().build());
        ByteBuffer data = ByteBuffer.allocate(10);
        
        ClientConnection mock = Mockito.mock(ClientConnection.class);
        PravegaNodeUri location = new PravegaNodeUri("localhost", 0);
        connectionFactory.provideConnection(location, mock);
        setupAppend(connectionFactory, segment, mock, location);
        Mockito.doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                ConditionalAppend argument = (ConditionalAppend) invocation.getArgument(0);
                ReplyProcessor processor = connectionFactory.getProcessor(location);
                if (argument.getExpectedOffset() == 0 || argument.getExpectedOffset() == 2) {
                    processor.process(new WireCommands.DataAppended(argument.getWriterId(), argument.getEventNumber(), 0));
                } else { 
                    processor.process(new WireCommands.ConditionalCheckFailed(argument.getWriterId(), argument.getEventNumber()));
                }
                return null;
            }
        }).when(mock).sendAsync(any(ConditionalAppend.class), any(ClientConnection.CompletedCallback.class));
        assertTrue(cOut.write(data, 0));
        assertFalse(cOut.write(data, 1));
        assertTrue(cOut.write(data, 2));
        assertFalse(cOut.write(data, 3));
    }
    
    @Test
    public void testClose() throws SegmentSealedException {
        MockConnectionFactoryImpl connectionFactory = new MockConnectionFactoryImpl();
        MockController controller = new MockController("localhost", 0, connectionFactory, true);
        ConditionalOutputStreamFactory factory = new ConditionalOutputStreamFactoryImpl(controller, connectionFactory);
        Segment segment = new Segment("scope", "testWrite", 1);       
        ConditionalOutputStream cOut = factory.createConditionalOutputStream(segment, "token", EventWriterConfig.builder().build());
        cOut.close();
        AssertExtensions.assertThrows(IllegalStateException.class, () -> cOut.write(ByteBuffer.allocate(0), 0));
    }
    
    @Test
    public void testRetries() throws ConnectionFailedException, SegmentSealedException {
        MockConnectionFactoryImpl connectionFactory = new MockConnectionFactoryImpl();
        MockController controller = new MockController("localhost", 0, connectionFactory, true);
        ConditionalOutputStreamFactory factory = new ConditionalOutputStreamFactoryImpl(controller, connectionFactory);
        Segment segment = new Segment("scope", "testWrite", 1);       
        ConditionalOutputStream cOut = factory.createConditionalOutputStream(segment, "token", EventWriterConfig.builder().build());
        ByteBuffer data = ByteBuffer.allocate(10);
        
        ClientConnection mock = Mockito.mock(ClientConnection.class);
        PravegaNodeUri location = new PravegaNodeUri("localhost", 0);
        connectionFactory.provideConnection(location, mock);
        setupAppend(connectionFactory, segment, mock, location);
        final AtomicLong count = new AtomicLong(0);
        Mockito.doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                ConditionalAppend argument = (ConditionalAppend) invocation.getArgument(0);
                ReplyProcessor processor = connectionFactory.getProcessor(location);
                if (count.getAndIncrement() < 2) {
                    processor.connectionDropped();
                } else {
                    processor.process(new WireCommands.DataAppended(argument.getWriterId(), argument.getEventNumber(), 0));
                }                
                return null;
            }
        }).when(mock).sendAsync(any(ConditionalAppend.class), any(ClientConnection.CompletedCallback.class));
        assertTrue(cOut.write(data, 0));
        assertEquals(3, count.get());
    }

    private void setupAppend(MockConnectionFactoryImpl connectionFactory, Segment segment, ClientConnection mock,
                             PravegaNodeUri location) throws ConnectionFailedException {
        Mockito.doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                SetupAppend argument = (SetupAppend) invocation.getArgument(0);
                connectionFactory.getProcessor(location)
                                 .process(new AppendSetup(argument.getRequestId(), segment.getScopedName(),
                                                              argument.getWriterId(), 0));
                return null;
            }
        }).when(mock).sendAsync(any(SetupAppend.class), any(ClientConnection.CompletedCallback.class));
    }
    
    @Test(timeout = 10000)
    public void testSegmentSealed() throws ConnectionFailedException, SegmentSealedException {
        MockConnectionFactoryImpl connectionFactory = new MockConnectionFactoryImpl();
        MockController controller = new MockController("localhost", 0, connectionFactory, true);
        ConditionalOutputStreamFactory factory = new ConditionalOutputStreamFactoryImpl(controller, connectionFactory);
        Segment segment = new Segment("scope", "testWrite", 1);       
        ConditionalOutputStream cOut = factory.createConditionalOutputStream(segment, "token", EventWriterConfig.builder().build());
        ByteBuffer data = ByteBuffer.allocate(10);

        String mockClientReplyStackTrace = "SomeException";
        ClientConnection mock = Mockito.mock(ClientConnection.class);
        PravegaNodeUri location = new PravegaNodeUri("localhost", 0);
        connectionFactory.provideConnection(location, mock);
        setupAppend(connectionFactory, segment, mock, location);
        Mockito.doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                ConditionalAppend argument = (ConditionalAppend) invocation.getArgument(0);
                ReplyProcessor processor = connectionFactory.getProcessor(location);
                processor.process(new WireCommands.SegmentIsSealed(argument.getEventNumber(), segment.getScopedName(), mockClientReplyStackTrace));
                return null;
            }
        }).when(mock).sendAsync(any(ConditionalAppend.class), any(ClientConnection.CompletedCallback.class));
        AssertExtensions.assertThrows(SegmentSealedException.class, () -> cOut.write(data, 0));
    }
    
    /**
     * It is necessary to only have one outstanding conditional append per writerId to make sure the status can be resolved in the event of a reconnect.
     */
    @Test(timeout = 10000)
    public void testOnlyOneWriteAtATime() throws ConnectionFailedException, SegmentSealedException {
        MockConnectionFactoryImpl connectionFactory = new MockConnectionFactoryImpl();
        MockController controller = new MockController("localhost", 0, connectionFactory, true);
        ConditionalOutputStreamFactory factory = new ConditionalOutputStreamFactoryImpl(controller, connectionFactory);
        Segment segment = new Segment("scope", "testWrite", 1);       
        ConditionalOutputStream cOut = factory.createConditionalOutputStream(segment, "token", EventWriterConfig.builder().build());
        ByteBuffer data = ByteBuffer.allocate(10);
        ClientConnection mock = Mockito.mock(ClientConnection.class);
        PravegaNodeUri location = new PravegaNodeUri("localhost", 0);
        connectionFactory.provideConnection(location, mock);
        setupAppend(connectionFactory, segment, mock, location);
        LinkedBlockingQueue<Boolean> replies = new LinkedBlockingQueue<>();
        Mockito.doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                ConditionalAppend argument = (ConditionalAppend) invocation.getArgument(0);
                ReplyProcessor processor = connectionFactory.getProcessor(location);
                if (replies.take()) {                    
                    processor.process(new WireCommands.DataAppended(argument.getWriterId(), argument.getEventNumber(), 0));
                } else {
                    processor.process(new WireCommands.ConditionalCheckFailed(argument.getWriterId(), argument.getEventNumber()));
                }
                return null;
            }
        }).when(mock).sendAsync(any(ConditionalAppend.class), any(ClientConnection.CompletedCallback.class));
        replies.add(true);
        replies.add(false);
        assertTrue(cOut.write(data, 0));
        assertFalse(cOut.write(data, 1));
        AssertExtensions.assertBlocks(() -> {
            assertTrue(cOut.write(data, 2));
        }, () -> {
            replies.add(true);
        });
        AssertExtensions.assertBlocks(() -> {
            assertFalse(cOut.write(data, 3));
        }, () -> {
            replies.add(false);
        });
        AssertExtensions.assertBlocks(() -> {
            assertTrue(cOut.write(data, 4));
        }, () -> {
            AssertExtensions.assertBlocks(() -> {
                assertFalse(cOut.write(data, 5));
            }, () -> {
                replies.add(true);
                replies.add(false);
            });
        });
        AssertExtensions.assertBlocks(() -> {
            assertFalse(cOut.write(data, 6));
        }, () -> {
            AssertExtensions.assertBlocks(() -> {
                assertTrue(cOut.write(data, 7));
            }, () -> {
                replies.add(false);
                replies.add(true);
            });
        });
    }
    
}
