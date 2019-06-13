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
import io.pravega.client.netty.impl.ConnectionFactory;
import io.pravega.client.netty.impl.Flow;
import io.pravega.client.stream.mock.MockConnectionFactoryImpl;
import io.pravega.client.stream.mock.MockController;
import io.pravega.common.concurrent.Futures;
import io.pravega.shared.protocol.netty.ConnectionFailedException;
import io.pravega.shared.protocol.netty.PravegaNodeUri;
import io.pravega.shared.protocol.netty.ReplyProcessor;
import io.pravega.shared.protocol.netty.WireCommands;
import io.pravega.shared.protocol.netty.WireCommands.SegmentAttributeUpdated;
import io.pravega.shared.protocol.netty.WireCommands.SegmentTruncated;
import io.pravega.shared.protocol.netty.WireCommands.StreamSegmentInfo;
import io.pravega.test.common.InlineExecutor;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import lombok.Cleanup;
import org.junit.Test;
import org.mockito.InOrder;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;

public class SegmentMetadataClientTest {
    
    @Test(timeout = 10000)
    public void testCurrentStreamLength() {
        Segment segment = new Segment("scope", "testRetry", 4);
        PravegaNodeUri endpoint = new PravegaNodeUri("localhost", 0);
        @Cleanup
        MockConnectionFactoryImpl cf = new MockConnectionFactoryImpl();
        @Cleanup
        MockController controller = new MockController(endpoint.getEndpoint(), endpoint.getPort(), cf, true);
        @Cleanup
        ClientConnection connection = mock(ClientConnection.class);
        cf.provideConnection(endpoint, connection);
        @Cleanup
        SegmentMetadataClientImpl client = new SegmentMetadataClientImpl(segment, controller, cf, "");
        client.getConnection();
        ReplyProcessor processor = cf.getProcessor(endpoint);
        Mockito.doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                WireCommands.GetStreamSegmentInfo getStreamInfo = invocation.getArgument(0);
                processor.process(new StreamSegmentInfo(getStreamInfo.getRequestId(), segment.getScopedName(), true, false, false, 0,
                                                                  123, 121));
                return null;
            }
        }).when(connection).sendAsync(any(WireCommands.GetStreamSegmentInfo.class),
                                      Mockito.any(ClientConnection.CompletedCallback.class));
        long length = client.fetchCurrentSegmentLength();
        assertEquals(123, length);
    }
    
    @Test(timeout = 10000)
    public void testTruncate() {
        Segment segment = new Segment("scope", "testTruncate", 4);
        PravegaNodeUri endpoint = new PravegaNodeUri("localhost", 0);
        @Cleanup
        MockConnectionFactoryImpl cf = new MockConnectionFactoryImpl();
        @Cleanup
        MockController controller = new MockController(endpoint.getEndpoint(), endpoint.getPort(), cf, true);
        @Cleanup
        ClientConnection connection = mock(ClientConnection.class);
        cf.provideConnection(endpoint, connection);
        @Cleanup
        SegmentMetadataClientImpl client = new SegmentMetadataClientImpl(segment, controller, cf, "");
        client.getConnection();
        ReplyProcessor processor = cf.getProcessor(endpoint);
        AtomicLong requestId = new AtomicLong();
        Mockito.doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                WireCommands.TruncateSegment truncateSegment = invocation.getArgument(0);
                processor.process(new SegmentTruncated(truncateSegment.getRequestId(), segment.getScopedName()));
                requestId.set(truncateSegment.getRequestId());
                return null;
            }
        }).when(connection).sendAsync(any(WireCommands.TruncateSegment.class), Mockito.any(ClientConnection.CompletedCallback.class));
        client.truncateSegment(123L);
        Mockito.verify(connection).sendAsync(Mockito.eq(new WireCommands.TruncateSegment(requestId.get(), segment.getScopedName(), 123L, "")),
                                             Mockito.any(ClientConnection.CompletedCallback.class));
    }
    
    @Test(timeout = 10000)
    public void testSeal() {
        Segment segment = new Segment("scope", "testSeal", 4);
        PravegaNodeUri endpoint = new PravegaNodeUri("localhost", 0);
        @Cleanup
        MockConnectionFactoryImpl cf = new MockConnectionFactoryImpl();
        @Cleanup
        MockController controller = new MockController(endpoint.getEndpoint(), endpoint.getPort(), cf, true);
        @Cleanup
        ClientConnection connection = mock(ClientConnection.class);
        cf.provideConnection(endpoint, connection);
        @Cleanup
        SegmentMetadataClientImpl client = new SegmentMetadataClientImpl(segment, controller, cf, "");
        client.getConnection();
        ReplyProcessor processor = cf.getProcessor(endpoint);
        AtomicLong requestId = new AtomicLong();
        Mockito.doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                WireCommands.SealSegment sealSegment = invocation.getArgument(0);
                processor.process(new WireCommands.SegmentSealed(sealSegment.getRequestId(), segment.getScopedName()));
                requestId.set(sealSegment.getRequestId());
                return null;
            }
        }).when(connection).sendAsync(any(WireCommands.SealSegment.class), Mockito.any(ClientConnection.CompletedCallback.class));
        client.sealSegment();
        Mockito.verify(connection).sendAsync(Mockito.eq(new WireCommands.SealSegment(requestId.get(), segment.getScopedName(), "")),
                                             Mockito.any(ClientConnection.CompletedCallback.class));
    }  

    @Test(timeout = 10000)
    public void testGetProperty() throws Exception {
        UUID attributeId = SegmentAttribute.RevisionStreamClientMark.getValue();
        Segment segment = new Segment("scope", "testRetry", 4);
        PravegaNodeUri endpoint = new PravegaNodeUri("localhost", 0);
        @Cleanup
        MockConnectionFactoryImpl cf = new MockConnectionFactoryImpl();
        @Cleanup
        MockController controller = new MockController(endpoint.getEndpoint(), endpoint.getPort(), cf, true);
        @Cleanup
        ClientConnection connection = mock(ClientConnection.class);
        cf.provideConnection(endpoint, connection);
        @Cleanup
        SegmentMetadataClientImpl client = new SegmentMetadataClientImpl(segment, controller, cf, "");
        client.getConnection();
        ReplyProcessor processor = cf.getProcessor(endpoint);
        Mockito.doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                WireCommands.GetSegmentAttribute request = invocation.getArgument(0);
                processor.process(new WireCommands.SegmentAttribute(request.getRequestId(), 123));
                return null;
            }
        }).when(connection).sendAsync(any(WireCommands.GetSegmentAttribute.class), Mockito.any(ClientConnection.CompletedCallback.class));
        long value = client.fetchProperty(SegmentAttribute.RevisionStreamClientMark);
        assertEquals(123, value);
    }

    @Test(timeout = 10000)
    public void compareAndSetAttribute() {
        UUID attributeId = SegmentAttribute.RevisionStreamClientMark.getValue();
        Segment segment = new Segment("scope", "testRetry", 4);
        PravegaNodeUri endpoint = new PravegaNodeUri("localhost", 0);
        MockConnectionFactoryImpl cf = new MockConnectionFactoryImpl();
        MockController controller = new MockController(endpoint.getEndpoint(), endpoint.getPort(), cf, true);
        ClientConnection connection = mock(ClientConnection.class);
        cf.provideConnection(endpoint, connection);
        @Cleanup
        SegmentMetadataClientImpl client = new SegmentMetadataClientImpl(segment, controller, cf, "");
        client.getConnection();
        ReplyProcessor processor = cf.getProcessor(endpoint);
        Mockito.doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                WireCommands.UpdateSegmentAttribute request = invocation.getArgument(0);
                processor.process(new SegmentAttributeUpdated(request.getRequestId(), true));
                return null;
            }
        }).when(connection).sendAsync(any(WireCommands.UpdateSegmentAttribute.class), Mockito.any(ClientConnection.CompletedCallback.class));
        assertTrue(client.compareAndSetAttribute(SegmentAttribute.RevisionStreamClientMark, -1234, 1234));
    }

    @Test(timeout = 10000)
    public void testReconnects() throws Exception {
        Segment segment = new Segment("scope", "testRetry", 4);
        PravegaNodeUri endpoint = new PravegaNodeUri("localhost", 0);
        @Cleanup
        MockConnectionFactoryImpl cf = Mockito.spy(new MockConnectionFactoryImpl());
        @Cleanup
        MockController controller = new MockController(endpoint.getEndpoint(), endpoint.getPort(), cf, true);
        @Cleanup
        ClientConnection connection = mock(ClientConnection.class);
        cf.provideConnection(endpoint, connection);
        @Cleanup
        SegmentMetadataClientImpl client = new SegmentMetadataClientImpl(segment, controller, cf, "");
        client.getConnection();
        final List<Long> requestIds = new ArrayList<>();
        Mockito.doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                WireCommands.GetStreamSegmentInfo request = invocation.getArgument(0);
                requestIds.add(request.getRequestId());
                if (requestIds.size() == 1) {
                    ReplyProcessor processor = cf.getProcessor(endpoint);
                    processor.connectionDropped();
                } else {
                    ReplyProcessor processor = cf.getProcessor(endpoint);
                    processor.process(new StreamSegmentInfo(request.getRequestId(), segment.getScopedName(), true, false, false, 0,
                                                            123, 121));
                }
                return null;
            }
        }).when(connection).sendAsync(any(WireCommands.GetStreamSegmentInfo.class), Mockito.any(ClientConnection.CompletedCallback.class));

        long length = client.fetchCurrentSegmentLength();
        InOrder order = Mockito.inOrder(connection, cf);
        order.verify(cf).establishConnection(eq(endpoint), any(ReplyProcessor.class));
        order.verify(connection).sendAsync(Mockito.eq(new WireCommands.GetStreamSegmentInfo(requestIds.get(0), segment.getScopedName(), "")),
                                           Mockito.any(ClientConnection.CompletedCallback.class));
        order.verify(cf).establishConnection(eq(endpoint), any(ReplyProcessor.class));
        order.verify(connection).sendAsync(Mockito.eq(new WireCommands.GetStreamSegmentInfo(requestIds.get(1), segment.getScopedName(), "")),
                                           Mockito.any(ClientConnection.CompletedCallback.class));
        order.verify(cf).getProcessor(eq(endpoint));
        order.verifyNoMoreInteractions();
        assertEquals(123, length);
    }
    
    @Test(timeout = 10000)
    public void testExceptionOnSend() {
        Segment segment = new Segment("scope", "testRetry", 4);
        PravegaNodeUri endpoint = new PravegaNodeUri("localhost", 0);
        @Cleanup("shutdown")
        InlineExecutor executor = new InlineExecutor();
        @Cleanup
        ConnectionFactory cf = Mockito.mock(ConnectionFactory.class);
        Mockito.when(cf.getInternalExecutor()).thenReturn(executor);
        @Cleanup
        MockController controller = new MockController(endpoint.getEndpoint(), endpoint.getPort(), cf, true);
        ClientConnection connection1 = mock(ClientConnection.class);
        ClientConnection connection2 = mock(ClientConnection.class);
        AtomicReference<ReplyProcessor> processor = new AtomicReference<>();
        Mockito.when(cf.establishConnection(Mockito.any(Flow.class), Mockito.eq(endpoint), Mockito.any()))
               .thenReturn(Futures.failedFuture(new ConnectionFailedException()))
               .thenReturn(CompletableFuture.completedFuture(connection1))
               .thenAnswer(new Answer<CompletableFuture<ClientConnection>>() {
                   @Override
                   public CompletableFuture<ClientConnection> answer(InvocationOnMock invocation) throws Throwable {
                       processor.set(invocation.getArgument(2));
                       return CompletableFuture.completedFuture(connection2);
                   }
               });
        final List<Long> requestIds = new ArrayList<>();
        Mockito.doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                WireCommands.GetStreamSegmentInfo request = invocation.getArgument(0);
                requestIds.add(request.getRequestId());
                if (requestIds.size() == 1) {
                    ClientConnection.CompletedCallback callback = invocation.getArgument(1);
                    callback.complete(new ConnectionFailedException());
                } else {
                    processor.get().process(new StreamSegmentInfo(request.getRequestId(), segment.getScopedName(), true, false, false, 0,
                                                                  123, 121));
                }
                return null;
            }
        }).when(connection1).sendAsync(any(WireCommands.GetStreamSegmentInfo.class), Mockito.any(ClientConnection.CompletedCallback.class));
        Mockito.doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                WireCommands.GetStreamSegmentInfo request = invocation.getArgument(0);
                requestIds.add(request.getRequestId());
                processor.get().process(new StreamSegmentInfo(request.getRequestId(), segment.getScopedName(), true, false, false, 0,
                                                              123, 121));
                return null;
            }
        }).when(connection2).sendAsync(any(WireCommands.GetStreamSegmentInfo.class), Mockito.any(ClientConnection.CompletedCallback.class));
        @Cleanup
        SegmentMetadataClientImpl client = new SegmentMetadataClientImpl(segment, controller, cf, "");
        InOrder order = Mockito.inOrder(connection1, connection2, cf);
        long length = client.fetchCurrentSegmentLength();
        order.verify(cf, Mockito.times(2)).establishConnection(Mockito.any(Flow.class),  Mockito.eq(endpoint), Mockito.any());
        order.verify(connection1).sendAsync(Mockito.eq(new WireCommands.GetStreamSegmentInfo(requestIds.get(0), segment.getScopedName(), "")),
                                            Mockito.any(ClientConnection.CompletedCallback.class));
        order.verify(connection1).close();
        order.verify(cf).establishConnection(Mockito.any(Flow.class), Mockito.eq(endpoint), Mockito.any());
        order.verify(connection2).sendAsync(Mockito.eq(new WireCommands.GetStreamSegmentInfo(requestIds.get(1), segment.getScopedName(), "")),
                                            Mockito.any(ClientConnection.CompletedCallback.class));
        order.verifyNoMoreInteractions();
        assertEquals(123, length);
    }
    
}
