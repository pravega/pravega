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
import io.pravega.client.stream.mock.MockConnectionFactoryImpl;
import io.pravega.client.stream.mock.MockController;
import io.pravega.shared.protocol.netty.PravegaNodeUri;
import io.pravega.shared.protocol.netty.ReplyProcessor;
import io.pravega.shared.protocol.netty.WireCommands;
import io.pravega.shared.protocol.netty.WireCommands.SegmentAttributeUpdated;
import io.pravega.shared.protocol.netty.WireCommands.StreamSegmentInfo;
import java.util.UUID;
import org.junit.Test;
import org.mockito.InOrder;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

public class SegmentMetadataClientTest {
    
    @Test(timeout = 10000)
    public void testCurrentStreamLength() throws Exception {
        Segment segment = new Segment("scope", "testRetry", 4);
        PravegaNodeUri endpoint = new PravegaNodeUri("localhost", 0);
        MockConnectionFactoryImpl cf = new MockConnectionFactoryImpl();
        MockController controller = new MockController(endpoint.getEndpoint(), endpoint.getPort(), cf);
        ClientConnection connection = mock(ClientConnection.class);
        cf.provideConnection(endpoint, connection);
        SegmentMetadataClientImpl client = new SegmentMetadataClientImpl(segment, controller, cf);
        client.getConnection();
        ReplyProcessor processor = cf.getProcessor(endpoint);
        Mockito.doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                processor.streamSegmentInfo(new StreamSegmentInfo(1, segment.getScopedName(), true, false, false, 0,
                                                                  123));
                return null;
            }
        }).when(connection).send(new WireCommands.GetStreamSegmentInfo(1, segment.getScopedName()));
        long length = client.fetchCurrentStreamLength();
        assertEquals(123, length);
    }

    @Test(timeout = 10000)
    public void testGetProperty() throws Exception {
        UUID attributeId = SegmentAttribute.RevisionStreamClientMark.getValue();
        Segment segment = new Segment("scope", "testRetry", 4);
        PravegaNodeUri endpoint = new PravegaNodeUri("localhost", 0);
        MockConnectionFactoryImpl cf = new MockConnectionFactoryImpl();
        MockController controller = new MockController(endpoint.getEndpoint(), endpoint.getPort(), cf);
        ClientConnection connection = mock(ClientConnection.class);
        cf.provideConnection(endpoint, connection);
        SegmentMetadataClientImpl client = new SegmentMetadataClientImpl(segment, controller, cf);
        client.getConnection();
        ReplyProcessor processor = cf.getProcessor(endpoint);
        Mockito.doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                processor.segmentAttribute(new WireCommands.SegmentAttribute(1, 123));
                return null;
            }
        }).when(connection).send(new WireCommands.GetSegmentAttribute(1, segment.getScopedName(), attributeId));
        long value = client.fetchProperty(SegmentAttribute.RevisionStreamClientMark);
        assertEquals(123, value);
    }

    @Test(timeout = 10000)
    public void compareAndSetAttribute() throws Exception {
        UUID attributeId = SegmentAttribute.RevisionStreamClientMark.getValue();
        Segment segment = new Segment("scope", "testRetry", 4);
        PravegaNodeUri endpoint = new PravegaNodeUri("localhost", 0);
        MockConnectionFactoryImpl cf = new MockConnectionFactoryImpl();
        MockController controller = new MockController(endpoint.getEndpoint(), endpoint.getPort(), cf);
        ClientConnection connection = mock(ClientConnection.class);
        cf.provideConnection(endpoint, connection);
        SegmentMetadataClientImpl client = new SegmentMetadataClientImpl(segment, controller, cf);
        client.getConnection();
        ReplyProcessor processor = cf.getProcessor(endpoint);
        Mockito.doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                processor.segmentAttributeUpdated(new SegmentAttributeUpdated(1, true));
                return null;
            }
        }).when(connection).send(new WireCommands.UpdateSegmentAttribute(1, segment.getScopedName(), attributeId, 1234,
                                                                         -1234));
        assertTrue(client.compareAndSetAttribute(SegmentAttribute.RevisionStreamClientMark, -1234, 1234));
    }

    @Test(timeout = 10000)
    public void testReconnects() throws Exception {
        Segment segment = new Segment("scope", "testRetry", 4);
        PravegaNodeUri endpoint = new PravegaNodeUri("localhost", 0);
        MockConnectionFactoryImpl cf = Mockito.spy(new MockConnectionFactoryImpl());
        MockController controller = new MockController(endpoint.getEndpoint(), endpoint.getPort(), cf);
        ClientConnection connection = mock(ClientConnection.class);
        cf.provideConnection(endpoint, connection);
        SegmentMetadataClientImpl client = new SegmentMetadataClientImpl(segment, controller, cf);
        client.getConnection();
        ReplyProcessor processor = cf.getProcessor(endpoint);
        WireCommands.GetStreamSegmentInfo getSegmentInfo1 = new WireCommands.GetStreamSegmentInfo(1, segment.getScopedName());
        Mockito.doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                processor.connectionDropped();
                return null;
            }
        }).when(connection).send(getSegmentInfo1);
        WireCommands.GetStreamSegmentInfo getSegmentInfo2 = new WireCommands.GetStreamSegmentInfo(2, segment.getScopedName());
        Mockito.doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                processor.streamSegmentInfo(new StreamSegmentInfo(2, segment.getScopedName(), true, false, false, 0,
                                                                  123));
                return null;
            }
        }).when(connection).send(getSegmentInfo2);
        long length = client.fetchCurrentStreamLength();
        InOrder order = Mockito.inOrder(connection, cf);
        order.verify(cf).establishConnection(endpoint, processor);
        order.verify(connection).send(getSegmentInfo1);
        order.verify(cf).establishConnection(endpoint, processor);
        order.verify(connection).send(getSegmentInfo2);
        order.verifyNoMoreInteractions();
        assertEquals(123, length);
    }
    
}
