/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.batch.impl;

import io.pravega.client.batch.SegmentRange;
import io.pravega.client.netty.impl.ClientConnection;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.impl.StreamImpl;
import io.pravega.client.stream.mock.MockConnectionFactoryImpl;
import io.pravega.client.stream.mock.MockController;
import io.pravega.shared.protocol.netty.ConnectionFailedException;
import io.pravega.shared.protocol.netty.PravegaNodeUri;
import io.pravega.shared.protocol.netty.WireCommands.CreateSegment;
import io.pravega.shared.protocol.netty.WireCommands.GetStreamSegmentInfo;
import io.pravega.shared.protocol.netty.WireCommands.SegmentCreated;
import io.pravega.shared.protocol.netty.WireCommands.StreamSegmentInfo;
import java.util.Iterator;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class BatchClientImplTest {

    @Test(timeout = 5000)
    public void testSegmentIterator() throws ConnectionFailedException {
        MockConnectionFactoryImpl connectionFactory = new MockConnectionFactoryImpl();
        ClientConnection connection = Mockito.mock(ClientConnection.class);
        PravegaNodeUri location = new PravegaNodeUri("localhost", 0);
        Mockito.doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                CreateSegment request = (CreateSegment) invocation.getArgument(0);
                connectionFactory.getProcessor(location)
                                 .segmentCreated(new SegmentCreated(request.getRequestId(), request.getSegment()));
                return null;
            }
        }).when(connection).send(Mockito.any(CreateSegment.class));
        Mockito.doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                GetStreamSegmentInfo request = (GetStreamSegmentInfo) invocation.getArgument(0);
                connectionFactory.getProcessor(location)
                                 .streamSegmentInfo(new StreamSegmentInfo(request.getRequestId(),
                                                                          request.getSegmentName(), true, false, false,
                                                                          0, 0, 0));
                return null;
            }
        }).when(connection).send(Mockito.any(GetStreamSegmentInfo.class));
        connectionFactory.provideConnection(location, connection);
        MockController mockController = new MockController(location.getEndpoint(), location.getPort(),
                                                           connectionFactory);
        BatchClientImpl client = new BatchClientImpl(mockController, connectionFactory);
        Stream stream = new StreamImpl("scope", "stream");
        mockController.createScope("scope");
        mockController.createStream(StreamConfiguration.builder()
                                                       .scope("scope")
                                                       .streamName("stream")
                                                       .scalingPolicy(ScalingPolicy.fixed(3))
                                                       .build())
                      .join();
        Iterator<SegmentRange> segments = client.getSegments(stream, null, null).getSegmentRangeIterator();
        assertTrue(segments.hasNext());
        assertEquals(0, segments.next().asImpl().getSegment().getSegmentNumber());
        assertTrue(segments.hasNext());
        assertEquals(1, segments.next().asImpl().getSegment().getSegmentNumber());
        assertTrue(segments.hasNext());
        assertEquals(2, segments.next().asImpl().getSegment().getSegmentNumber());
        assertFalse(segments.hasNext());
    }

}
