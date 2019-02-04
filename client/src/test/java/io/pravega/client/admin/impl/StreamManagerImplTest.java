/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.admin.impl;

import io.pravega.client.admin.StreamInfo;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.netty.impl.ClientConnection;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.impl.Controller;
import io.pravega.client.stream.impl.StreamImpl;
import io.pravega.client.stream.mock.MockConnectionFactoryImpl;
import io.pravega.client.stream.mock.MockController;
import io.pravega.shared.protocol.netty.PravegaNodeUri;
import io.pravega.shared.protocol.netty.WireCommands;
import io.pravega.test.common.AssertExtensions;
import lombok.Cleanup;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;


public class StreamManagerImplTest {

    private static final int SERVICE_PORT = 12345;
    private final String defaultScope = "foo";
    private StreamManager streamManager;
    private Controller controller = null;

    @Before
    public void setUp() {
        PravegaNodeUri uri = new PravegaNodeUri("endpoint", SERVICE_PORT);
        MockConnectionFactoryImpl cf = new MockConnectionFactoryImpl();
        this.controller = new MockController(uri.getEndpoint(), uri.getPort(), cf);
        this.streamManager = new StreamManagerImpl(controller, cf);
    }

    @Test
    public void testCreateAndDeleteScope() {
        // Create and delete immediately
        Assert.assertTrue(streamManager.createScope(defaultScope));
        Assert.assertTrue(streamManager.deleteScope(defaultScope));

        // Create twice
        Assert.assertTrue(streamManager.createScope(defaultScope));
        Assert.assertFalse(streamManager.createScope(defaultScope));
        Assert.assertTrue(streamManager.deleteScope(defaultScope));

        // Try to create invalid scope name.
        AssertExtensions.assertThrows(Exception.class, () -> streamManager.createScope("_system"));

        // This call should actually fail
        Assert.assertFalse(streamManager.deleteScope(defaultScope));
    }

    @Test(timeout = 5000)
    public void testStreamInfo() throws Exception {
        final String streamName = "stream";
        final Stream stream = new StreamImpl(defaultScope, streamName);

        // Setup Mocks
        MockConnectionFactoryImpl connectionFactory = new MockConnectionFactoryImpl();
        ClientConnection connection = mock(ClientConnection.class);
        PravegaNodeUri location = new PravegaNodeUri("localhost", 0);
        Mockito.doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                WireCommands.CreateSegment request = (WireCommands.CreateSegment) invocation.getArgument(0);
                connectionFactory.getProcessor(location)
                                 .process(new WireCommands.SegmentCreated(request.getRequestId(), request.getSegment()));
                return null;
            }
        }).when(connection).sendAsync(Mockito.any(WireCommands.CreateSegment.class),
                                      Mockito.any(ClientConnection.CompletedCallback.class));

        Mockito.doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                WireCommands.GetStreamSegmentInfo request = (WireCommands.GetStreamSegmentInfo) invocation.getArgument(0);
                connectionFactory.getProcessor(location)
                                 .process(new WireCommands.StreamSegmentInfo(request.getRequestId(), request.getSegmentName(), true,
                                                                             false, false, 0, 0, 0));
                return null;
            }
        }).when(connection).sendAsync(Mockito.any(WireCommands.GetStreamSegmentInfo.class),
                                      Mockito.any(ClientConnection.CompletedCallback.class));
        connectionFactory.provideConnection(location, connection);
        MockController mockController = new MockController(location.getEndpoint(), location.getPort(),
                                                           connectionFactory);
        @Cleanup
        final StreamManager streamManager = new StreamManagerImpl(mockController, connectionFactory);

        streamManager.createScope(defaultScope);
        streamManager.createStream(defaultScope, streamName, StreamConfiguration.builder()
                                                                                .scalingPolicy(ScalingPolicy.fixed(3))
                                                                                .build());
        // fetch StreamInfo.
        StreamInfo info = streamManager.getStreamInfo(defaultScope, streamName);

        //validate results.
        assertEquals(defaultScope, info.getScope());
        assertEquals(streamName, info.getStreamName());
        assertNotNull(info.getTailStreamCut());
        assertEquals(stream, info.getTailStreamCut().asImpl().getStream());
        assertEquals(3, info.getTailStreamCut().asImpl().getPositions().size());
        assertNotNull(info.getHeadStreamCut());
        assertEquals(stream, info.getHeadStreamCut().asImpl().getStream());
        assertEquals(3, info.getHeadStreamCut().asImpl().getPositions().size());
    }
}
