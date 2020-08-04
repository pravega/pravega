/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.segment.impl;

import io.pravega.client.control.impl.Controller;
import io.pravega.client.netty.impl.ClientConnection;
import io.pravega.client.netty.impl.ConnectionFactory;
import io.pravega.client.netty.impl.Flow;
import io.pravega.client.security.auth.EmptyTokenProviderImpl;
import io.pravega.shared.protocol.netty.PravegaNodeUri;
import io.pravega.shared.protocol.netty.ReplyProcessor;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class SegmentInputStreamFactoryImplTest {

    @Mock
    private Controller controller;
    @Mock
    private ConnectionFactory cf;
    @Mock
    private ClientConnection connection;
    @Mock
    private ScheduledExecutorService executor;

    @Before
    public void setup() {
        when(controller.getEndpointForSegment(anyString()))
                .thenReturn(CompletableFuture.completedFuture(new PravegaNodeUri("localhost", 9090)));
        when(cf.establishConnection(any(Flow.class), any(PravegaNodeUri.class), any(ReplyProcessor.class)))
                .thenReturn(CompletableFuture.completedFuture(connection));
        when(cf.getInternalExecutor()).thenReturn(executor);
    }

    @Test
    public void createInputStreamForSegment() {
        SegmentInputStreamFactoryImpl factory = new SegmentInputStreamFactoryImpl(controller, cf);
        SegmentInputStream segmentInputStream = factory
                .createInputStreamForSegment(Segment.fromScopedName("scope/stream/0"), new EmptyTokenProviderImpl());
        assertEquals(0, segmentInputStream.getOffset());
    }

    @Test
    public void testCreateInputStreamForSegmentWithOffset() {
        SegmentInputStreamFactoryImpl factory = new SegmentInputStreamFactoryImpl(controller, cf);
        SegmentInputStream segmentInputStream = factory
                .createInputStreamForSegment(Segment
                        .fromScopedName("scope/stream/0"), new EmptyTokenProviderImpl(), 100);
        assertEquals(100, segmentInputStream.getOffset());
        assertEquals(Segment.fromScopedName("scope/stream/0"), segmentInputStream.getSegmentId());

    }
}