/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.server.host.handler;

import io.netty.channel.Channel;
import io.netty.channel.ChannelConfig;
import io.netty.channel.ChannelHandlerContext;
import lombok.val;
import org.junit.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for the {@link ServerConnectionInboundHandler} class.
 */
public class ServerConnectionInboundHandlerTests {

    /**
     * Tests the {@link ServerConnectionInboundHandler#adjustOutstandingBytes} class.
     */
    @Test
    public void testAdjustOutstandingBytes() throws Exception {
        val tracker = mock(ConnectionTracker.class);
        val context = mock(ChannelHandlerContext.class);
        val channel = mock(Channel.class);
        val channelConfig = mock(ChannelConfig.class);
        val configVerifier = inOrder(channelConfig);
        when(context.channel()).thenReturn(channel);
        when(channel.config()).thenReturn(channelConfig);

        val h = new ServerConnectionInboundHandler(tracker);
        h.channelRegistered(context);

        // 1. Verify the outstanding bytes are correctly calculated.
        when(tracker.adjustOutstandingBytes(any(long.class), any(long.class))).thenReturn(true);
        h.adjustOutstandingBytes(10);
        verify(tracker).adjustOutstandingBytes(10, 10);
        configVerifier.verify(channelConfig).setAutoRead(true);

        h.adjustOutstandingBytes(5);
        verify(tracker).adjustOutstandingBytes(5, 15);
        configVerifier.verify(channelConfig).setAutoRead(true);

        h.adjustOutstandingBytes(-20); // This would make it negative; verify it will put a low-bound of 0 on it.
        verify(tracker).adjustOutstandingBytes(-20, 0);
        configVerifier.verify(channelConfig).setAutoRead(true);

        // 2. Verify the connection is paused and resumed as needed.
        when(tracker.adjustOutstandingBytes(any(long.class), any(long.class))).thenReturn(false);
        h.adjustOutstandingBytes(10);
        configVerifier.verify(channelConfig).setAutoRead(false);

        when(tracker.adjustOutstandingBytes(any(long.class), any(long.class))).thenReturn(true);
        h.adjustOutstandingBytes(10);
        configVerifier.verify(channelConfig).setAutoRead(true);
        configVerifier.verifyNoMoreInteractions();
    }
}
