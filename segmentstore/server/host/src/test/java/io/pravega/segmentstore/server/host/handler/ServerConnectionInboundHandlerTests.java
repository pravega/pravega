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

import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelConfig;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.EventLoop;
import io.pravega.common.concurrent.Futures;
import io.pravega.segmentstore.contracts.BadAttributeUpdateException;
import io.pravega.segmentstore.contracts.BadOffsetException;
import io.pravega.segmentstore.contracts.StreamSegmentStore;
import io.pravega.shared.protocol.netty.Append;
import io.pravega.shared.protocol.netty.FailingRequestProcessor;
import io.pravega.shared.protocol.netty.WireCommands;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.ThreadPooledTestSuite;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
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
public class ServerConnectionInboundHandlerTests extends ThreadPooledTestSuite {

    /**
     * When overridden in a derived class, indicates how many threads should be in the thread pool.
     * If this method returns 0 (default value), then an InlineExecutor is used; otherwise a regular ThreadPool is used.
     */
    protected int getThreadPoolSize() {
        return 3;
    }

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

    /**
     * Simulates multiple connections being set up and all sending appends (conditional or unconditional). Some may be
     * failed by the store. Verifies that {@link ConnectionTracker#getTotalOutstanding()} does not drift with time,
     * regardless of append outcome.
     */
    @Test
    public void tesOutstandingByteTracking() throws Exception {
        final int connectionCount = 5;
        final int writersCount = 5;
        final int segmentCount = 5;

        val tracker = new ConnectionTracker();
        val context = mock(ChannelHandlerContext.class);
        val channel = mock(Channel.class);
        val channelConfig = mock(ChannelConfig.class);
        when(context.channel()).thenReturn(channel);
        when(channel.config()).thenReturn(channelConfig);
        val eventLoop = mock(EventLoop.class);
        when(channel.eventLoop()).thenReturn(eventLoop);
        when(eventLoop.inEventLoop()).thenReturn(false);
        val channelFuture = mock(ChannelFuture.class);
        when(channel.writeAndFlush(any())).thenReturn(channelFuture);

        val segments = IntStream.range(0, segmentCount).mapToObj(Integer::toString).collect(Collectors.toList());
        val writers = IntStream.range(0, writersCount).mapToObj(i -> UUID.randomUUID()).collect(Collectors.toList());

        val store = mock(StreamSegmentStore.class);
        val processors = new ArrayList<AppendProcessor>();
        for (int i = 0; i < connectionCount; i++) {
            val h = new ServerConnectionInboundHandler(tracker);
            h.channelRegistered(context);
            val p = new AppendProcessor(store, h, new FailingRequestProcessor(), null);
            processors.add(p);
        }

        // Setup appends.
        for (int connectionId = 0; connectionId < processors.size(); connectionId++) {
            for (val s : segments) {
                for (val w : writers) {
                    when(store.getAttributes(s, Collections.singleton(w), true, AppendProcessor.TIMEOUT))
                            .thenReturn(CompletableFuture.completedFuture(Collections.singletonMap(w, 0L)));
                    processors.get(connectionId).setupAppend(new WireCommands.SetupAppend(0, w, s, null));
                }
            }
        }

        // Divide the segments into conditional and unconditional.
        val conditionalSegments = segments.subList(0, segments.size() / 2);
        val unconditionalSegments = segments.subList(conditionalSegments.size(), segments.size() - 1);

        // Send a few appends to each connection from each writer.
        val appendData = Unpooled.wrappedBuffer(new byte[1]);
        when(store.append(any(), any(), any(), any()))
                .thenReturn(delayedResponse(0L));
        for (val s : unconditionalSegments) {
            for (val p : processors) {
                for (val w : writers) {
                    p.append(new Append(s, w, 1, new WireCommands.Event(appendData.retain()), 0));
                }
            }
        }

        // Send a few conditional appends to each connection from each writer. Fail some along the way.
        int appendOffset = 0;
        for (val s : conditionalSegments) {
            for (val p : processors) {
                for (val w : writers) {
                    boolean fail = appendOffset % 3 == 0;
                    if (fail) {
                        when(store.append(any(), any(long.class), any(), any(), any()))
                                .thenReturn(delayedFailure(new BadOffsetException(s, appendOffset, appendOffset)));
                    } else {
                        when(store.append(any(), any(long.class), any(), any(), any()))
                                .thenReturn(delayedResponse(0L));
                    }
                    p.append(new Append(s, w, 1, new WireCommands.Event(appendData.retain()), appendOffset, 0));
                    appendOffset++;
                }
            }
        }

        // Fail (attributes) all connections.
        when(store.append(any(), any(), any(), any()))
                .thenReturn(delayedFailure(new BadAttributeUpdateException("s", null, false, "intentional")));
        for (val s : conditionalSegments) {
            for (val p : processors) {
                for (val w : writers) {
                    p.append(new Append(s, w, 1, new WireCommands.Event(appendData.retain()), 0));
                }
            }
        }

        // Verify that there is no drift in the ConnectionTracker#getTotalOutstanding value. Due to the async nature
        // of the calls, this value may not immediately be updated.
        AssertExtensions.assertEventuallyEquals(0L, tracker::getTotalOutstanding, 10000);

    }

    private <T> CompletableFuture<T> delayedResponse(T value) {
        return Futures.delayedFuture(Duration.ofMillis(1), executorService()).thenApply(v -> value);
    }

    private <V, T extends Throwable> CompletableFuture<V> delayedFailure(T ex) {
        return Futures.delayedFuture(Duration.ofMillis(1), executorService())
                .thenCompose(v -> Futures.failedFuture(ex));
    }
}
