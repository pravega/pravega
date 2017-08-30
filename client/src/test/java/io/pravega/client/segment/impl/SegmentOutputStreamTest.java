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


import com.google.common.collect.ImmutableList;
import io.netty.buffer.Unpooled;
import io.pravega.client.netty.impl.ClientConnection;
import io.pravega.client.netty.impl.ClientConnection.CompletedCallback;
import io.pravega.client.stream.impl.PendingEvent;
import io.pravega.client.stream.mock.MockConnectionFactoryImpl;
import io.pravega.client.stream.mock.MockController;
import io.pravega.common.concurrent.FutureHelpers;
import io.pravega.common.util.Retry;
import io.pravega.common.util.Retry.RetryWithBackoff;
import io.pravega.shared.protocol.netty.Append;
import io.pravega.shared.protocol.netty.ConnectionFailedException;
import io.pravega.shared.protocol.netty.PravegaNodeUri;
import io.pravega.shared.protocol.netty.WireCommands;
import io.pravega.shared.protocol.netty.WireCommands.AppendSetup;
import io.pravega.shared.protocol.netty.WireCommands.NoSuchSegment;
import io.pravega.shared.protocol.netty.WireCommands.SetupAppend;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.Async;
import io.pravega.test.common.InlineExecutor;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import lombok.Cleanup;
import org.junit.Test;
import org.mockito.InOrder;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;


public class SegmentOutputStreamTest {

    private static final String SEGMENT = "test/0";
    private static final int SERVICE_PORT = 12345;
    private static final RetryWithBackoff RETRY_SCHEDULE = Retry.withExpBackoff(1, 1, 2);
    private final Consumer<Segment> segmentSealedCallback = segment -> { };

    private static ByteBuffer getBuffer(String s) {
        return ByteBuffer.wrap(s.getBytes());
    }

    @Test(timeout = 10000)
    public void testConnectAndSend() throws SegmentSealedException, ConnectionFailedException {
        UUID cid = UUID.randomUUID();
        PravegaNodeUri uri = new PravegaNodeUri("endpoint", SERVICE_PORT);
        MockConnectionFactoryImpl cf = new MockConnectionFactoryImpl();
        @Cleanup("shutdown")
        InlineExecutor inlineExecutor = new InlineExecutor();
        cf.setExecutor(inlineExecutor);
        MockController controller = new MockController(uri.getEndpoint(), uri.getPort(), cf);
        ClientConnection connection = mock(ClientConnection.class);
        cf.provideConnection(uri, connection);
        SegmentOutputStreamImpl output = new SegmentOutputStreamImpl(SEGMENT, controller, cf, cid, segmentSealedCallback, RETRY_SCHEDULE);
        output.reconnect();
        verify(connection).send(new SetupAppend(1, cid, SEGMENT));
        cf.getProcessor(uri).appendSetup(new AppendSetup(1, SEGMENT, cid, 0));

        sendAndVerifyEvent(cid, connection, output, getBuffer("test"), 1, null);
        verifyNoMoreInteractions(connection);
    }

    @Test(timeout = 10000)
    public void testConnectAndConnectionDrop() throws Exception {
        UUID cid = UUID.randomUUID();
        PravegaNodeUri uri = new PravegaNodeUri("endpoint", SERVICE_PORT);

        MockConnectionFactoryImpl cf = new MockConnectionFactoryImpl();
        ScheduledExecutorService executor = mock(ScheduledExecutorService.class);
        implementAsDirectExecutor(executor); // Ensure task submitted to executor is run inline.
        cf.setExecutor(executor);

        MockController controller = new MockController(uri.getEndpoint(), uri.getPort(), cf);
        ClientConnection connection = mock(ClientConnection.class);
        cf.provideConnection(uri, connection);
        SegmentOutputStreamImpl output = new SegmentOutputStreamImpl(SEGMENT, controller, cf, cid, segmentSealedCallback, RETRY_SCHEDULE);
        output.reconnect();
        verify(connection).send(new SetupAppend(1, cid, SEGMENT));

        cf.getProcessor(uri).connectionDropped(); // simulate a connection dropped
        //Ensure setup Append is invoked on the executor.
        verify(connection).send(new SetupAppend(2, cid, SEGMENT));
    }

    @Test(timeout = 10000)
    public void testConnectWithMultipleFailures() throws Exception {
        UUID cid = UUID.randomUUID();
        PravegaNodeUri uri = new PravegaNodeUri("endpoint", SERVICE_PORT);

        MockConnectionFactoryImpl cf = new MockConnectionFactoryImpl();
        ScheduledExecutorService executor = mock(ScheduledExecutorService.class);
        implementAsDirectExecutor(executor); // Ensure task submitted to executor is run inline.
        cf.setExecutor(executor);

        MockController controller = new MockController(uri.getEndpoint(), uri.getPort(), cf);
        ClientConnection connection = mock(ClientConnection.class);
        cf.provideConnection(uri, connection);
        SegmentOutputStreamImpl output = new SegmentOutputStreamImpl(SEGMENT, controller, cf, cid, segmentSealedCallback, RETRY_SCHEDULE);
        output.reconnect();
        verify(connection).send(new SetupAppend(1, cid, SEGMENT));

        //simulate a processing Failure and ensure SetupAppend is executed.
        cf.getProcessor(uri).processingFailure(new IOException());
        verify(connection).send(new SetupAppend(2, cid, SEGMENT));

        cf.getProcessor(uri).connectionDropped();
        verify(connection).send(new SetupAppend(3, cid, SEGMENT));

        cf.getProcessor(uri).wrongHost(new WireCommands.WrongHost(3, SEGMENT, "newHost"));
        verify(connection).send(new SetupAppend(4, cid, SEGMENT));
    }

    protected void implementAsDirectExecutor(ScheduledExecutorService executor) {
        doAnswer(new Answer<Object>() {
            public Object answer(InvocationOnMock invocation) throws Exception {
                ((Runnable) invocation.getArguments()[0]).run();
                return null;
            }
        }).when(executor).execute(any(Runnable.class));
        doAnswer(new Answer<Object>() {
            public Object answer(InvocationOnMock invocation) throws Exception {
                ((Runnable) invocation.getArguments()[0]).run();
                return null;
            }
        }).when(executor).schedule(any(Runnable.class), Mockito.anyLong(), any(TimeUnit.class));
        doAnswer(new Answer<Object>() {
            public Object answer(InvocationOnMock invocation) throws Exception {
                ((Callable) invocation.getArguments()[0]).call();
                return null;
            }
        }).when(executor).schedule(any(Callable.class), Mockito.anyLong(), any(TimeUnit.class));
    }

    @Test(timeout = 10000)
    public void testConditionalSend() throws SegmentSealedException, ConnectionFailedException {
        UUID cid = UUID.randomUUID();
        PravegaNodeUri uri = new PravegaNodeUri("endpoint", SERVICE_PORT);
        MockConnectionFactoryImpl cf = new MockConnectionFactoryImpl();
        @Cleanup("shutdown")
        InlineExecutor inlineExecutor = new InlineExecutor();
        cf.setExecutor(inlineExecutor);
        MockController controller = new MockController(uri.getEndpoint(), uri.getPort(), cf);
        ClientConnection connection = mock(ClientConnection.class);
        cf.provideConnection(uri, connection);
        SegmentOutputStreamImpl output = new SegmentOutputStreamImpl(SEGMENT, controller, cf, cid, segmentSealedCallback, RETRY_SCHEDULE);
        output.reconnect();
        verify(connection).send(new SetupAppend(1, cid, SEGMENT));
        cf.getProcessor(uri).appendSetup(new AppendSetup(1, SEGMENT, cid, 0));

        sendAndVerifyEvent(cid, connection, output, getBuffer("test"), 1, 0L);
        verifyNoMoreInteractions(connection);
    }

    @Test(timeout = 20000)
    public void testNewEventsGoAfterInflight() throws ConnectionFailedException {
        UUID cid = UUID.randomUUID();
        PravegaNodeUri uri = new PravegaNodeUri("endpoint", SERVICE_PORT);
        MockConnectionFactoryImpl cf = new MockConnectionFactoryImpl();
        @Cleanup("shutdown")
        InlineExecutor inlineExecutor = new InlineExecutor();
        cf.setExecutor(inlineExecutor);
        MockController controller = new MockController(uri.getEndpoint(), uri.getPort(), cf);
        ClientConnection connection = mock(ClientConnection.class);
        InOrder inOrder = inOrder(connection);
        cf.provideConnection(uri, connection);
        @SuppressWarnings("resource")
        SegmentOutputStreamImpl output = new SegmentOutputStreamImpl(SEGMENT, controller, cf, cid, segmentSealedCallback, RETRY_SCHEDULE);
        
        output.reconnect();
        cf.getProcessor(uri).appendSetup(new AppendSetup(1, SEGMENT, cid, 0));
        output.write(new PendingEvent(null, getBuffer("test1"), new CompletableFuture<>()));
        output.write(new PendingEvent(null, getBuffer("test2"), new CompletableFuture<>()));
        answerSuccess(connection);
        cf.getProcessor(uri).connectionDropped();
        Async.testBlocking(() -> output.write(new PendingEvent(null, getBuffer("test3"), new CompletableFuture<>())),
                           () -> cf.getProcessor(uri).appendSetup(new AppendSetup(1, SEGMENT, cid, 0)));
        output.write(new PendingEvent(null, getBuffer("test4"), new CompletableFuture<>()));
        
        Append append1 = new Append(SEGMENT, cid, 1, Unpooled.wrappedBuffer(getBuffer("test1")), null);
        Append append2 = new Append(SEGMENT, cid, 2, Unpooled.wrappedBuffer(getBuffer("test2")), null);
        Append append3 = new Append(SEGMENT, cid, 3, Unpooled.wrappedBuffer(getBuffer("test3")), null);
        Append append4 = new Append(SEGMENT, cid, 4, Unpooled.wrappedBuffer(getBuffer("test4")), null);
        inOrder.verify(connection).send(new SetupAppend(1, cid, SEGMENT));
        inOrder.verify(connection).send(append1);
        inOrder.verify(connection).send(append2);
        inOrder.verify(connection).close();
        inOrder.verify(connection).send(new SetupAppend(2, cid, SEGMENT));
        inOrder.verify(connection).sendAsync(eq(ImmutableList.of(append1, append2)), any());
        inOrder.verify(connection).send(append3);
        inOrder.verify(connection).send(append4);
        
        verifyNoMoreInteractions(connection);
    }

    private void answerSuccess(ClientConnection connection) {
        doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                CompletedCallback callback = (CompletedCallback) invocation.getArgument(1);
                callback.complete(null);
                return null;
            }
        }).when(connection).sendAsync(Mockito.any(), Mockito.any());
    }

    private void sendAndVerifyEvent(UUID cid, ClientConnection connection, SegmentOutputStreamImpl output,
            ByteBuffer data, int num, Long expectedLength) throws SegmentSealedException, ConnectionFailedException {
        CompletableFuture<Boolean> acked = new CompletableFuture<>();
        output.write(new PendingEvent(null, data, acked, expectedLength));
        verify(connection).send(new Append(SEGMENT, cid, num, Unpooled.wrappedBuffer(data), expectedLength));
        assertEquals(false, acked.isDone());
    }

    @Test(timeout = 10000)
    public void testClose() throws ConnectionFailedException, SegmentSealedException {
        UUID cid = UUID.randomUUID();
        PravegaNodeUri uri = new PravegaNodeUri("endpoint", SERVICE_PORT);
        MockConnectionFactoryImpl cf = new MockConnectionFactoryImpl();
        @Cleanup("shutdown")
        InlineExecutor inlineExecutor = new InlineExecutor();
        cf.setExecutor(inlineExecutor);
        MockController controller = new MockController(uri.getEndpoint(), uri.getPort(), cf);
        ClientConnection connection = mock(ClientConnection.class);
        cf.provideConnection(uri, connection);

        SegmentOutputStreamImpl output = new SegmentOutputStreamImpl(SEGMENT, controller, cf, cid, segmentSealedCallback, RETRY_SCHEDULE);
        output.reconnect();
        verify(connection).send(new SetupAppend(1, cid, SEGMENT));
        cf.getProcessor(uri).appendSetup(new AppendSetup(1, SEGMENT, cid, 0));
        ByteBuffer data = getBuffer("test");

        CompletableFuture<Boolean> acked = new CompletableFuture<>();
        output.write(new PendingEvent(null, data, acked));
        verify(connection).send(new Append(SEGMENT, cid, 1, Unpooled.wrappedBuffer(data), null));
        assertEquals(false, acked.isDone());
        Async.testBlocking(() -> output.close(), () -> cf.getProcessor(uri).dataAppended(new WireCommands.DataAppended(cid, 1)));
        assertEquals(false, acked.isCompletedExceptionally());
        assertEquals(true, acked.isDone());
        verify(connection, Mockito.atMost(1)).send(new WireCommands.KeepAlive());
        verify(connection).close();
        verifyNoMoreInteractions(connection);
    }

    @Test(timeout = 10000)
    public void testFlush() throws ConnectionFailedException, SegmentSealedException {
        UUID cid = UUID.randomUUID();
        PravegaNodeUri uri = new PravegaNodeUri("endpoint", SERVICE_PORT);
        MockConnectionFactoryImpl cf = new MockConnectionFactoryImpl();
        ScheduledExecutorService executor = mock(ScheduledExecutorService.class);
        implementAsDirectExecutor(executor); // Ensure task submitted to executor is run inline.
        cf.setExecutor(executor);
        MockController controller = new MockController(uri.getEndpoint(), uri.getPort(), cf);
        ClientConnection connection = mock(ClientConnection.class);
        cf.provideConnection(uri, connection);
        InOrder order = Mockito.inOrder(connection);
        SegmentOutputStreamImpl output = new SegmentOutputStreamImpl(SEGMENT, controller, cf, cid, segmentSealedCallback, RETRY_SCHEDULE);
        output.reconnect();
        order.verify(connection).send(new SetupAppend(1, cid, SEGMENT));
        cf.getProcessor(uri).appendSetup(new AppendSetup(1, SEGMENT, cid, 0));
        ByteBuffer data = getBuffer("test");

        CompletableFuture<Boolean> acked1 = new CompletableFuture<>();
        output.write(new PendingEvent(null, data, acked1));
        order.verify(connection).send(new Append(SEGMENT, cid, 1, Unpooled.wrappedBuffer(data), null));
        assertEquals(false, acked1.isDone());
        Async.testBlocking(() -> output.flush(), () -> cf.getProcessor(uri).dataAppended(new WireCommands.DataAppended(cid, 1)));
        assertEquals(false, acked1.isCompletedExceptionally());
        assertEquals(true, acked1.isDone());
        order.verify(connection).send(new WireCommands.KeepAlive());
        
        CompletableFuture<Boolean> acked2 = new CompletableFuture<>();
        output.write(new PendingEvent(null, data, acked2));
        order.verify(connection).send(new Append(SEGMENT, cid, 2, Unpooled.wrappedBuffer(data), null));
        assertEquals(false, acked2.isDone());
        Async.testBlocking(() -> output.flush(), () -> cf.getProcessor(uri).dataAppended(new WireCommands.DataAppended(cid, 2)));
        assertEquals(false, acked2.isCompletedExceptionally());
        assertEquals(true, acked2.isDone());
        order.verify(connection).send(new WireCommands.KeepAlive());
        order.verifyNoMoreInteractions();
    }

    @Test
    public void testConnectionFailure() throws ConnectionFailedException {
        UUID cid = UUID.randomUUID();
        PravegaNodeUri uri = new PravegaNodeUri("endpoint", SERVICE_PORT);
        MockConnectionFactoryImpl cf = new MockConnectionFactoryImpl();
        @Cleanup("shutdown")
        InlineExecutor inlineExecutor = new InlineExecutor();
        cf.setExecutor(inlineExecutor);
        MockController controller = new MockController(uri.getEndpoint(), uri.getPort(), cf);
        ClientConnection connection = mock(ClientConnection.class);
        cf.provideConnection(uri, connection);
        SegmentOutputStreamImpl output = new SegmentOutputStreamImpl(SEGMENT, controller, cf, cid, segmentSealedCallback, RETRY_SCHEDULE);
        output.reconnect();
        InOrder inOrder = Mockito.inOrder(connection);
        inOrder.verify(connection).send(new SetupAppend(1, cid, SEGMENT));
        cf.getProcessor(uri).appendSetup(new AppendSetup(1, SEGMENT, cid, 0));
        ByteBuffer data = getBuffer("test");

        CompletableFuture<Boolean> acked = new CompletableFuture<>();
        Append append = new Append(SEGMENT, cid, 1, Unpooled.wrappedBuffer(data), null);
        CompletableFuture<Boolean> acked2 = new CompletableFuture<>();
        Append append2 = new Append(SEGMENT, cid, 2, Unpooled.wrappedBuffer(data), null);
        doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                cf.getProcessor(uri).connectionDropped();
                throw new ConnectionFailedException();
            }
        }).when(connection).send(append);
        doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                CompletedCallback callback = (CompletedCallback) invocation.getArgument(1);
                callback.complete(null);
                return null;
            }
        }).when(connection).sendAsync(Mockito.eq(Collections.singletonList(append)), Mockito.any());
        
        Async.testBlocking(() -> {            
            output.write(new PendingEvent(null, data, acked, null));
            output.write(new PendingEvent(null, data, acked2, null));
        }, () -> {            
            cf.getProcessor(uri).appendSetup(new AppendSetup(2, SEGMENT, cid, 0));
        });
        inOrder.verify(connection).send(append);
        inOrder.verify(connection).send(new SetupAppend(2, cid, SEGMENT));
        inOrder.verify(connection).sendAsync(Mockito.eq(Collections.singletonList(append)), Mockito.any());
        inOrder.verify(connection).send(append2);
        assertEquals(false, acked.isDone());
        assertEquals(false, acked2.isDone());
        inOrder.verifyNoMoreInteractions();
    }

    
    /**
     * Verifies that if a exception is encountered while flushing data inside of close, the
     * connection is reestablished and the data is retransmitted before close returns.
     */
    @Test
    public void testFailOnClose() throws ConnectionFailedException {
        UUID cid = UUID.randomUUID();
        PravegaNodeUri uri = new PravegaNodeUri("endpoint", SERVICE_PORT);
        MockConnectionFactoryImpl cf = new MockConnectionFactoryImpl();
        @Cleanup("shutdown")
        InlineExecutor inlineExecutor = new InlineExecutor();
        cf.setExecutor(inlineExecutor);
        MockController controller = new MockController(uri.getEndpoint(), uri.getPort(), cf);
        ClientConnection connection = mock(ClientConnection.class);
        cf.provideConnection(uri, connection);
        SegmentOutputStreamImpl output = new SegmentOutputStreamImpl(SEGMENT, controller, cf, cid, segmentSealedCallback, RETRY_SCHEDULE);
        output.reconnect();
        InOrder inOrder = Mockito.inOrder(connection);
        inOrder.verify(connection).send(new SetupAppend(1, cid, SEGMENT));
        cf.getProcessor(uri).appendSetup(new AppendSetup(1, SEGMENT, cid, 0));
        ByteBuffer data = getBuffer("test");
        
        //Prep mock: the mockito doAnswers setup below are triggered during the close inside of the testBlocking() call.
        CompletableFuture<Boolean> acked = new CompletableFuture<>();
        Append append = new Append(SEGMENT, cid, 1, Unpooled.wrappedBuffer(data), null);
        doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                cf.getProcessor(uri).connectionDropped();
                throw new ConnectionFailedException();
            }
        }).doNothing().when(connection).send(new WireCommands.KeepAlive());
        doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                CompletedCallback callback = (CompletedCallback) invocation.getArgument(1);
                callback.complete(null);
                return null;
            }
        }).when(connection).sendAsync(Mockito.eq(Collections.singletonList(append)), Mockito.any()); 
        //Queue up event.
        output.write(new PendingEvent(null, data, acked, null));
        inOrder.verify(connection).send(append);
        //Verify behavior
        Async.testBlocking(() -> {
            output.close();
        }, () -> {            
            cf.getProcessor(uri).appendSetup(new AppendSetup(2, SEGMENT, cid, 0));
            cf.getProcessor(uri).dataAppended(new WireCommands.DataAppended(cid, 1));
        });
        inOrder.verify(connection).send(new WireCommands.KeepAlive());
        inOrder.verify(connection).send(new SetupAppend(2, cid, SEGMENT));
        inOrder.verify(connection).sendAsync(Mockito.eq(Collections.singletonList(append)), Mockito.any());
        inOrder.verify(connection).close();
        assertEquals(true, acked.isDone());
        inOrder.verifyNoMoreInteractions();
    }

    @Test
    public void testOverSizedWriteFails() throws ConnectionFailedException, SegmentSealedException {
        UUID cid = UUID.randomUUID();
        PravegaNodeUri uri = new PravegaNodeUri("endpoint", SERVICE_PORT);
        MockConnectionFactoryImpl cf = new MockConnectionFactoryImpl();
        @Cleanup("shutdown")
        InlineExecutor inlineExecutor = new InlineExecutor();
        cf.setExecutor(inlineExecutor);
        MockController controller = new MockController(uri.getEndpoint(), uri.getPort(), cf);
        ClientConnection connection = mock(ClientConnection.class);
        cf.provideConnection(uri, connection);
        @Cleanup
        SegmentOutputStreamImpl output = new SegmentOutputStreamImpl(SEGMENT, controller, cf, cid, segmentSealedCallback, RETRY_SCHEDULE);
        output.reconnect();
        verify(connection).send(new SetupAppend(1, cid, SEGMENT));
        cf.getProcessor(uri).appendSetup(new AppendSetup(1, SEGMENT, cid, 0));

        ByteBuffer data = ByteBuffer.allocate(PendingEvent.MAX_WRITE_SIZE + 1);
        CompletableFuture<Boolean> acked = new CompletableFuture<>();
        try {
            output.write(new PendingEvent("routingKey", data, acked));
            fail("Did not throw");
        } catch (IllegalArgumentException e) {
            // expected
        }
        assertEquals(false, acked.isDone());
        verifyNoMoreInteractions(connection);
    }

    @Test(timeout = 10000)
    public void testSealedBeforeFlush() throws ConnectionFailedException {
        UUID cid = UUID.randomUUID();
        PravegaNodeUri uri = new PravegaNodeUri("endpoint", SERVICE_PORT);
        MockConnectionFactoryImpl cf = new MockConnectionFactoryImpl();
        @Cleanup("shutdown")
        InlineExecutor inlineExecutor = new InlineExecutor();
        cf.setExecutor(inlineExecutor);
        MockController controller = new MockController(uri.getEndpoint(), uri.getPort(), cf);
        ClientConnection connection = mock(ClientConnection.class);
        cf.provideConnection(uri, connection);
        InOrder order = Mockito.inOrder(connection);
        SegmentOutputStreamImpl output = new SegmentOutputStreamImpl(SEGMENT, controller, cf, cid, segmentSealedCallback, RETRY_SCHEDULE);
        output.reconnect();
        order.verify(connection).send(new SetupAppend(1, cid, SEGMENT));
        cf.getProcessor(uri).appendSetup(new AppendSetup(1, SEGMENT, cid, 0));
        ByteBuffer data = getBuffer("test");

        CompletableFuture<Boolean> ack = new CompletableFuture<>();
        output.write(new PendingEvent(null, data, ack));
        order.verify(connection).send(new Append(SEGMENT, cid, 1, Unpooled.wrappedBuffer(data), null));
        assertEquals(false, ack.isDone());
        cf.getProcessor(uri).segmentIsSealed(new WireCommands.SegmentIsSealed(1, SEGMENT));
        output.getUnackedEventsOnSeal(); // this is invoked by the segmentSealedCallback.
        AssertExtensions.assertThrows(SegmentSealedException.class, () -> output.flush());
    }

    @Test(timeout = 10000)
    public void testSealedAfterFlush() throws ConnectionFailedException {
        UUID cid = UUID.randomUUID();
        PravegaNodeUri uri = new PravegaNodeUri("endpoint", SERVICE_PORT);
        MockConnectionFactoryImpl cf = new MockConnectionFactoryImpl();
        @Cleanup("shutdown")
        InlineExecutor inlineExecutor = new InlineExecutor();
        cf.setExecutor(inlineExecutor);
        MockController controller = new MockController(uri.getEndpoint(), uri.getPort(), cf);
        ClientConnection connection = mock(ClientConnection.class);
        cf.provideConnection(uri, connection);
        InOrder order = Mockito.inOrder(connection);
        SegmentOutputStreamImpl output = new SegmentOutputStreamImpl(SEGMENT, controller, cf, cid, segmentSealedCallback, RETRY_SCHEDULE);
        output.reconnect();
        order.verify(connection).send(new SetupAppend(1, cid, SEGMENT));
        cf.getProcessor(uri).appendSetup(new AppendSetup(1, SEGMENT, cid, 0));
        ByteBuffer data = getBuffer("test");

        CompletableFuture<Boolean> ack = new CompletableFuture<>();
        output.write(new PendingEvent(null, data, ack));
        order.verify(connection).send(new Append(SEGMENT, cid, 1, Unpooled.wrappedBuffer(data), null));
        assertEquals(false, ack.isDone());
        Async.testBlocking(() -> {
            AssertExtensions.assertThrows(SegmentSealedException.class, () -> output.flush());
        }, () -> {
            cf.getProcessor(uri).segmentIsSealed(new WireCommands.SegmentIsSealed(1, SEGMENT));
            output.getUnackedEventsOnSeal();
        });
        AssertExtensions.assertThrows(SegmentSealedException.class, () -> output.flush());
    }
    
    @Test(timeout = 10000)
    public void testFailDurringFlush() throws ConnectionFailedException {
        UUID cid = UUID.randomUUID();
        PravegaNodeUri uri = new PravegaNodeUri("endpoint", SERVICE_PORT);
        MockConnectionFactoryImpl cf = new MockConnectionFactoryImpl();
        ScheduledExecutorService executor = mock(ScheduledExecutorService.class);
        implementAsDirectExecutor(executor); // Ensure task submitted to executor is run inline.
        cf.setExecutor(executor);
        MockController controller = new MockController(uri.getEndpoint(), uri.getPort(), cf);
        ClientConnection connection = mock(ClientConnection.class);
        cf.provideConnection(uri, connection);
        InOrder order = Mockito.inOrder(connection);
        SegmentOutputStreamImpl output = new SegmentOutputStreamImpl(SEGMENT, controller, cf, cid,
                segmentSealedCallback, RETRY_SCHEDULE);
        output.reconnect();
        order.verify(connection).send(new SetupAppend(1, cid, SEGMENT));
        cf.getProcessor(uri).appendSetup(new AppendSetup(1, SEGMENT, cid, 0));
        ByteBuffer data = getBuffer("test");

        CompletableFuture<Boolean> ack = new CompletableFuture<>();
        output.write(new PendingEvent(null, data, ack));
        order.verify(connection).send(new Append(SEGMENT, cid, 1, Unpooled.wrappedBuffer(data), null));
        assertEquals(false, ack.isDone());
        
        Mockito.doThrow(new ConnectionFailedException()).when(connection).send(new WireCommands.KeepAlive());
        Mockito.doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                cf.getProcessor(uri).appendSetup(new AppendSetup(3, SEGMENT, cid, 1));
                return null;
            }
        }).when(connection).send(new SetupAppend(3, cid, SEGMENT));
        Async.testBlocking(() -> {
            output.flush();
        }, () -> {
            cf.getProcessor(uri).connectionDropped();
        });
        order.verify(connection).send(new SetupAppend(3, cid, SEGMENT));
        assertEquals(true, ack.isDone());
    }

    @Test(timeout = 10000)
    public void testFailureWhileRetransmittingInflight() throws ConnectionFailedException {
        UUID cid = UUID.randomUUID();
        PravegaNodeUri uri = new PravegaNodeUri("endpoint", SERVICE_PORT);
        MockConnectionFactoryImpl cf = new MockConnectionFactoryImpl();
        ScheduledExecutorService executor = mock(ScheduledExecutorService.class);
        implementAsDirectExecutor(executor); // Ensure task submitted to executor is run inline.
        cf.setExecutor(executor);
        MockController controller = new MockController(uri.getEndpoint(), uri.getPort(), cf);
        ClientConnection connection = mock(ClientConnection.class);
        InOrder inOrder = inOrder(connection);
        cf.provideConnection(uri, connection);
        @SuppressWarnings("resource")
        SegmentOutputStreamImpl output = new SegmentOutputStreamImpl(SEGMENT, controller, cf, cid, segmentSealedCallback, RETRY_SCHEDULE);
        
        output.reconnect();
        cf.getProcessor(uri).appendSetup(new AppendSetup(1, SEGMENT, cid, 0));
        output.write(new PendingEvent(null, getBuffer("test1"), new CompletableFuture<>()));
        output.write(new PendingEvent(null, getBuffer("test2"), new CompletableFuture<>()));
        doAnswer(new Answer<Void>() {
            boolean failed = false;
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                CompletedCallback callback = (CompletedCallback) invocation.getArgument(1);
                if (failed) {
                    callback.complete(null);
                } else {
                    failed = true;
                    callback.complete(new ConnectionFailedException("Injected"));
                }
                return null;
            }
        }).when(connection).sendAsync(Mockito.any(), Mockito.any());
        
        doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                cf.getProcessor(uri).appendSetup(new AppendSetup(2, SEGMENT, cid, 0));
                return null;
            }
        }).when(connection).send(new SetupAppend(2, cid, SEGMENT));
        
        cf.getProcessor(uri).connectionDropped();
        Async.testBlocking(() -> output.write(new PendingEvent(null, getBuffer("test3"), new CompletableFuture<>())),
                           () -> {
                               cf.getProcessor(uri).appendSetup(new AppendSetup(1, SEGMENT, cid, 0));
                           });
        output.write(new PendingEvent(null, getBuffer("test4"), new CompletableFuture<>()));
        
        Append append1 = new Append(SEGMENT, cid, 1, Unpooled.wrappedBuffer(getBuffer("test1")), null);
        Append append2 = new Append(SEGMENT, cid, 2, Unpooled.wrappedBuffer(getBuffer("test2")), null);
        Append append3 = new Append(SEGMENT, cid, 3, Unpooled.wrappedBuffer(getBuffer("test3")), null);
        Append append4 = new Append(SEGMENT, cid, 4, Unpooled.wrappedBuffer(getBuffer("test4")), null);
        inOrder.verify(connection).send(new SetupAppend(1, cid, SEGMENT));
        inOrder.verify(connection).send(append1);
        inOrder.verify(connection).send(append2);
        inOrder.verify(connection).close();
        inOrder.verify(connection).send(new SetupAppend(2, cid, SEGMENT));
        inOrder.verify(connection).sendAsync(eq(ImmutableList.of(append1, append2)), any());
        inOrder.verify(connection).close();
        inOrder.verify(connection).send(new SetupAppend(3, cid, SEGMENT));
        inOrder.verify(connection).sendAsync(eq(ImmutableList.of(append1, append2)), any());
        inOrder.verify(connection).send(append3);
        inOrder.verify(connection).send(append4);
        
        verifyNoMoreInteractions(connection);
    }

    @Test(timeout = 10000)
    public void testExceptionSealedCallback() throws ConnectionFailedException {
        UUID cid = UUID.randomUUID();
        PravegaNodeUri uri = new PravegaNodeUri("endpoint", SERVICE_PORT);
        MockConnectionFactoryImpl cf = new MockConnectionFactoryImpl();
        ScheduledExecutorService executor = mock(ScheduledExecutorService.class);
        implementAsDirectExecutor(executor); // Ensure task submitted to executor is run inline.
        cf.setExecutor(executor);
        MockController controller = new MockController(uri.getEndpoint(), uri.getPort(), cf);
        ClientConnection connection = mock(ClientConnection.class);
        cf.provideConnection(uri, connection);
        AtomicBoolean shouldThrow = new AtomicBoolean(true);
        // call back which throws an exception.
        Consumer<Segment> exceptionCallback = s -> {
            if (shouldThrow.getAndSet(false)) {
                throw new IllegalStateException();
            }
        };
        SegmentOutputStreamImpl output = new SegmentOutputStreamImpl(SEGMENT, controller, cf, cid, exceptionCallback, RETRY_SCHEDULE);
        output.reconnect();
        verify(connection).send(new SetupAppend(1, cid, SEGMENT));
        cf.getProcessor(uri).appendSetup(new AppendSetup(1, SEGMENT, cid, 0));
        ByteBuffer data = getBuffer("test");

        CompletableFuture<Boolean> ack = new CompletableFuture<>();
        output.write(new PendingEvent(null, data, ack));
        assertEquals(false, ack.isDone());
        Mockito.doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                cf.getProcessor(uri).appendSetup(new AppendSetup(3, SEGMENT, cid, 0));
                return null;
            }
        }).when(connection).send(new SetupAppend(3, cid, SEGMENT));
        Async.testBlocking(() -> {
            AssertExtensions.assertThrows(SegmentSealedException.class, () -> output.flush());
        }, () -> {
            cf.getProcessor(uri).segmentIsSealed(new WireCommands.SegmentIsSealed(1, SEGMENT));
            output.getUnackedEventsOnSeal();
        });
        verify(connection).send(new WireCommands.KeepAlive());
        verify(connection).send(new Append(SEGMENT, cid, 1, Unpooled.wrappedBuffer(data), null));
        assertEquals(false, ack.isDone());
    }
    
    @Test(timeout = 10000)
    public void testNoSuchSegment() throws Exception {
        UUID cid = UUID.randomUUID();
        PravegaNodeUri uri = new PravegaNodeUri("endpoint", SERVICE_PORT);

        MockConnectionFactoryImpl cf = new MockConnectionFactoryImpl();
        ScheduledExecutorService executor = mock(ScheduledExecutorService.class);
        implementAsDirectExecutor(executor); // Ensure task submitted to executor is run inline.
        cf.setExecutor(executor);

        MockController controller = new MockController(uri.getEndpoint(), uri.getPort(), cf);
        ClientConnection connection = mock(ClientConnection.class);
        cf.provideConnection(uri, connection);
        SegmentOutputStreamImpl output = new SegmentOutputStreamImpl(SEGMENT, controller, cf, cid, segmentSealedCallback, RETRY_SCHEDULE);
        output.reconnect();
        verify(connection).send(new SetupAppend(1, cid, SEGMENT));

        cf.getProcessor(uri).noSuchSegment(new NoSuchSegment(1, SEGMENT)); // simulate a connection dropped
        verify(connection).close();
        
        //With an inflight event.
        connection = mock(ClientConnection.class);
        cf.provideConnection(uri, connection);
        output = new SegmentOutputStreamImpl(SEGMENT, controller, cf, cid, segmentSealedCallback, RETRY_SCHEDULE);
        output.reconnect();
        verify(connection).send(new SetupAppend(1, cid, SEGMENT));
        cf.getProcessor(uri).appendSetup(new AppendSetup(1, SEGMENT, cid, 0));
        CompletableFuture<Boolean> ack = new CompletableFuture<>();
        output.write(new PendingEvent("RoutingKey", ByteBuffer.wrap(new byte[] { 1, 2, 3 }), ack));
        assertFalse(ack.isDone());

        cf.getProcessor(uri).noSuchSegment(new NoSuchSegment(1, SEGMENT)); // simulate a connection dropped
        verify(connection).close();
        assertTrue(ack.isCompletedExceptionally());
        assertTrue(FutureHelpers.getException(ack) instanceof NoSuchSegmentException);
    }
    
}
