/**
 * Copyright Pravega Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.pravega.client.segment.impl;

import com.google.common.collect.ImmutableList;
import io.netty.buffer.Unpooled;
import io.pravega.client.connection.impl.ClientConnection;
import io.pravega.client.connection.impl.ClientConnection.CompletedCallback;
import io.pravega.client.connection.impl.ConnectionPool;
import io.pravega.client.security.auth.DelegationTokenProviderFactory;
import io.pravega.client.stream.impl.PendingEvent;
import io.pravega.client.stream.mock.MockConnectionFactoryImpl;
import io.pravega.client.stream.mock.MockController;
import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.ExecutorServiceHelpers;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.util.RetriesExhaustedException;
import io.pravega.common.util.Retry;
import io.pravega.common.util.Retry.RetryWithBackoff;
import io.pravega.common.util.ReusableLatch;
import io.pravega.shared.protocol.netty.Append;
import io.pravega.shared.protocol.netty.ConnectionFailedException;
import io.pravega.shared.protocol.netty.PravegaNodeUri;
import io.pravega.shared.protocol.netty.ReplyProcessor;
import io.pravega.shared.protocol.netty.WireCommands;
import io.pravega.shared.protocol.netty.WireCommands.AppendSetup;
import io.pravega.shared.protocol.netty.WireCommands.SetupAppend;
import io.pravega.shared.security.auth.AccessOperation;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.InlineExecutor;
import io.pravega.test.common.LeakDetectorTestSuite;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import lombok.Cleanup;
import lombok.val;
import org.junit.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.InOrder;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import static io.pravega.test.common.AssertExtensions.assertThrows;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

public class SegmentOutputStreamTest extends LeakDetectorTestSuite {

    private static final String SEGMENT = "test/0";
    private static final String TXN_SEGMENT = "scope/stream/0.#epoch.0#transaction.00000000000000000000000000000001";
    private static final int SERVICE_PORT = 12345;
    private static final RetryWithBackoff RETRY_SCHEDULE = Retry.withExpBackoff(1, 1, 2);
    private final Consumer<Segment> segmentSealedCallback = segment -> { };

    private static ByteBuffer getBuffer(String s) {
        return ByteBuffer.wrap(s.getBytes());
    }

    @Test(timeout = 10000)
    public void testConnectAndSend() throws ConnectionFailedException, InterruptedException {
        UUID cid = UUID.randomUUID();
        PravegaNodeUri uri = new PravegaNodeUri("endpoint", SERVICE_PORT);
        MockConnectionFactoryImpl cf = new MockConnectionFactoryImpl();
        InlineExecutor executor = new InlineExecutor();
        cf.setExecutor(executor);
        MockController controller = new MockController(uri.getEndpoint(), uri.getPort(), cf, true);
        ClientConnection connection = mock(ClientConnection.class);
        cf.provideConnection(uri, connection);
        SegmentOutputStreamImpl output = new SegmentOutputStreamImpl(SEGMENT, true, controller, cf, cid, segmentSealedCallback,
                Retry.withoutBackoff(2), DelegationTokenProviderFactory.createWithEmptyToken());
        output.reconnect();
        verify(connection).send(new SetupAppend(output.getRequestId(), cid, SEGMENT, ""));
        cf.getProcessor(uri).appendSetup(new AppendSetup(output.getRequestId(), SEGMENT, cid, 0));

        sendAndVerifyEvent(cid, connection, output, getBuffer("test"), 1);
        verifyNoMoreInteractions(connection);
    }

    @Test(timeout = 10000)
    public void testConnectAndSetupAppendTimeoutOnce() throws Exception {
        UUID cid = UUID.randomUUID();
        PravegaNodeUri uri = new PravegaNodeUri("endpoint", SERVICE_PORT);

        MockConnectionFactoryImpl cf = new MockConnectionFactoryImpl();
        @Cleanup
        InlineExecutor executor = new InlineExecutor();
        cf.setExecutor(executor);

        MockController controller = new MockController(uri.getEndpoint(), uri.getPort(), cf, true);
        ClientConnection connection = mock(ClientConnection.class);
        doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                SetupAppend request = invocation.getArgument(0, SetupAppend.class);
                WireCommands.ErrorMessage reply = Mockito.mock(WireCommands.ErrorMessage.class);
                Mockito.when(reply.getThrowableException()).thenReturn(new ServerTimeoutException("Mock timeout"));
                Mockito.when(reply.getErrorCode()).thenReturn(WireCommands.ErrorMessage.ErrorCode.UNSPECIFIED);
                Mockito.when(reply.getSegment()).thenReturn(request.getSegment());
                cf.getProcessor(uri).errorMessage(reply);
                return null;
            }
        }).doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                SetupAppend request = invocation.getArgument(0, SetupAppend.class);
                cf.getProcessor(uri)
                        .appendSetup(new AppendSetup(request.getRequestId(),
                                request.getSegment(),
                                request.getWriterId(),
                                0));
                return null;
            }
        }).when(connection).send(any(SetupAppend.class));
        cf.provideConnection(uri, connection);
        @SuppressWarnings("resource")
        SegmentOutputStreamImpl output = new SegmentOutputStreamImpl(SEGMENT,
                true,
                controller,
                cf,
                cid,
                segmentSealedCallback,
                Retry.withoutBackoff(2),
                DelegationTokenProviderFactory.createWithEmptyToken());
        output.reconnect();
        byte[] eventData = "test data".getBytes();
        CompletableFuture<Void> ack1 = new CompletableFuture<>();
        output.write(PendingEvent.withoutHeader(null, ByteBuffer.wrap(eventData), ack1));
        verify(connection, times(2)).send(new SetupAppend(output.getRequestId(), cid, SEGMENT, ""));
        verify(connection).send(any(Append.class));
    }

    @Test (timeout = 10000)
    public void testTimeoutException() throws SegmentSealedException, InterruptedException {
        int requestId = 0;
        UUID cid = UUID.randomUUID();
        PravegaNodeUri uri = new PravegaNodeUri("endpoint", SERVICE_PORT);
        MockConnectionFactoryImpl cf = new MockConnectionFactoryImpl();
        cf.setExecutor(executorService());
        MockController controller = new MockController(uri.getEndpoint(), uri.getPort(), cf, true);
        ClientConnection connection = mock(ClientConnection.class);
        cf.provideConnection(uri, connection);
        InOrder verify = inOrder(connection);
        @Cleanup
        SegmentOutputStreamImpl output = new SegmentOutputStreamImpl(SEGMENT, true, controller, cf, cid, segmentSealedCallback,
                Retry.withoutBackoff(2), DelegationTokenProviderFactory.createWithEmptyToken());
        output.reconnect();

        ReplyProcessor processor = cf.getProcessor(uri);

        processor.processingFailure(new TimeoutException());
        verify.verify(connection, atLeast(2)).close();
    }

    @Test
    public void testRecvErrorMessage() throws SegmentSealedException, InterruptedException {
        int requestId = 0;
        UUID cid = UUID.randomUUID();
        PravegaNodeUri uri = new PravegaNodeUri("endpoint", SERVICE_PORT);
        MockConnectionFactoryImpl cf = new MockConnectionFactoryImpl();
        cf.setExecutor(executorService());
        MockController controller = new MockController(uri.getEndpoint(), uri.getPort(), cf, true);
        ClientConnection connection = mock(ClientConnection.class);
        cf.provideConnection(uri, connection);
        @Cleanup
        SegmentOutputStreamImpl output = new SegmentOutputStreamImpl(SEGMENT, true, controller, cf, cid, segmentSealedCallback,
                Retry.withoutBackoff(2), DelegationTokenProviderFactory.createWithEmptyToken());
        output.reconnect();

        ReplyProcessor processor = cf.getProcessor(uri);

        WireCommands.ErrorMessage reply = new WireCommands.ErrorMessage(requestId, "segment", "error.", WireCommands.ErrorMessage.ErrorCode.ILLEGAL_ARGUMENT_EXCEPTION);
        processor.process(reply);
        verify(connection).close();
    }


    @Test(timeout = 10000)
    public void testReconnectWorksWithTokenTaskInInternalExecutor() throws SegmentSealedException {
        UUID cid = UUID.randomUUID();
        PravegaNodeUri uri = new PravegaNodeUri("endpoint", SERVICE_PORT);

        @Cleanup("shutdownNow")
        val executor = ExecutorServiceHelpers.newScheduledThreadPool(1, "test");
        @Cleanup
        MockConnectionFactoryImpl cf = new MockConnectionFactoryImpl();
        // create one thread on connection factory
        cf.setExecutor(executor);
        CompletableFuture<Void> signal = new CompletableFuture<>();
        MockControllerWithTokenTask controller = spy(new MockControllerWithTokenTask(uri.getEndpoint(), uri.getPort(), cf,
                true, signal));

        ClientConnection connection = mock(ClientConnection.class);
        cf.provideConnection(uri, connection);
        @Cleanup
        SegmentOutputStreamImpl output = new SegmentOutputStreamImpl(SEGMENT, true, controller, cf, cid, segmentSealedCallback,
                RETRY_SCHEDULE, DelegationTokenProviderFactory.create(controller, "scope", "stream", AccessOperation.ANY));
        output.reconnect();

        signal.join();
        verify(controller, times(1)).getOrRefreshDelegationTokenFor("scope", "stream",
                AccessOperation.ANY);
    }

    @Test(timeout = 10000)
    public void testConnectAndConnectionDrop() throws Exception {
        UUID cid = UUID.randomUUID();
        PravegaNodeUri uri = new PravegaNodeUri("endpoint", SERVICE_PORT);

        MockConnectionFactoryImpl cf = new MockConnectionFactoryImpl();
        InlineExecutor executor = new InlineExecutor(); // Ensure task submitted to executor is run inline.
        cf.setExecutor(executor);

        MockController controller = new MockController(uri.getEndpoint(), uri.getPort(), cf, true);
        ClientConnection connection = mock(ClientConnection.class);
        cf.provideConnection(uri, connection);
        @Cleanup
        SegmentOutputStreamImpl output = new SegmentOutputStreamImpl(SEGMENT, true, controller, cf, cid, segmentSealedCallback,
                                                                     Retry.withoutBackoff(2), DelegationTokenProviderFactory.createWithEmptyToken());
        output.reconnect();
        verify(connection).send(new SetupAppend(output.getRequestId(), cid, SEGMENT, ""));
        reset(connection);
        cf.getProcessor(uri).connectionDropped(); // simulate a connection dropped
        //Ensure setup Append is invoked on the executor.
        verify(connection).send(new SetupAppend(output.getRequestId(), cid, SEGMENT, ""));
    }

    @Test(timeout = 10000)
    public void testConnectAndFailedSetupAppend() throws Exception {
        UUID cid = UUID.randomUUID();
        PravegaNodeUri uri = new PravegaNodeUri("endpoint", SERVICE_PORT);

        MockConnectionFactoryImpl cf = new MockConnectionFactoryImpl();
        @Cleanup
        InlineExecutor executor = new InlineExecutor();
        cf.setExecutor(executor);

        MockController controller = new MockController(uri.getEndpoint(), uri.getPort(), cf, true);
        ClientConnection connection = mock(ClientConnection.class);
        doThrow(ConnectionFailedException.class).doNothing().when(connection).send(any(SetupAppend.class));
        cf.provideConnection(uri, connection);
        @Cleanup
        SegmentOutputStreamImpl output = new SegmentOutputStreamImpl(SEGMENT, true, controller, cf, cid, segmentSealedCallback,
                                                                     Retry.withoutBackoff(2), DelegationTokenProviderFactory.createWithEmptyToken());
        output.reconnect();
        verify(connection, times(2)).send(new SetupAppend(output.getRequestId(), cid, SEGMENT, ""));
    }

    @Test(timeout = 10000)
    public void testConnectAndFailedSetupAppendDueToTruncation() throws Exception {
        AtomicBoolean callbackInvoked = new AtomicBoolean();
        Consumer<Segment> resendToSuccessorsCallback = segment -> {
            callbackInvoked.set(true);
        };
        UUID cid = UUID.randomUUID();
        PravegaNodeUri uri = new PravegaNodeUri("endpoint", SERVICE_PORT);

        MockConnectionFactoryImpl cf = new MockConnectionFactoryImpl();
        @Cleanup
        InlineExecutor executor = new InlineExecutor();
        cf.setExecutor(executor);

        MockController controller = new MockController(uri.getEndpoint(), uri.getPort(), cf, true);
        ClientConnection connection = mock(ClientConnection.class);
        cf.provideConnection(uri, connection);
        @Cleanup
        SegmentOutputStreamImpl output = new SegmentOutputStreamImpl(SEGMENT, true, controller, cf, cid, resendToSuccessorsCallback,
                                                                     Retry.withoutBackoff(2), DelegationTokenProviderFactory.createWithEmptyToken());
        output.reconnect();
        verify(connection).send(new SetupAppend(output.getRequestId(), cid, SEGMENT, ""));
        cf.getProcessor(uri).noSuchSegment(new WireCommands.NoSuchSegment(output.getRequestId(), SEGMENT, "SomeException", -1L));
        assertThrows(SegmentSealedException.class, () -> Futures.getThrowingException(output.getConnection()));
        assertTrue(callbackInvoked.get());
    }

    @Test(timeout = 10000)
    public void testConnectWithMultipleFailures() throws Exception {
        UUID cid = UUID.randomUUID();
        PravegaNodeUri uri = new PravegaNodeUri("endpoint", SERVICE_PORT);
        RetryWithBackoff retryConfig = Retry.withoutBackoff( 4);
        MockConnectionFactoryImpl cf = new MockConnectionFactoryImpl();
        @Cleanup
        InlineExecutor executor = new InlineExecutor();
        cf.setExecutor(executor);

        MockController controller = new MockController(uri.getEndpoint(), uri.getPort(), cf, true);
        ClientConnection connection = mock(ClientConnection.class);
        InOrder verify = inOrder(connection);
        cf.provideConnection(uri, connection);
        SegmentOutputStreamImpl output = new SegmentOutputStreamImpl(SEGMENT, true, controller, cf, cid, segmentSealedCallback,
                                                                     retryConfig, DelegationTokenProviderFactory.createWithEmptyToken());
            output.reconnect();
            verify.verify(connection).send(new SetupAppend(output.getRequestId(), cid, SEGMENT, ""));
            //simulate a processing Failure and ensure SetupAppend is executed.
            ReplyProcessor processor = cf.getProcessor(uri);
            processor.processingFailure(new IOException());
            verify.verify(connection).close();
            verify.verify(connection).send(new SetupAppend(output.getRequestId(), cid, SEGMENT, ""));
            verifyNoMoreInteractions(connection);
            processor.connectionDropped();
            verify.verify(connection).close();
            verify.verify(connection).send(new SetupAppend(output.getRequestId(), cid, SEGMENT, ""));
            processor.wrongHost(new WireCommands.WrongHost(output.getRequestId(), SEGMENT, "newHost", "SomeException"));
            verify.verify(connection).getLocation();
            verify.verify(connection).close();
            verify.verify(connection).send(new SetupAppend(output.getRequestId(), cid, SEGMENT, ""));
            verifyNoMoreInteractions(connection);
            processor.processingFailure(new IOException());
            assertTrue("Connection is  exceptionally closed with RetriesExhaustedException", output.getConnection().isCompletedExceptionally());
            AssertExtensions.assertThrows(RetriesExhaustedException.class, () -> Futures.getThrowingException(output.getConnection()));
            verify.verify(connection).close();
            verifyNoMoreInteractions(connection);
            // Verify that a close on the SegmentOutputStream does throw a RetriesExhaustedException.
            AssertExtensions.assertThrows(RetriesExhaustedException.class, output::close);
    }

    @Test(timeout = 10000)
    public void testInflightWithMultipleConnectFailures() throws Exception {
        UUID cid = UUID.randomUUID();
        PravegaNodeUri uri = new PravegaNodeUri("endpoint", SERVICE_PORT);
        RetryWithBackoff retryConfig = Retry.withoutBackoff(1);
        MockConnectionFactoryImpl cf = new MockConnectionFactoryImpl();
        InlineExecutor executor = new InlineExecutor(); // Ensure task submitted to executor is run inline.
        cf.setExecutor(executor);

        MockController controller = new MockController(uri.getEndpoint(), uri.getPort(), cf, true);
        ClientConnection connection = mock(ClientConnection.class);
        cf.provideConnection(uri, connection);
        SegmentOutputStreamImpl output = new SegmentOutputStreamImpl(SEGMENT, true, controller, cf, cid, segmentSealedCallback,
                retryConfig, DelegationTokenProviderFactory.createWithEmptyToken());
        try {
            output.reconnect();
            verify(connection).send(new SetupAppend(output.getRequestId(), cid, SEGMENT, ""));
            //Simulate a successful connection setup.
            cf.getProcessor(uri).appendSetup(new AppendSetup(output.getRequestId(), SEGMENT, cid, 0));

            // try sending an event.
            byte[] eventData = "test data".getBytes();
            CompletableFuture<Void> ack1 = new CompletableFuture<>();
            output.write(PendingEvent.withoutHeader(null, ByteBuffer.wrap(eventData), ack1));
            verify(connection).send(new Append(SEGMENT, cid, 1, 1, Unpooled.wrappedBuffer(eventData), null, output.getRequestId()));
            reset(connection);
            //simulate a connection drop and verify if the writer tries to establish a new connection.
            cf.getProcessor(uri).connectionDropped();
            verify(connection).send(new SetupAppend(output.getRequestId(), cid, SEGMENT, ""));
            reset(connection);
            // Simulate a connection drop again.
            cf.getProcessor(uri).connectionDropped();
            // Since we have exceeded the retry attempts verify we do not try to establish a connection.
            verify(connection, never()).send(new SetupAppend(output.getRequestId(), cid, SEGMENT, ""));
            assertTrue("Connection is  exceptionally closed with RetriesExhaustedException", output.getConnection().isCompletedExceptionally());
            AssertExtensions.assertThrows(RetriesExhaustedException.class, () -> Futures.getThrowingException(output.getConnection()));
            // Verify that the inflight event future is completed exceptionally.
            AssertExtensions.assertThrows(RetriesExhaustedException.class, () -> Futures.getThrowingException(ack1));

            //Write an additional event to a writer that has failed with RetriesExhaustedException.
            CompletableFuture<Void> ack2 = new CompletableFuture<>();
            output.write(PendingEvent.withoutHeader(null, ByteBuffer.wrap(eventData), ack2));
            verify(connection, never()).send(new SetupAppend(output.getRequestId(), cid, SEGMENT, ""));
            AssertExtensions.assertThrows(RetriesExhaustedException.class, () -> Futures.getThrowingException(ack2));
            // Verify that a flush on the SegmentOutputStream does throw a RetriesExhaustedException.
            AssertExtensions.assertThrows(RetriesExhaustedException.class, output::flush);
        } finally {
            // Verify that a close on the SegmentOutputStream does throw a RetriesExhaustedException.
            AssertExtensions.assertThrows(RetriesExhaustedException.class, output::close);
        }
    }

    @Test(timeout = 10000)
    public void testFlushWithMultipleConnectFailures() throws Exception {
        UUID cid = UUID.randomUUID();
        PravegaNodeUri uri = new PravegaNodeUri("endpoint", SERVICE_PORT);
        RetryWithBackoff retryConfig = Retry.withoutBackoff(1);
        MockConnectionFactoryImpl cf = new MockConnectionFactoryImpl();
        InlineExecutor executor = new InlineExecutor(); // Ensure task submitted to executor is run inline.
        cf.setExecutor(executor);

        MockController controller = new MockController(uri.getEndpoint(), uri.getPort(), cf, true);
        ClientConnection connection = mock(ClientConnection.class);
        cf.provideConnection(uri, connection);
        SegmentOutputStreamImpl output = new SegmentOutputStreamImpl(SEGMENT, true, controller, cf, cid, segmentSealedCallback,
                retryConfig, DelegationTokenProviderFactory.createWithEmptyToken());
        try {
            output.reconnect();
            verify(connection).send(new SetupAppend(output.getRequestId(), cid, SEGMENT, ""));
            //Simulate a successful connection setup.
            cf.getProcessor(uri).appendSetup(new AppendSetup(output.getRequestId(), SEGMENT, cid, 0));

            // try sending an event.
            byte[] eventData = "test data".getBytes();
            CompletableFuture<Void> acked = new CompletableFuture<>();
            // this is an inflight event and the client will track it until there is a response from SSS.
            output.write(PendingEvent.withoutHeader(null, ByteBuffer.wrap(eventData), acked));
            verify(connection).send(new Append(SEGMENT, cid, 1, 1, Unpooled.wrappedBuffer(eventData), null, output.getRequestId()));
            reset(connection);
            //simulate a connection drop and verify if the writer tries to establish a new connection.
            cf.getProcessor(uri).connectionDropped();
            verify(connection).send(new SetupAppend(output.getRequestId(), cid, SEGMENT, ""));
            reset(connection);

            // Verify flush blocks until there is a response from SSS. Incase of connection error the client retries. If the
            // retry count more than the configuration ensure flush returns exceptionally.
            AssertExtensions.assertBlocks(() -> AssertExtensions.assertThrows(RetriesExhaustedException.class, output::flush),
                                          () -> cf.getProcessor(uri).connectionDropped());
            assertTrue("Connection is  exceptionally closed with RetriesExhaustedException", output.getConnection().isCompletedExceptionally());
            AssertExtensions.assertThrows(RetriesExhaustedException.class, () -> Futures.getThrowingException(output.getConnection()));
            // Verify that the inflight event future is completed exceptionally.
            AssertExtensions.assertThrows(RetriesExhaustedException.class, () -> Futures.getThrowingException(acked));
        } finally {
            // Verify that a close on the SegmentOutputStream does throw a RetriesExhaustedException.
            AssertExtensions.assertThrows(RetriesExhaustedException.class, output::close);
        }
    }

    @SuppressWarnings("unchecked")
    protected void implementAsDirectExecutor(ScheduledExecutorService executor) {
        doAnswer(new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Exception {
                ((Runnable) invocation.getArguments()[0]).run();
                return null;
            }
        }).when(executor).execute(any(Runnable.class));
        doAnswer(new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Exception {
                ((Runnable) invocation.getArguments()[0]).run();
                return new InlineExecutor.NonScheduledFuture<>(CompletableFuture.completedFuture(null));
            }
        }).when(executor).schedule(any(Runnable.class), Mockito.anyLong(), any(TimeUnit.class));
        doAnswer(new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Exception {
                Object result = ((Callable<?>) invocation.getArguments()[0]).call();
                return new InlineExecutor.NonScheduledFuture<>(CompletableFuture.completedFuture(result));
            }
        }).when(executor).schedule(any(Callable.class), Mockito.anyLong(), any(TimeUnit.class));
    }

    @Test(timeout = 10000)
    public void testConditionalSend() throws ConnectionFailedException, InterruptedException {
        UUID cid = UUID.randomUUID();
        PravegaNodeUri uri = new PravegaNodeUri("endpoint", SERVICE_PORT);
        MockConnectionFactoryImpl cf = new MockConnectionFactoryImpl();
        cf.setExecutor(executorService());
        MockController controller = new MockController(uri.getEndpoint(), uri.getPort(), cf, true);
        ClientConnection connection = mock(ClientConnection.class);
        cf.provideConnection(uri, connection);
        SegmentOutputStreamImpl output = new SegmentOutputStreamImpl(SEGMENT, true, controller, cf, cid, segmentSealedCallback,
                                                                     Retry.withoutBackoff(2), DelegationTokenProviderFactory.createWithEmptyToken());
        output.reconnect();
        verify(connection).send(new SetupAppend(output.getRequestId(), cid, SEGMENT, ""));
        cf.getProcessor(uri).appendSetup(new AppendSetup(output.getRequestId(), SEGMENT, cid, 0));

        sendAndVerifyEvent(cid, connection, output, getBuffer("test"), 1);
        verifyNoMoreInteractions(connection);
    }

    @Test(timeout = 20000)
    public void testNewEventsGoAfterInflight() throws ConnectionFailedException, InterruptedException {
        UUID cid = UUID.randomUUID();
        PravegaNodeUri uri = new PravegaNodeUri("endpoint", SERVICE_PORT);
        MockConnectionFactoryImpl cf = new MockConnectionFactoryImpl();
        cf.setExecutor(executorService());
        MockController controller = new MockController(uri.getEndpoint(), uri.getPort(), cf, true);
        ClientConnection connection = mock(ClientConnection.class);
        InOrder inOrder = inOrder(connection);
        cf.provideConnection(uri, connection);
        @SuppressWarnings("resource")
        SegmentOutputStreamImpl output = new SegmentOutputStreamImpl(SEGMENT, true, controller, cf, cid, segmentSealedCallback,
                                                                     Retry.withoutBackoff(2), DelegationTokenProviderFactory.createWithEmptyToken());

        output.reconnect();
        cf.getProcessor(uri).appendSetup(new AppendSetup(1, SEGMENT, cid, 0));
        output.write(PendingEvent.withoutHeader(null, getBuffer("test1"), new CompletableFuture<>()));
        output.write(PendingEvent.withoutHeader(null, getBuffer("test2"), new CompletableFuture<>()));
        answerSuccess(connection);
        cf.getProcessor(uri).connectionDropped();
        AssertExtensions.assertBlocks(() -> output.write(PendingEvent.withoutHeader(null, getBuffer("test3"), new CompletableFuture<>())),
                                      () -> cf.getProcessor(uri).appendSetup(new AppendSetup(1, SEGMENT, cid, 0)));
        output.write(PendingEvent.withoutHeader(null, getBuffer("test4"), new CompletableFuture<>()));
        
        Append append1 = new Append(SEGMENT, cid, 1, 1, Unpooled.wrappedBuffer(getBuffer("test1")), null, output.getRequestId());
        Append append2 = new Append(SEGMENT, cid, 2, 1, Unpooled.wrappedBuffer(getBuffer("test2")), null, output.getRequestId());
        Append append3 = new Append(SEGMENT, cid, 3, 1, Unpooled.wrappedBuffer(getBuffer("test3")), null, output.getRequestId());
        Append append4 = new Append(SEGMENT, cid, 4, 1, Unpooled.wrappedBuffer(getBuffer("test4")), null, output.getRequestId());
        inOrder.verify(connection).send(new SetupAppend(output.getRequestId(), cid, SEGMENT, ""));
        inOrder.verify(connection).send(append1);
        inOrder.verify(connection).send(append2);
        inOrder.verify(connection).close();
        inOrder.verify(connection).send(new SetupAppend(output.getRequestId(), cid, SEGMENT, ""));
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
        }).when(connection).sendAsync(ArgumentMatchers.<List<Append>>any(), Mockito.any(CompletedCallback.class));
    }

    private void sendAndVerifyEvent(UUID cid, ClientConnection connection, SegmentOutputStreamImpl output,
            ByteBuffer data, int num) throws ConnectionFailedException {
        CompletableFuture<Void> acked = new CompletableFuture<>();
        output.write(PendingEvent.withoutHeader(null, data, acked));
        verify(connection).send(new Append(SEGMENT, cid, num, 1, Unpooled.wrappedBuffer(data), null, output.getRequestId()));
        assertEquals(false, acked.isDone());
    }

    @Test(timeout = 10000)
    public void testClose() throws ConnectionFailedException, InterruptedException {
        UUID cid = UUID.randomUUID();
        PravegaNodeUri uri = new PravegaNodeUri("endpoint", SERVICE_PORT);
        MockConnectionFactoryImpl cf = new MockConnectionFactoryImpl();
        cf.setExecutor(executorService());
        MockController controller = new MockController(uri.getEndpoint(), uri.getPort(), cf, true);
        ClientConnection connection = mock(ClientConnection.class);
        cf.provideConnection(uri, connection);

        SegmentOutputStreamImpl output = new SegmentOutputStreamImpl(SEGMENT, true, controller, cf, cid, segmentSealedCallback,
                                                                     Retry.withoutBackoff(2), DelegationTokenProviderFactory.createWithEmptyToken());
        output.reconnect();
        verify(connection).send(new SetupAppend(output.getRequestId(), cid, SEGMENT, ""));
        cf.getProcessor(uri).appendSetup(new AppendSetup(output.getRequestId(), SEGMENT, cid, 0));
        ByteBuffer data = getBuffer("test");

        CompletableFuture<Void> acked = new CompletableFuture<>();
        output.write(PendingEvent.withoutHeader(null, data, acked));
        verify(connection).send(new Append(SEGMENT, cid, 1, 1, Unpooled.wrappedBuffer(data), null, output.getRequestId()));
        assertEquals(false, acked.isDone());
        AssertExtensions.assertBlocks(() -> output.close(),
                                      () -> cf.getProcessor(uri).dataAppended(new WireCommands.DataAppended(output.getRequestId(), cid, 1, 0, -1)));
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
        InlineExecutor executor = new InlineExecutor(); // Ensure task submitted to executor is run inline.
        cf.setExecutor(executor);
        MockController controller = new MockController(uri.getEndpoint(), uri.getPort(), cf, true);
        ClientConnection connection = mock(ClientConnection.class);
        cf.provideConnection(uri, connection);
        InOrder order = Mockito.inOrder(connection);
        @Cleanup
        SegmentOutputStreamImpl output = new SegmentOutputStreamImpl(SEGMENT, true, controller, cf, cid, segmentSealedCallback,
                                                                     Retry.withoutBackoff(2), DelegationTokenProviderFactory.createWithEmptyToken());
        output.reconnect();
        order.verify(connection).send(new SetupAppend(output.getRequestId(), cid, SEGMENT, ""));
        cf.getProcessor(uri).appendSetup(new AppendSetup(1, SEGMENT, cid, 0));
        ByteBuffer data = getBuffer("test");

        CompletableFuture<Void> acked1 = new CompletableFuture<>();
        output.write(PendingEvent.withoutHeader(null, data, acked1));
        order.verify(connection).send(new Append(SEGMENT, cid, 1, 1, Unpooled.wrappedBuffer(data), null, output.getRequestId()));
        assertEquals(false, acked1.isDone());
        AssertExtensions.assertBlocks(() -> output.flush(),
                                      () -> cf.getProcessor(uri).dataAppended(new WireCommands.DataAppended(output.getRequestId(), cid, 1, 0, -1)));
        assertEquals(false, acked1.isCompletedExceptionally());
        assertEquals(true, acked1.isDone());
        order.verify(connection).send(new WireCommands.KeepAlive());
        
        CompletableFuture<Void> acked2 = new CompletableFuture<>();
        output.write(PendingEvent.withoutHeader(null, data, acked2));
        order.verify(connection).send(new Append(SEGMENT, cid, 2, 1, Unpooled.wrappedBuffer(data), null, output.getRequestId()));
        assertEquals(false, acked2.isDone());
        AssertExtensions.assertBlocks(() -> output.flush(),
                                      () -> cf.getProcessor(uri).dataAppended(new WireCommands.DataAppended(output.getRequestId(), cid, 2, 1, -1)));
        assertEquals(false, acked2.isCompletedExceptionally());
        assertEquals(true, acked2.isDone());
        order.verify(connection).send(new WireCommands.KeepAlive());
        order.verifyNoMoreInteractions();
    }

    @Test(timeout = 10000)
    public void testReconnectOnMissedAcks() throws ConnectionFailedException {
        UUID cid = UUID.randomUUID();
        PravegaNodeUri uri = new PravegaNodeUri("endpoint", SERVICE_PORT);
        MockConnectionFactoryImpl cf = spy(new MockConnectionFactoryImpl());
        InlineExecutor executor = new InlineExecutor(); // Ensure task submitted to executor is run inline.
        cf.setExecutor(executor);
        MockController controller = new MockController(uri.getEndpoint(), uri.getPort(), cf, true);
        ClientConnection connection = mock(ClientConnection.class);
        cf.provideConnection(uri, connection);
        InOrder order = Mockito.inOrder(connection);
        @SuppressWarnings("resource")
        SegmentOutputStreamImpl output = new SegmentOutputStreamImpl(SEGMENT, true, controller, cf, cid, segmentSealedCallback,
                Retry.withoutBackoff(2), DelegationTokenProviderFactory.createWithEmptyToken());
        output.reconnect();
        order.verify(connection).send(new SetupAppend(output.getRequestId(), cid, SEGMENT, ""));
        cf.getProcessor(uri).appendSetup(new AppendSetup(1, SEGMENT, cid, 0));
        ByteBuffer data = getBuffer("test");

        CompletableFuture<Void> acked1 = new CompletableFuture<>();
        output.write(PendingEvent.withoutHeader(null, data, acked1));
        order.verify(connection).send(new Append(SEGMENT, cid, 1, 1, Unpooled.wrappedBuffer(data), null, output.getRequestId()));
        assertEquals(false, acked1.isDone());
        AssertExtensions.assertBlocks(() -> output.flush(),
                                      () -> cf.getProcessor(uri).dataAppended(new WireCommands.DataAppended(output.getRequestId(), cid, 1, 0, -1)));
        assertEquals(false, acked1.isCompletedExceptionally());
        assertEquals(true, acked1.isDone());
        order.verify(connection).send(new WireCommands.KeepAlive());

        //simulate missed ack

        CompletableFuture<Void> acked2 = new CompletableFuture<>();
        output.write(PendingEvent.withoutHeader(null, data, acked2));
        order.verify(connection).send(new Append(SEGMENT, cid, 2, 1, Unpooled.wrappedBuffer(data), null, output.getRequestId()));
        assertEquals(false, acked2.isDone());
        cf.getProcessor(uri).dataAppended(new WireCommands.DataAppended(output.getRequestId(), cid, 3, 2L, -1));

        // check that client reconnected
        verify(cf, times(2)).establishConnection(any(), any());

    }

    @Test(timeout = 10000)
    public void testReconnectSendsSetupAppendOnTokenExpiration() throws ConnectionFailedException, SegmentSealedException {
        UUID writerId = UUID.randomUUID();
        PravegaNodeUri segmentStoreUri = new PravegaNodeUri("endpoint", SERVICE_PORT);

        MockConnectionFactoryImpl mockConnectionFactory = spy(new MockConnectionFactoryImpl());

       InlineExecutor executor = new InlineExecutor(); // Ensure task submitted to executor is run inline.
        mockConnectionFactory.setExecutor(executor);

        MockController controller = new MockController(segmentStoreUri.getEndpoint(),
                segmentStoreUri.getPort(), mockConnectionFactory, true);
        ClientConnection mockConnection = mock(ClientConnection.class);

        mockConnectionFactory.provideConnection(segmentStoreUri, mockConnection);
        @Cleanup
        SegmentOutputStreamImpl output = new SegmentOutputStreamImpl(SEGMENT, true, controller,
                mockConnectionFactory, writerId, segmentSealedCallback,  Retry.withoutBackoff(2),
                DelegationTokenProviderFactory.createWithEmptyToken());

        output.reconnect();
        verify(mockConnection).send(new SetupAppend(output.getRequestId(), writerId, SEGMENT, ""));

        reset(mockConnection);

        // Simulate token expiry
        WireCommands.AuthTokenCheckFailed authTokenCheckFailed = new WireCommands.AuthTokenCheckFailed(
                1, "server-stacktrace", WireCommands.AuthTokenCheckFailed.ErrorCode.TOKEN_EXPIRED);
        mockConnectionFactory.getProcessor(segmentStoreUri).authTokenCheckFailed(authTokenCheckFailed);

        // Upon token expiry we expect that the SetupAppend will occur again.
        verify(mockConnection).send(new SetupAppend(output.getRequestId(), writerId, SEGMENT, ""));
    }

    @Test(timeout = 10000)
    public void testReconnectDoesNotSetupAppendOnTokenCheckFailure() throws ConnectionFailedException, SegmentSealedException {
        UUID writerId = UUID.randomUUID();
        PravegaNodeUri segmentStoreUri = new PravegaNodeUri("endpoint", SERVICE_PORT);

        MockConnectionFactoryImpl mockConnectionFactory = spy(new MockConnectionFactoryImpl());

       InlineExecutor executor = new InlineExecutor();
        mockConnectionFactory.setExecutor(executor);

        MockController controller = new MockController(segmentStoreUri.getEndpoint(),
                segmentStoreUri.getPort(), mockConnectionFactory, true);
        ClientConnection mockConnection = mock(ClientConnection.class);

        mockConnectionFactory.provideConnection(segmentStoreUri, mockConnection);
        @Cleanup
        SegmentOutputStreamImpl output = new SegmentOutputStreamImpl(SEGMENT, true, controller,
                mockConnectionFactory, writerId, segmentSealedCallback,  Retry.withoutBackoff(2),
                DelegationTokenProviderFactory.createWithEmptyToken());

        output.reconnect();
        verify(mockConnection).send(new SetupAppend(output.getRequestId(), writerId, SEGMENT, ""));

        reset(mockConnection);

        // Simulate token check failure
        WireCommands.AuthTokenCheckFailed authTokenCheckFailed = new WireCommands.AuthTokenCheckFailed(
                1, "server-stacktrace", WireCommands.AuthTokenCheckFailed.ErrorCode.TOKEN_CHECK_FAILED);
        mockConnectionFactory.getProcessor(segmentStoreUri).authTokenCheckFailed(authTokenCheckFailed);

        // Verify that SetupAppend is NOT sent. We expect the client to get an exception.
        verify(mockConnection, times(0)).send(
                new SetupAppend(output.getRequestId(), writerId, SEGMENT, ""));
    }

    @Test(timeout = 10000)
    public void testReconnectOnBadAcks() throws ConnectionFailedException {
        UUID cid = UUID.randomUUID();
        PravegaNodeUri uri = new PravegaNodeUri("endpoint", SERVICE_PORT);
        MockConnectionFactoryImpl cf = spy(new MockConnectionFactoryImpl());
        InlineExecutor executor = new InlineExecutor(); // Ensure task submitted to executor is run inline.
        cf.setExecutor(executor);
        MockController controller = new MockController(uri.getEndpoint(), uri.getPort(), cf, true);
        ClientConnection connection = mock(ClientConnection.class);
        cf.provideConnection(uri, connection);
        InOrder order = Mockito.inOrder(connection);
        @SuppressWarnings("resource")
        SegmentOutputStreamImpl output = new SegmentOutputStreamImpl(SEGMENT, true, controller, cf, cid, segmentSealedCallback,
                                                                     Retry.withoutBackoff(2), DelegationTokenProviderFactory.createWithEmptyToken());
        output.reconnect();
        order.verify(connection).send(new SetupAppend(output.getRequestId(), cid, SEGMENT, ""));
        cf.getProcessor(uri).appendSetup(new AppendSetup(1, SEGMENT, cid, 0));
        ByteBuffer data = getBuffer("test");

        CompletableFuture<Void> acked1 = new CompletableFuture<>();
        output.write(PendingEvent.withoutHeader(null, data, acked1));
        order.verify(connection).send(new Append(SEGMENT, cid, 1, 1, Unpooled.wrappedBuffer(data), null, output.getRequestId()));
        assertEquals(false, acked1.isDone());
        AssertExtensions.assertBlocks(() -> output.flush(),
                                      () -> cf.getProcessor(uri).dataAppended(new WireCommands.DataAppended(output.getRequestId(), cid, 1, 0, -1)));
        assertEquals(false, acked1.isCompletedExceptionally());
        assertEquals(true, acked1.isDone());
        order.verify(connection).send(new WireCommands.KeepAlive());

        //simulate bad ack

        CompletableFuture<Void> acked2 = new CompletableFuture<>();
        output.write(PendingEvent.withoutHeader(null, data, acked2));
        order.verify(connection).send(new Append(SEGMENT, cid, 2, 1, Unpooled.wrappedBuffer(data), null, output.getRequestId()));
        assertEquals(false, acked2.isDone());
        cf.getProcessor(uri).dataAppended(new WireCommands.DataAppended(output.getRequestId(), cid, 2, 3, -1));

        // check that client reconnected
        verify(cf, times(2)).establishConnection(any(), any());

    }

    @Test(timeout = 10000)
    public void testConnectionFailure() throws Exception {
        UUID cid = UUID.randomUUID();
        PravegaNodeUri uri = new PravegaNodeUri("endpoint", SERVICE_PORT);
        MockConnectionFactoryImpl cf = new MockConnectionFactoryImpl();
        cf.setExecutor(executorService());
        MockController controller = new MockController(uri.getEndpoint(), uri.getPort(), cf, true);
        ClientConnection connection = mock(ClientConnection.class);
        cf.provideConnection(uri, connection);
        @SuppressWarnings("resource")
        SegmentOutputStreamImpl output = new SegmentOutputStreamImpl(SEGMENT, true, controller, cf, cid, segmentSealedCallback,
                Retry.withoutBackoff(2), DelegationTokenProviderFactory.createWithEmptyToken());
        output.reconnect();
        InOrder inOrder = Mockito.inOrder(connection);
        inOrder.verify(connection).send(new SetupAppend(output.getRequestId(), cid, SEGMENT, ""));
        cf.getProcessor(uri).appendSetup(new AppendSetup(output.getRequestId(), SEGMENT, cid, 0));
        ByteBuffer data = getBuffer("test");

        CompletableFuture<Void> acked = new CompletableFuture<>();
        Append append = new Append(SEGMENT, cid, 1, 1, Unpooled.wrappedBuffer(data), null, output.getRequestId());
        CompletableFuture<Void> acked2 = new CompletableFuture<>();
        Append append2 = new Append(SEGMENT, cid, 2, 1, Unpooled.wrappedBuffer(data), null, output.getRequestId());
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
        
        AssertExtensions.assertBlocks(() -> {            
            output.write(PendingEvent.withoutHeader(null, data, acked));
            output.write(PendingEvent.withoutHeader(null, data, acked2));
        }, () -> {            
            cf.getProcessor(uri).appendSetup(new AppendSetup(output.getRequestId(), SEGMENT, cid, 0));
        });
        inOrder.verify(connection).send(append);
        inOrder.verify(connection).send(new SetupAppend(output.getRequestId(), cid, SEGMENT, ""));
        inOrder.verify(connection).sendAsync(Mockito.eq(Collections.singletonList(append)), Mockito.any());
        inOrder.verify(connection).send(append2);
        assertEquals(false, acked.isDone());
        assertEquals(false, acked2.isDone());
        inOrder.verifyNoMoreInteractions();
    }


    @Test(timeout = 10000)
    public void testConnectionFailureWithSegmentSealed() throws Exception {
        UUID cid = UUID.randomUUID();
        PravegaNodeUri uri = new PravegaNodeUri("endpoint", SERVICE_PORT);
        MockConnectionFactoryImpl cf = new MockConnectionFactoryImpl();
        cf.setExecutor(executorService());
        MockController controller = new MockController(uri.getEndpoint(), uri.getPort(), cf, true);
        ClientConnection connection = mock(ClientConnection.class);
        cf.provideConnection(uri, connection);
        @SuppressWarnings("resource")
        SegmentOutputStreamImpl output = new SegmentOutputStreamImpl(SEGMENT, true, controller, cf, cid, segmentSealedCallback,
                Retry.withoutBackoff(2), DelegationTokenProviderFactory.createWithEmptyToken());

        output.reconnect();
        InOrder inOrder = Mockito.inOrder(connection);
        inOrder.verify(connection).send(new SetupAppend(output.getRequestId(), cid, SEGMENT, ""));
        cf.getProcessor(uri).appendSetup(new AppendSetup(output.getRequestId(), SEGMENT, cid, 0));
        // connection is setup .
        ByteBuffer data = getBuffer("test");
        CompletableFuture<Void> acked = new CompletableFuture<>();
        Append append = new Append(SEGMENT, cid, 1, 1, Unpooled.wrappedBuffer(data), null, output.getRequestId());
        CompletableFuture<Void> acked2 = new CompletableFuture<>();

        Append append2 = new Append(SEGMENT, cid, 2, 1, Unpooled.wrappedBuffer(data), null, output.getRequestId());
        doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                // simulate in a race with segment is sealed callback and a connection drop.
                cf.getProcessor(uri).connectionDropped();
                // wait until the writer reattempts to establish a connection to simulate the race.
                verify(connection, times(2)).send(new SetupAppend(output.getRequestId(), cid, SEGMENT, ""));

                // the connection object throws a throws a
                throw new ConnectionFailedException();
            }
        }).when(connection).send(append);
        // trigger the first write
        output.write(PendingEvent.withoutHeader(null, data, acked));

        AssertExtensions.assertBlocks(() -> {
            // the below write will be blocked since the connection setup is not complete.
            output.write(PendingEvent.withoutHeader(null, data, acked2));
        }, () -> {
            // simulate a race between segmentIsSealed response and appendSetup.
            cf.getProcessor(uri).segmentIsSealed(new WireCommands.SegmentIsSealed(output.getRequestId(), SEGMENT, "Segment is sealed", 1));
            // invoke the segment is sealed to ensure
            cf.getProcessor(uri).appendSetup(new AppendSetup(output.getRequestId(), SEGMENT, cid, 0));
            // ensure the reconnect is invoked inline.
            output.reconnect();
        });
        CompletableFuture<Void> acked3 = new CompletableFuture<>();
        output.write(PendingEvent.withoutHeader(null, data, acked3));
        inOrder.verify(connection).send(append);
        inOrder.verify(connection).send(new SetupAppend(output.getRequestId(), cid, SEGMENT, ""));
        inOrder.verify(connection).send(eq(append2));
        // the setup append should not transmit the inflight events given that the segment is sealed.
        inOrder.verifyNoMoreInteractions();
        assertFalse(acked.isDone());
        assertFalse(acked2.isDone());
        assertFalse(acked3.isDone());
    }


    /**
     * Verifies that if a exception is encountered while flushing data inside of close, the
     * connection is reestablished and the data is retransmitted before close returns.
     */
    @Test
    public void testFailOnClose() throws ConnectionFailedException, InterruptedException {
        UUID cid = UUID.randomUUID();
        PravegaNodeUri uri = new PravegaNodeUri("endpoint", SERVICE_PORT);
        MockConnectionFactoryImpl cf = new MockConnectionFactoryImpl();
        cf.setExecutor(executorService());
        MockController controller = new MockController(uri.getEndpoint(), uri.getPort(), cf, true);
        ClientConnection connection = mock(ClientConnection.class);
        cf.provideConnection(uri, connection);
        SegmentOutputStreamImpl output = new SegmentOutputStreamImpl(SEGMENT, true, controller, cf, cid, segmentSealedCallback,
                                                                     Retry.withoutBackoff(2), DelegationTokenProviderFactory.createWithEmptyToken());
        output.reconnect();
        InOrder inOrder = Mockito.inOrder(connection);
        inOrder.verify(connection).send(new SetupAppend(output.getRequestId(), cid, SEGMENT, ""));
        cf.getProcessor(uri).appendSetup(new AppendSetup(output.getRequestId(), SEGMENT, cid, 0));
        ByteBuffer data = getBuffer("test");
        
        //Prep mock: the mockito doAnswers setup below are triggered during the close inside of the testBlocking() call.
        CompletableFuture<Void> acked = new CompletableFuture<>();
        Append append = new Append(SEGMENT, cid, 1, 1, Unpooled.wrappedBuffer(data), null, output.getRequestId());
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
        output.write(PendingEvent.withoutHeader(null, data, acked));
        inOrder.verify(connection).send(append);
        //Verify behavior
        AssertExtensions.assertBlocks(() -> {
            output.close();
        }, () -> {
            // close is unblocked once the connection is setup and data is appended on Segment store.
            cf.getProcessor(uri).appendSetup(new AppendSetup(output.getRequestId(), SEGMENT, cid, 0));
            cf.getProcessor(uri).dataAppended(new WireCommands.DataAppended(output.getRequestId(), cid, 1, 0, -1));
        });
        // Verify the order of WireCommands sent.
        inOrder.verify(connection).send(new WireCommands.KeepAlive());
        // Two SetupAppend WireCommands are sent since the connection is dropped right after the first KeepAlive WireCommand is sent.
        // The second SetupAppend WireCommand is sent while trying to re-establish connection.
        inOrder.verify(connection, times(2)).send(new SetupAppend(output.getRequestId(), cid, SEGMENT, ""));
        // Ensure the pending append is sent over the connection. The exact verification of the append data is performed while setting up
        // the when clause of setting up append.
        inOrder.verify(connection).sendAsync(Mockito.anyList(), Mockito.any());
        inOrder.verify(connection).close();
        assertEquals(true, acked.isDone());
        inOrder.verifyNoMoreInteractions();
    }

    @Test
    public void testOverSizedWriteFails() throws ConnectionFailedException, SegmentSealedException {
        UUID cid = UUID.randomUUID();
        PravegaNodeUri uri = new PravegaNodeUri("endpoint", SERVICE_PORT);
        MockConnectionFactoryImpl cf = new MockConnectionFactoryImpl();
        cf.setExecutor(executorService());
        MockController controller = new MockController(uri.getEndpoint(), uri.getPort(), cf, true);
        ClientConnection connection = mock(ClientConnection.class);
        cf.provideConnection(uri, connection);
        @Cleanup
        SegmentOutputStreamImpl output = new SegmentOutputStreamImpl(SEGMENT, true, controller, cf, cid, segmentSealedCallback,
                                                                     Retry.withoutBackoff(2), DelegationTokenProviderFactory.createWithEmptyToken());
        output.reconnect();
        verify(connection).send(new SetupAppend(output.getRequestId(), cid, SEGMENT, ""));
        cf.getProcessor(uri).appendSetup(new AppendSetup(output.getRequestId(), SEGMENT, cid, 0));

        ByteBuffer data = ByteBuffer.allocate(PendingEvent.MAX_WRITE_SIZE + 1);
        CompletableFuture<Void> acked = new CompletableFuture<>();
        try {
            output.write(PendingEvent.withoutHeader("routingKey", data, acked));
            fail("Did not throw");
        } catch (IllegalArgumentException e) {
            // expected
        }
        assertEquals(false, acked.isDone());
        verifyNoMoreInteractions(connection);
    }

    @Test(timeout = 10000)
    public void testSealedBeforeFlush() throws Exception {
        UUID cid = UUID.randomUUID();
        PravegaNodeUri uri = new PravegaNodeUri("endpoint", SERVICE_PORT);
        MockConnectionFactoryImpl cf = new MockConnectionFactoryImpl();
        cf.setExecutor(executorService());
        MockController controller = new MockController(uri.getEndpoint(), uri.getPort(), cf, true);
        ClientConnection connection = mock(ClientConnection.class);
        cf.provideConnection(uri, connection);
        InOrder order = Mockito.inOrder(connection);
        @SuppressWarnings("resource")
        SegmentOutputStreamImpl output = new SegmentOutputStreamImpl(SEGMENT, true, controller, cf, cid, segmentSealedCallback,
                Retry.withoutBackoff(2), DelegationTokenProviderFactory.createWithEmptyToken());
        output.reconnect();
        order.verify(connection).send(new SetupAppend(output.getRequestId(), cid, SEGMENT, ""));
        cf.getProcessor(uri).appendSetup(new AppendSetup(output.getRequestId(), SEGMENT, cid, 0));
        ByteBuffer data = getBuffer("test");

        CompletableFuture<Void> ack = new CompletableFuture<>();
        output.write(PendingEvent.withoutHeader(null, data, ack));
        order.verify(connection).send(new Append(SEGMENT, cid, 1, 1, Unpooled.wrappedBuffer(data), null, output.getRequestId()));
        assertEquals(false, ack.isDone());
        cf.getProcessor(uri).segmentIsSealed(new WireCommands.SegmentIsSealed(output.getRequestId(), SEGMENT, "SomeException", 1));
        output.getUnackedEventsOnSeal(); // this is invoked by the segmentSealedCallback.
        AssertExtensions.assertThrows(SegmentSealedException.class, () -> output.flush());
    }

    @Test(timeout = 10000)
    public void testSealedAfterFlush() throws Exception {
        UUID cid = UUID.randomUUID();
        PravegaNodeUri uri = new PravegaNodeUri("endpoint", SERVICE_PORT);
        MockConnectionFactoryImpl cf = new MockConnectionFactoryImpl();
        cf.setExecutor(executorService());
        MockController controller = new MockController(uri.getEndpoint(), uri.getPort(), cf, true);
        ClientConnection connection = mock(ClientConnection.class);
        cf.provideConnection(uri, connection);
        InOrder order = Mockito.inOrder(connection);
        @SuppressWarnings("resource")
        SegmentOutputStreamImpl output = new SegmentOutputStreamImpl(SEGMENT, true, controller, cf, cid, segmentSealedCallback,
                Retry.withoutBackoff(2), DelegationTokenProviderFactory.createWithEmptyToken());
        output.reconnect();
        order.verify(connection).send(new SetupAppend(output.getRequestId(), cid, SEGMENT, ""));
        cf.getProcessor(uri).appendSetup(new AppendSetup(output.getRequestId(), SEGMENT, cid, 0));
        ByteBuffer data = getBuffer("test");

        CompletableFuture<Void> ack = new CompletableFuture<>();
        output.write(PendingEvent.withoutHeader(null, data, ack));
        order.verify(connection).send(new Append(SEGMENT, cid, 1, 1, Unpooled.wrappedBuffer(data), null, output.getRequestId()));
        assertEquals(false, ack.isDone());
        AssertExtensions.assertBlocks(() -> {
            AssertExtensions.assertThrows(SegmentSealedException.class, () -> output.flush());
        }, () -> {
            cf.getProcessor(uri).segmentIsSealed(new WireCommands.SegmentIsSealed(output.getRequestId(), SEGMENT, "SomeException", 1));
            output.getUnackedEventsOnSeal();
        });
        AssertExtensions.assertThrows(SegmentSealedException.class, () -> output.flush());
    }

    /**
     * This test ensures that the flush() on a segment is released only after sealed segment callback is invoked.
     * The callback implemented in EventStreamWriter appends this segment to its sealedSegmentQueue.
     */
    @Test(timeout = 10000)
    public void testFlushIsBlockedUntilCallBackInvoked() throws Exception {

        // Segment sealed callback will finish execution only when the latch is released;
        ReusableLatch latch = new ReusableLatch(false);
        final Consumer<Segment> segmentSealedCallback = segment ->  Exceptions.handleInterrupted(() -> latch.await());

        UUID cid = UUID.randomUUID();
        PravegaNodeUri uri = new PravegaNodeUri("endpoint", SERVICE_PORT);
        MockConnectionFactoryImpl cf = new MockConnectionFactoryImpl();
        cf.setExecutor(executorService());
        MockController controller = new MockController(uri.getEndpoint(), uri.getPort(), cf, true);
        ClientConnection connection = mock(ClientConnection.class);
        cf.provideConnection(uri, connection);
        InOrder order = Mockito.inOrder(connection);
        @SuppressWarnings("resource")
        SegmentOutputStreamImpl output = new SegmentOutputStreamImpl(SEGMENT, true, controller, cf, cid, segmentSealedCallback,
                Retry.withoutBackoff(2), DelegationTokenProviderFactory.createWithEmptyToken());
        output.reconnect();
        order.verify(connection).send(new SetupAppend(output.getRequestId(), cid, SEGMENT, ""));
        cf.getProcessor(uri).appendSetup(new AppendSetup(output.getRequestId(), SEGMENT, cid, 0));
        ByteBuffer data = getBuffer("test");

        CompletableFuture<Void> ack = new CompletableFuture<>();
        output.write(PendingEvent.withoutHeader(null, data, ack));
        order.verify(connection).send(new Append(SEGMENT, cid, 1, 1, Unpooled.wrappedBuffer(data), null, output.getRequestId()));
        assertEquals(false, ack.isDone());

        @Cleanup("shutdownNow")
        ScheduledExecutorService executor = ExecutorServiceHelpers.newScheduledThreadPool(1, "netty-callback");
        //simulate a SegmentIsSealed WireCommand from SegmentStore.
        executor.submit(() -> cf.getProcessor(uri).segmentIsSealed(new WireCommands.SegmentIsSealed(output.getRequestId(), SEGMENT, "SomeException", 1)));

        AssertExtensions.assertBlocks(() -> {
            AssertExtensions.assertThrows(SegmentSealedException.class, () -> output.flush());
        }, () -> latch.release());

        AssertExtensions.assertThrows(SegmentSealedException.class, () -> output.flush());
    }

    @Test(timeout = 10000)
    public void testFailDuringFlush() throws Exception {
        UUID cid = UUID.randomUUID();
        PravegaNodeUri uri = new PravegaNodeUri("endpoint", SERVICE_PORT);
        MockConnectionFactoryImpl cf = new MockConnectionFactoryImpl();
        InlineExecutor executor = new InlineExecutor(); // Ensure task submitted to executor is run inline.
        cf.setExecutor(executor);
        MockController controller = new MockController(uri.getEndpoint(), uri.getPort(), cf, true);
        ClientConnection connection = mock(ClientConnection.class);
        cf.provideConnection(uri, connection);
        InOrder order = Mockito.inOrder(connection);
        @Cleanup
        SegmentOutputStreamImpl output = new SegmentOutputStreamImpl(SEGMENT, true, controller, cf, cid,
                segmentSealedCallback, Retry.withoutBackoff(2), DelegationTokenProviderFactory.createWithEmptyToken());
        output.reconnect();
        order.verify(connection).send(new SetupAppend(output.getRequestId(), cid, SEGMENT, ""));
        cf.getProcessor(uri).appendSetup(new AppendSetup(output.getRequestId(), SEGMENT, cid, 0));
        ByteBuffer data = getBuffer("test");

        CompletableFuture<Void> ack = new CompletableFuture<>();
        output.write(PendingEvent.withoutHeader(null, data, ack));
        order.verify(connection).send(new Append(SEGMENT, cid, 1, 1, Unpooled.wrappedBuffer(data), null, output.getRequestId()));
        assertEquals(false, ack.isDone());

        final CountDownLatch connectionDroppedLatch = new CountDownLatch(1);
        Mockito.doThrow(new ConnectionFailedException()).when(connection).send(new WireCommands.KeepAlive());
        doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                // The segment writer will try to reconnect once the connection is failed post sending a KeepAlive.
                // enable a response for AppendSetup only after the connection dropped is dropped.
                connectionDroppedLatch.await();
                cf.getProcessor(uri).appendSetup(new AppendSetup(output.getRequestId(), SEGMENT, cid, 1));
                return null;
            }
        }).when(connection).send(new SetupAppend(output.getRequestId(), cid, SEGMENT, ""));

        AssertExtensions.assertBlocks(() -> {
            output.flush();
        }, () -> {
            cf.getProcessor(uri).connectionDropped();
            connectionDroppedLatch.countDown();
        });
        order.verify(connection).send(new SetupAppend(output.getRequestId(), cid, SEGMENT, ""));
        assertEquals(true, ack.isDone());
    }

    @Test(timeout = 10000)
    public void testFailureWhileRetransmittingInflight() throws ConnectionFailedException {
        UUID cid = UUID.randomUUID();
        PravegaNodeUri uri = new PravegaNodeUri("endpoint", SERVICE_PORT);
        MockConnectionFactoryImpl cf = new MockConnectionFactoryImpl();
        InlineExecutor executor = new InlineExecutor();
        cf.setExecutor(executor);
        MockController controller = new MockController(uri.getEndpoint(), uri.getPort(), cf, true);
        ClientConnection connection = mock(ClientConnection.class);
        InOrder inOrder = inOrder(connection);
        cf.provideConnection(uri, connection);
        @SuppressWarnings("resource")
        SegmentOutputStreamImpl output = new SegmentOutputStreamImpl(SEGMENT, true, controller, cf, cid, segmentSealedCallback,
                                                                     Retry.withoutBackoff(2), DelegationTokenProviderFactory.createWithEmptyToken());

        output.reconnect();
        cf.getProcessor(uri).appendSetup(new AppendSetup(output.getRequestId(), SEGMENT, cid, 0));
        output.write(PendingEvent.withoutHeader(null, getBuffer("test1"), new CompletableFuture<>()));
        output.write(PendingEvent.withoutHeader(null, getBuffer("test2"), new CompletableFuture<>()));
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
        }).when(connection).sendAsync(ArgumentMatchers.<List<Append>>any(), Mockito.any(CompletedCallback.class));

        doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                cf.getProcessor(uri).appendSetup(new AppendSetup(output.getRequestId(), SEGMENT, cid, 0));
                return null;
            }
        }).doNothing() // disable the sending a new appendSetup the next time SetupAppend is invoked by the segment writer is invoked.
          .when(connection).send(new SetupAppend(output.getRequestId(), cid, SEGMENT, ""));

        cf.getProcessor(uri).connectionDropped();
        AssertExtensions.assertBlocks(() -> output.write(PendingEvent.withoutHeader(null, getBuffer("test3"), new CompletableFuture<>())),
                                      () -> {
                                          // the write should be blocked until the appendSetup is returned by the Segment stores.
                                          cf.getProcessor(uri).appendSetup(new AppendSetup(output.getRequestId(), SEGMENT, cid, 0));
                                      });
        output.write(PendingEvent.withoutHeader(null, getBuffer("test4"), new CompletableFuture<>()));
        
        Append append1 = new Append(SEGMENT, cid, 1, 1, Unpooled.wrappedBuffer(getBuffer("test1")), null, output.getRequestId());
        Append append2 = new Append(SEGMENT, cid, 2, 1, Unpooled.wrappedBuffer(getBuffer("test2")), null, output.getRequestId());
        Append append3 = new Append(SEGMENT, cid, 3, 1, Unpooled.wrappedBuffer(getBuffer("test3")), null, output.getRequestId());
        Append append4 = new Append(SEGMENT, cid, 4, 1, Unpooled.wrappedBuffer(getBuffer("test4")), null, output.getRequestId());
        inOrder.verify(connection).send(new SetupAppend(output.getRequestId(), cid, SEGMENT, ""));
        inOrder.verify(connection).send(append1);
        inOrder.verify(connection).send(append2);
        inOrder.verify(connection).close();
        inOrder.verify(connection).send(new SetupAppend(output.getRequestId(), cid, SEGMENT, ""));
        inOrder.verify(connection).sendAsync(eq(ImmutableList.of(append1, append2)), any());
        inOrder.verify(connection).close();
        inOrder.verify(connection).send(new SetupAppend(output.getRequestId(), cid, SEGMENT, ""));
        inOrder.verify(connection).sendAsync(eq(ImmutableList.of(append1, append2)), any());
        inOrder.verify(connection).send(append3);
        inOrder.verify(connection).send(append4);
        
        verifyNoMoreInteractions(connection);
    }

    @Test(timeout = 10000)
    public void testExceptionSealedCallback() throws Exception {
        UUID cid = UUID.randomUUID();
        PravegaNodeUri uri = new PravegaNodeUri("endpoint", SERVICE_PORT);
        MockConnectionFactoryImpl cf = new MockConnectionFactoryImpl();
        InlineExecutor executor = new InlineExecutor(); // Ensure task submitted to executor is run inline.
        cf.setExecutor(executor);
        MockController controller = new MockController(uri.getEndpoint(), uri.getPort(), cf, true);
        ClientConnection connection = mock(ClientConnection.class);
        cf.provideConnection(uri, connection);
        AtomicBoolean shouldThrow = new AtomicBoolean(true);
        // call back which throws an exception.
        Consumer<Segment> exceptionCallback = s -> {
            if (shouldThrow.getAndSet(false)) {
                throw new IllegalStateException();
            }
        };
        @SuppressWarnings("resource")
        SegmentOutputStreamImpl output = new SegmentOutputStreamImpl(SEGMENT, true, controller, cf, cid, exceptionCallback,
                Retry.withoutBackoff(2), DelegationTokenProviderFactory.createWithEmptyToken());
        output.reconnect();
        verify(connection).send(new SetupAppend(output.getRequestId(), cid, SEGMENT, ""));
        cf.getProcessor(uri).appendSetup(new AppendSetup(output.getRequestId(), SEGMENT, cid, 0));
        ByteBuffer data = getBuffer("test");

        CompletableFuture<Void> ack = new CompletableFuture<>();
        output.write(PendingEvent.withoutHeader(null, data, ack));
        assertEquals(false, ack.isDone());
        Mockito.doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                cf.getProcessor(uri).appendSetup(new AppendSetup(output.getRequestId(), SEGMENT, cid, 0));
                return null;
            }
        }).when(connection).send(new SetupAppend(3, cid, SEGMENT, ""));
        AssertExtensions.assertBlocks(() -> {
            AssertExtensions.assertThrows(SegmentSealedException.class, () -> output.flush());
        }, () -> {
            cf.getProcessor(uri).segmentIsSealed(new WireCommands.SegmentIsSealed(output.getRequestId(), SEGMENT, "SomeException", 1));
            output.getUnackedEventsOnSeal();
        });
        verify(connection).send(new WireCommands.KeepAlive());
        verify(connection).send(new Append(SEGMENT, cid, 1, 1, Unpooled.wrappedBuffer(data), null, output.getRequestId()));
        assertEquals(false, ack.isDone());
    }
    
    @Test(timeout = 10000)
    public void testNoSuchSegment() throws Exception {
        UUID cid = UUID.randomUUID();
        PravegaNodeUri uri = new PravegaNodeUri("endpoint", SERVICE_PORT);
        MockConnectionFactoryImpl cf = new MockConnectionFactoryImpl();
        cf.setExecutor(executorService());
        MockController controller = new MockController(uri.getEndpoint(), uri.getPort(), cf, true);
        ClientConnection connection = mock(ClientConnection.class);
        cf.provideConnection(uri, connection);
        InOrder order = Mockito.inOrder(connection);
        @SuppressWarnings("resource")
        SegmentOutputStreamImpl output = new SegmentOutputStreamImpl(SEGMENT, true, controller, cf, cid, segmentSealedCallback,
                Retry.withoutBackoff(2), DelegationTokenProviderFactory.createWithEmptyToken());
        output.reconnect();
        order.verify(connection).send(new SetupAppend(output.getRequestId(), cid, SEGMENT, ""));
        cf.getProcessor(uri).appendSetup(new AppendSetup(output.getRequestId(), SEGMENT, cid, 0));
        ByteBuffer data = getBuffer("test");

        //Write an Event.
        CompletableFuture<Void> ack = new CompletableFuture<>();
        output.write(PendingEvent.withoutHeader(null, data, ack));
        order.verify(connection).send(new Append(SEGMENT, cid, 1, 1, Unpooled.wrappedBuffer(data), null, output.getRequestId()));
        assertEquals(false, ack.isDone()); //writer is not complete until a response from Segment Store is received.

        //Simulate a No Such Segment while waiting on flush.
        AssertExtensions.assertBlocks(() -> {
            AssertExtensions.assertThrows(SegmentSealedException.class, () -> output.flush());
        }, () -> {
            cf.getProcessor(uri).noSuchSegment(new WireCommands.NoSuchSegment(output.getRequestId(), SEGMENT, "SomeException", -1L));
            output.getUnackedEventsOnSeal();
        });
        AssertExtensions.assertThrows(SegmentSealedException.class, () -> output.flush());
    }

    @Test(timeout = 10000)
    public void testAlreadySealedSegment() throws Exception {
        UUID cid = UUID.randomUUID();
        PravegaNodeUri uri = new PravegaNodeUri("endpoint", SERVICE_PORT);
        MockConnectionFactoryImpl cf = new MockConnectionFactoryImpl();
        cf.setExecutor(executorService());
        MockController controller = new MockController(uri.getEndpoint(), uri.getPort(), cf, true);
        ClientConnection connection = mock(ClientConnection.class);
        cf.provideConnection(uri, connection);
        InOrder order = Mockito.inOrder(connection);
        @SuppressWarnings("resource")
        SegmentOutputStreamImpl output = new SegmentOutputStreamImpl(SEGMENT, true, controller, cf, cid, segmentSealedCallback,
                                                                     Retry.withoutBackoff(2), DelegationTokenProviderFactory.createWithEmptyToken());
        output.reconnect();
        order.verify(connection).send(new SetupAppend(output.getRequestId(), cid, SEGMENT, ""));
        cf.getProcessor(uri).segmentIsSealed(new WireCommands.SegmentIsSealed(output.getRequestId(), SEGMENT, "SomeException", 1));
        cf.getProcessor(uri).appendSetup(new AppendSetup(output.getRequestId(), SEGMENT, cid, 0));
        order.verifyNoMoreInteractions();
    }

    @Test(timeout = 10000)
    public void testFlushDuringTransactionAbort() throws Exception {
        UUID cid = UUID.randomUUID();
        PravegaNodeUri uri = new PravegaNodeUri("endpoint", SERVICE_PORT);
        MockConnectionFactoryImpl cf = new MockConnectionFactoryImpl();
        cf.setExecutor(executorService());
        MockController controller = new MockController(uri.getEndpoint(), uri.getPort(), cf, true);
        ClientConnection connection = mock(ClientConnection.class);
        cf.provideConnection(uri, connection);
        InOrder order = Mockito.inOrder(connection);
        @SuppressWarnings("resource")
        SegmentOutputStreamImpl output = new SegmentOutputStreamImpl(TXN_SEGMENT, true, controller, cf, cid, segmentSealedCallback,
                                                                     Retry.withoutBackoff(2), DelegationTokenProviderFactory.createWithEmptyToken());
        output.reconnect();
        order.verify(connection).send(new SetupAppend(output.getRequestId(), cid, TXN_SEGMENT, ""));
        cf.getProcessor(uri).appendSetup(new AppendSetup(output.getRequestId(), TXN_SEGMENT, cid, 0));
        ByteBuffer data = getBuffer("test");

        // Write an Event.
        CompletableFuture<Void> ack = new CompletableFuture<>();
        output.write(PendingEvent.withoutHeader(null, data, ack));
        order.verify(connection).send(new Append(TXN_SEGMENT, cid, 1, 1, Unpooled.wrappedBuffer(data), null, output.getRequestId()));
        assertFalse(ack.isDone()); //writer is not complete until a response from Segment Store is received.

        // Validate that flush() is blocking until there is a response from Segment Store.
        AssertExtensions.assertBlocks(() -> {
            // A flush() should throw a SegmentSealedException.
            AssertExtensions.assertThrows(SegmentSealedException.class, () -> output.flush());
        }, () -> {
            // Simulate a NoSuchSegment response from SegmentStore due to a Transaction abort.
            cf.getProcessor(uri).noSuchSegment(new WireCommands.NoSuchSegment(output.getRequestId(), TXN_SEGMENT, "SomeException", -1L));
        });
        AssertExtensions.assertThrows(SegmentSealedException.class, () -> output.flush());
    }

    @Test(timeout = 10000)
    public void testCloseDuringTransactionAbort() throws Exception {

        UUID cid = UUID.randomUUID();
        PravegaNodeUri uri = new PravegaNodeUri("endpoint", SERVICE_PORT);
        MockConnectionFactoryImpl cf = new MockConnectionFactoryImpl();
        cf.setExecutor(executorService());
        MockController controller = new MockController(uri.getEndpoint(), uri.getPort(), cf, true);
        ClientConnection connection = mock(ClientConnection.class);
        cf.provideConnection(uri, connection);
        InOrder order = Mockito.inOrder(connection);

        SegmentOutputStreamImpl output = new SegmentOutputStreamImpl(TXN_SEGMENT, true, controller, cf, cid, segmentSealedCallback,
                                                                     Retry.withoutBackoff(2), DelegationTokenProviderFactory.createWithEmptyToken());
        output.reconnect();
        order.verify(connection).send(new SetupAppend(output.getRequestId(), cid, TXN_SEGMENT, ""));
        cf.getProcessor(uri).appendSetup(new AppendSetup(output.getRequestId(), TXN_SEGMENT, cid, 0));
        ByteBuffer data = getBuffer("test");

        // Write an Event.
        CompletableFuture<Void> ack1 = new CompletableFuture<>();
        output.write(PendingEvent.withoutHeader(null, data, ack1));
        order.verify(connection).send(new Append(TXN_SEGMENT, cid, 1, 1, Unpooled.wrappedBuffer(data), null, output.getRequestId()));
        assertFalse(ack1.isDone()); //writer is not complete until a response from Segment Store is received.

        // Simulate a NoSuchSegment response from SegmentStore due to a Transaction abort.
        cf.getProcessor(uri).noSuchSegment(new WireCommands.NoSuchSegment(output.getRequestId(), TXN_SEGMENT, "SomeException", -1L));

        //Trigger a second write.
        CompletableFuture<Void> ack2 = new CompletableFuture<>();
        output.write(PendingEvent.withoutHeader(null, data, ack2));

        // Closing the Segment writer should cause a SegmentSealedException.
        AssertExtensions.assertThrows(SegmentSealedException.class, () -> output.close());
    }

    @Test(timeout = 10000)
    public void testSegmentSealedFollowedbyConnectionDrop() throws Exception {

        @Cleanup("shutdownNow")
        ScheduledExecutorService executor = ExecutorServiceHelpers.newScheduledThreadPool(2, "netty-callback");

        // Segment sealed callback will finish execution only when the releaseCallbackLatch is released;
        ReusableLatch releaseCallbackLatch = new ReusableLatch(false);
        ReusableLatch callBackInvokedLatch = new ReusableLatch(false);
        final Consumer<Segment> segmentSealedCallback = segment ->  Exceptions.handleInterrupted(() -> {
            callBackInvokedLatch.release();
            releaseCallbackLatch.await();
        });

        // Setup mocks.
        UUID cid = UUID.randomUUID();
        PravegaNodeUri uri = new PravegaNodeUri("endpoint", SERVICE_PORT);
        MockConnectionFactoryImpl cf = new MockConnectionFactoryImpl();
        cf.setExecutor(executorService());
        MockController controller = new MockController(uri.getEndpoint(), uri.getPort(), cf, true);
        // Mock client connection that is returned for every invocation of ConnectionFactory#establishConnection.
        ClientConnection connection = mock(ClientConnection.class);
        cf.provideConnection(uri, connection);
        InOrder order = Mockito.inOrder(connection);

        // Create a Segment writer.
        @SuppressWarnings("resource")
        SegmentOutputStreamImpl output = new SegmentOutputStreamImpl(SEGMENT, true, controller, cf, cid, segmentSealedCallback,
                                                                     Retry.withoutBackoff(2), DelegationTokenProviderFactory.createWithEmptyToken());

        // trigger establishment of connection.
        output.reconnect();
        // Verify if SetupAppend is sent over the connection.
        order.verify(connection).send(new SetupAppend(output.getRequestId(), cid, SEGMENT, ""));
        cf.getProcessor(uri).appendSetup(new AppendSetup(output.getRequestId(), SEGMENT, cid, 0));

        // Write an event and ensure inflight has an event.
        ByteBuffer data = getBuffer("test");
        CompletableFuture<Void> ack = new CompletableFuture<>();
        output.write(PendingEvent.withoutHeader(null, data, ack));
        order.verify(connection).send(new Append(SEGMENT, cid, 1, 1, Unpooled.wrappedBuffer(data), null, output.getRequestId()));
        assertFalse(ack.isDone());

        // Simulate a SegmentIsSealed WireCommand from SegmentStore.
        executor.submit(() -> cf.getProcessor(uri).segmentIsSealed(new WireCommands.SegmentIsSealed(output.getRequestId(), SEGMENT, "SomeException", 1)));
        // Wait until callback invocation has been triggered, but has not completed.
        // If the callback is not invoked the test will fail due to a timeout.
        callBackInvokedLatch.await();

        // Now trigger a connection drop netty callback and wait until it is executed.
        executor.submit(() -> cf.getProcessor(uri).connectionDropped()).get();
        // close is invoked on the connection.
        order.verify(connection).close();

        // Verify no further reconnection attempts which involves sending of SetupAppend wire command.
        order.verifyNoMoreInteractions();
        // Release latch to ensure the callback is completed.
        releaseCallbackLatch.release();
        // Verify no further reconnection attempts which involves sending of SetupAppend wire command.
        order.verifyNoMoreInteractions();
        // Trigger a reconnect again and verify if any new connections are initiated.
        output.reconnect();

        // Reconnect operation will be executed on the executor service.
        ScheduledExecutorService service = executorService();
        service.shutdown();
        // Wait until all the tasks for reconnect have been completed.
        service.awaitTermination(10, TimeUnit.SECONDS);

        // Verify no further reconnection attempts which involves sending of SetupAppend wire command.
        order.verifyNoMoreInteractions();
    }

    @Test(timeout = 10000)
    public void testConnectAndSendWithoutConnectionPooling() throws ConnectionFailedException, InterruptedException {
        UUID cid = UUID.randomUUID();
        PravegaNodeUri uri = new PravegaNodeUri("endpoint", SERVICE_PORT);
        MockConnectionFactoryImpl cf = new MockConnectionFactoryImpl();
        cf.setExecutor(executorService());
        MockController controller = new MockController(uri.getEndpoint(), uri.getPort(), cf, true);
        ClientConnection connection = mock(ClientConnection.class);
        cf.provideConnection(uri, connection);
        SegmentOutputStreamImpl output = new SegmentOutputStreamImpl(SEGMENT, false, controller, cf, cid, segmentSealedCallback,
                                                                     Retry.withoutBackoff(2), DelegationTokenProviderFactory.createWithEmptyToken());
        output.reconnect();
        verify(connection).send(new SetupAppend(output.getRequestId(), cid, SEGMENT, ""));
        cf.getProcessor(uri).appendSetup(new AppendSetup(output.getRequestId(), SEGMENT, cid, 0));

        sendAndVerifyEvent(cid, connection, output, getBuffer("test"), 1);
        verifyNoMoreInteractions(connection);
    }

    private static class MockControllerWithTokenTask extends MockController {
        final ConnectionPool cp;
        final CompletableFuture<Void> signal;

        public MockControllerWithTokenTask(String endpoint, int port, ConnectionPool connectionPool,
                                           boolean callServer, CompletableFuture<Void> signal) {
            super(endpoint, port, connectionPool, callServer);
            this.cp = connectionPool;
            this.signal = signal;
        }

        @Override
        public CompletableFuture<String> getOrRefreshDelegationTokenFor(String scope, String streamName,
                                                                        AccessOperation accessOperation) {
            return CompletableFuture.supplyAsync(() -> {
                signal.complete(null);
                return "my-test-token";
            }, cp.getInternalExecutor());
        }
    }
}
