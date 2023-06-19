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
package io.pravega.client.stream.impl;

import com.google.common.collect.ImmutableMap;
import io.netty.buffer.ByteBuf;
import io.pravega.auth.AuthenticationException;
import io.pravega.client.connection.impl.ClientConnection;
import io.pravega.client.security.auth.EmptyTokenProviderImpl;
import io.pravega.client.segment.impl.NoSuchSegmentException;
import io.pravega.client.segment.impl.Segment;
import io.pravega.client.segment.impl.SegmentOutputStream;
import io.pravega.client.segment.impl.SegmentOutputStreamFactory;
import io.pravega.client.segment.impl.SegmentSealedException;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.Serializer;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.mock.MockConnectionFactoryImpl;
import io.pravega.client.stream.mock.MockController;
import io.pravega.common.util.RetriesExhaustedException;
import io.pravega.shared.NameUtils;
import io.pravega.shared.protocol.netty.ConnectionFailedException;
import io.pravega.shared.protocol.netty.PravegaNodeUri;
import io.pravega.shared.protocol.netty.Reply;
import io.pravega.shared.protocol.netty.WireCommand;
import io.pravega.shared.protocol.netty.WireCommands;
import io.pravega.shared.protocol.netty.WireCommands.AppendSetup;
import io.pravega.shared.protocol.netty.WireCommands.AuthTokenCheckFailed;
import io.pravega.shared.protocol.netty.WireCommands.AuthTokenCheckFailed.ErrorCode;
import io.pravega.shared.protocol.netty.WireCommands.ConditionalBlockEnd;
import io.pravega.shared.protocol.netty.WireCommands.ConditionalCheckFailed;
import io.pravega.shared.protocol.netty.WireCommands.CreateTransientSegment;
import io.pravega.shared.protocol.netty.WireCommands.DataAppended;
import io.pravega.shared.protocol.netty.WireCommands.InvalidEventNumber;
import io.pravega.shared.protocol.netty.WireCommands.MergeSegments;
import io.pravega.shared.protocol.netty.WireCommands.NoSuchSegment;
import io.pravega.shared.protocol.netty.WireCommands.OperationUnsupported;
import io.pravega.shared.protocol.netty.WireCommands.SegmentCreated;
import io.pravega.shared.protocol.netty.WireCommands.SegmentIsSealed;
import io.pravega.shared.protocol.netty.WireCommands.SegmentsMerged;
import io.pravega.shared.protocol.netty.WireCommands.SetupAppend;
import io.pravega.shared.protocol.netty.WireCommands.WrongHost;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.InlineExecutor;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
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
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;

public class LargeEventWriterTest {

    private final UUID writerId = UUID.randomUUID();

    @Test(timeout = 15000)
    public void testBufferSplitting()
            throws NoSuchSegmentException, AuthenticationException, SegmentSealedException, ConnectionFailedException {
        Segment segment = Segment.fromScopedName("foo/bar/1");
        MockConnectionFactoryImpl connectionFactory = new MockConnectionFactoryImpl();
        MockController controller = new MockController("localhost", 0, connectionFactory, false);
        ClientConnection connection = Mockito.mock(ClientConnection.class);
        PravegaNodeUri location = new PravegaNodeUri("localhost", 0);
        connectionFactory.provideConnection(location, connection);
        ArrayList<ByteBuf> written = new ArrayList<>();

        answerRequest(connectionFactory,
                      connection,
                      location,
                      CreateTransientSegment.class,
                      r -> new SegmentCreated(r.getRequestId(), "transient-segment"));
        answerRequest(connectionFactory,
                      connection,
                      location,
                      SetupAppend.class,
                      r -> new AppendSetup(r.getRequestId(), segment.getScopedName(), r.getWriterId(), WireCommands.NULL_ATTRIBUTE_VALUE));
        answerRequest(connectionFactory, connection, location, ConditionalBlockEnd.class, r -> {
            ByteBuf data = r.getData();
            written.add(data);
            return new DataAppended(r.getRequestId(),
                    r.getWriterId(),
                    r.getEventNumber(),
                    r.getEventNumber() - 1,
                    r.getExpectedOffset() + data.readableBytes());
        });
        answerRequest(connectionFactory, connection, location, MergeSegments.class, r -> {
            return new SegmentsMerged(r.getRequestId(), r.getSource(), r.getTarget(), -1);
        });

        LargeEventWriter writer = new LargeEventWriter(writerId, controller, connectionFactory);
        EmptyTokenProviderImpl tokenProvider = new EmptyTokenProviderImpl();
        ArrayList<ByteBuffer> buffers = new ArrayList<>();
        buffers.add(ByteBuffer.allocate(Serializer.MAX_EVENT_SIZE * 2 + 1));
        buffers.add(ByteBuffer.allocate(Serializer.MAX_EVENT_SIZE));
        buffers.add(ByteBuffer.allocate(5));
        writer.writeLargeEvent(segment, buffers, tokenProvider, EventWriterConfig.builder().enableLargeEvents(true).build());

        assertEquals(4, written.size());
        assertEquals(Serializer.MAX_EVENT_SIZE, written.get(0).readableBytes());
        assertEquals(Serializer.MAX_EVENT_SIZE, written.get(1).readableBytes());
        assertEquals(Serializer.MAX_EVENT_SIZE, written.get(2).readableBytes());
        assertEquals(6 + WireCommands.TYPE_PLUS_LENGTH_SIZE * 3, written.get(3).readableBytes());
    }

    @Test(timeout = 15000)
    public void testRetries()
            throws ConnectionFailedException, NoSuchSegmentException, AuthenticationException {
        Segment segment = Segment.fromScopedName("foo/bar/1");
        MockConnectionFactoryImpl connectionFactory = new MockConnectionFactoryImpl();
        MockController controller = new MockController("localhost", 0, connectionFactory, false);
        ClientConnection connection = Mockito.mock(ClientConnection.class);
        PravegaNodeUri location = new PravegaNodeUri("localhost", 0);
        connectionFactory.provideConnection(location, connection);
        AtomicInteger count = new AtomicInteger(0);

        answerRequest(connectionFactory,
                      connection,
                      location,
                      CreateTransientSegment.class,
                      r -> new SegmentCreated(r.getRequestId(), "transient-segment"));
        answerRequest(connectionFactory,
                      connection,
                      location,
                      SetupAppend.class,
                      r -> new AppendSetup(r.getRequestId(), segment.getScopedName(), r.getWriterId(), WireCommands.NULL_ATTRIBUTE_VALUE));
        answerRequest(connectionFactory, connection, location, ConditionalBlockEnd.class, r -> {
            count.getAndIncrement();
            return new WrongHost(r.getRequestId(), segment.getScopedName(), null, null);
        });

        LargeEventWriter writer = new LargeEventWriter(writerId, controller, connectionFactory);
        EmptyTokenProviderImpl tokenProvider = new EmptyTokenProviderImpl();
        ArrayList<ByteBuffer> buffers = new ArrayList<>();
        buffers.add(ByteBuffer.allocate(5));
        AssertExtensions.assertThrows(RetriesExhaustedException.class,
                                      () -> writer.writeLargeEvent(segment,
                                                                   buffers,
                                                                   tokenProvider,
                                                                   EventWriterConfig.builder()
                                                                       .initialBackoffMillis(1)
                                                                       .backoffMultiple(1)
                                                                       .enableLargeEvents(true)
                                                                       .retryAttempts(7)
                                                                       .build()));
        assertEquals(8, count.get());

    }

    @Test(timeout = 15000)
    public void testThrownErrors() throws ConnectionFailedException {
        Segment segment = Segment.fromScopedName("foo/bar/1");
        MockConnectionFactoryImpl connectionFactory = new MockConnectionFactoryImpl();
        MockController controller = new MockController("localhost", 0, connectionFactory, false);
        ClientConnection connection = Mockito.mock(ClientConnection.class);
        PravegaNodeUri location = new PravegaNodeUri("localhost", 0);
        connectionFactory.provideConnection(location, connection);
        EmptyTokenProviderImpl tokenProvider = new EmptyTokenProviderImpl();
        EventWriterConfig config = EventWriterConfig.builder().enableLargeEvents(true).build();
        ArrayList<ByteBuffer> events = new ArrayList<>();
        events.add(ByteBuffer.allocate(1));
        
        answerRequest(connectionFactory,
                      connection,
                      location,
                      CreateTransientSegment.class,
                      r -> new NoSuchSegment(r.getRequestId(), "foo/bar/1", "stacktrace", -1));
        
        AssertExtensions.assertThrows(NoSuchSegmentException.class, () -> {
            LargeEventWriter writer = new LargeEventWriter(writerId, controller, connectionFactory);
            writer.writeLargeEvent(segment, events, tokenProvider, config);
        });
        
        answerRequest(connectionFactory,
                      connection,
                      location,
                      CreateTransientSegment.class,
                      r -> new SegmentIsSealed(r.getRequestId(), "foo/bar/1", "stacktrace", -1));
        
        AssertExtensions.assertThrows(SegmentSealedException.class, () -> {
            LargeEventWriter writer = new LargeEventWriter(writerId, controller, connectionFactory);
            writer.writeLargeEvent(segment, events, tokenProvider, config);
        });
        
        answerRequest(connectionFactory,
                      connection,
                      location,
                      CreateTransientSegment.class,
                      r -> new OperationUnsupported(r.getRequestId(), "CreateTransientSegment", "stacktrace"));
        
        AssertExtensions.assertThrows(UnsupportedOperationException.class, () -> {
            LargeEventWriter writer = new LargeEventWriter(writerId, controller, connectionFactory);
            writer.writeLargeEvent(segment, events, tokenProvider, config);
        });
    }
    
    @Test(timeout = 15000)
    public void testRetriedErrors() throws ConnectionFailedException, NoSuchSegmentException, AuthenticationException, SegmentSealedException {
        Segment segment = Segment.fromScopedName("foo/bar/1");
        MockConnectionFactoryImpl connectionFactory = new MockConnectionFactoryImpl();
        MockController controller = new MockController("localhost", 0, connectionFactory, false);
        ClientConnection connection = Mockito.mock(ClientConnection.class);
        PravegaNodeUri location = new PravegaNodeUri("localhost", 0);
        connectionFactory.provideConnection(location, connection);
        EmptyTokenProviderImpl tokenProvider = new EmptyTokenProviderImpl();
        ArrayList<ByteBuffer> events = new ArrayList<>();
        events.add(ByteBuffer.allocate(1));
        
        AtomicBoolean failed = new AtomicBoolean(false);
        AtomicBoolean succeeded = new AtomicBoolean(false);
               
        answerRequest(connectionFactory,
                      connection,
                      location,
                      SetupAppend.class,
                      r -> new AppendSetup(r.getRequestId(), segment.getScopedName(), r.getWriterId(), WireCommands.NULL_ATTRIBUTE_VALUE));
        answerRequest(connectionFactory, connection, location, ConditionalBlockEnd.class, r -> {
            ByteBuf data = r.getData();
            return new DataAppended(r.getRequestId(),
                    r.getWriterId(),
                    r.getEventNumber(),
                    r.getEventNumber() - 1,
                    r.getExpectedOffset() + data.readableBytes());
        });
        answerRequest(connectionFactory, connection, location, MergeSegments.class, r -> {
            return new SegmentsMerged(r.getRequestId(), r.getSource(), r.getTarget(), -1);
        });

        Mockito.doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                CreateTransientSegment argument = (CreateTransientSegment) invocation.getArgument(0);
                failed.set(true);
                connectionFactory.getProcessor(location).process(new AuthTokenCheckFailed(argument.getRequestId(), "stacktrace", ErrorCode.TOKEN_EXPIRED));
                return null;
            }
        }).doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                CreateTransientSegment argument = (CreateTransientSegment) invocation.getArgument(0);
                succeeded.set(true);
                connectionFactory.getProcessor(location).process(new SegmentCreated(argument.getRequestId(), "transient-segment"));
                return null;
            }
        }).when(connection).send(any(CreateTransientSegment.class));
        
        LargeEventWriter writer = new LargeEventWriter(writerId, controller, connectionFactory);
        writer.writeLargeEvent(segment, events, tokenProvider, EventWriterConfig.builder().build());
        assertTrue(failed.getAndSet(false));
        assertTrue(succeeded.getAndSet(false));
        
        Mockito.doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                CreateTransientSegment argument = (CreateTransientSegment) invocation.getArgument(0);
                failed.set(true);
                connectionFactory.getProcessor(location).process(new WrongHost(argument.getRequestId(), "foo/bar/1", null, "stacktrace"));
                return null;
            }
        }).doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                CreateTransientSegment argument = (CreateTransientSegment) invocation.getArgument(0);
                succeeded.set(true);
                connectionFactory.getProcessor(location).process(new SegmentCreated(argument.getRequestId(), "transient-segment"));
                return null;
            }
        }).when(connection).send(any(CreateTransientSegment.class));
        
        writer = new LargeEventWriter(writerId, controller, connectionFactory);
        writer.writeLargeEvent(segment, events, tokenProvider, EventWriterConfig.builder().build());
        assertTrue(failed.getAndSet(false));
        assertTrue(succeeded.getAndSet(false));
        
        Mockito.doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                CreateTransientSegment argument = (CreateTransientSegment) invocation.getArgument(0);
                failed.set(true);
                connectionFactory.getProcessor(location).processingFailure(new ConnectionFailedException());
                return null;
            }
        }).doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                CreateTransientSegment argument = (CreateTransientSegment) invocation.getArgument(0);
                succeeded.set(true);
                connectionFactory.getProcessor(location).process(new SegmentCreated(argument.getRequestId(), "transient-segment"));
                return null;
            }
        }).when(connection).send(any(CreateTransientSegment.class));
        
        writer = new LargeEventWriter(writerId, controller, connectionFactory);
        writer.writeLargeEvent(segment, events, tokenProvider, EventWriterConfig.builder().build());
        assertTrue(failed.getAndSet(false));
        assertTrue(succeeded.getAndSet(false));
    }

    @Test(timeout = 15000)
    public void testUnexpectedErrors() throws ConnectionFailedException, NoSuchSegmentException, AuthenticationException, SegmentSealedException {
        Segment segment = Segment.fromScopedName("foo/bar/1");
        MockConnectionFactoryImpl connectionFactory = new MockConnectionFactoryImpl();
        MockController controller = new MockController("localhost", 0, connectionFactory, false);
        ClientConnection connection = Mockito.mock(ClientConnection.class);
        PravegaNodeUri location = new PravegaNodeUri("localhost", 0);
        connectionFactory.provideConnection(location, connection);
        EmptyTokenProviderImpl tokenProvider = new EmptyTokenProviderImpl();
        EventWriterConfig config = EventWriterConfig.builder().initialBackoffMillis(0).build();
        ArrayList<ByteBuffer> events = new ArrayList<>();
        events.add(ByteBuffer.allocate(1));
        
        AtomicBoolean failed = new AtomicBoolean(false);
        AtomicBoolean succeeded = new AtomicBoolean(false);
        
        answerRequest(connectionFactory,
                      connection,
                      location,
                      CreateTransientSegment.class,
                      r -> new SegmentCreated(r.getRequestId(), "transient-segment"));
        answerRequest(connectionFactory,
                      connection,
                      location,
                      SetupAppend.class,
                      r -> new AppendSetup(r.getRequestId(), segment.getScopedName(), r.getWriterId(), WireCommands.NULL_ATTRIBUTE_VALUE));
        
        Mockito.doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                ConditionalBlockEnd argument = (ConditionalBlockEnd) invocation.getArgument(0);
                failed.set(true);
                connectionFactory.getProcessor(location).process(new InvalidEventNumber(argument.getWriterId(), argument.getRequestId(), "stacktrace", argument.getEventNumber()));
                return null;
            }
        }).doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                ConditionalBlockEnd argument = (ConditionalBlockEnd) invocation.getArgument(0);
                ByteBuf data = argument.getData();
                succeeded.set(true);
                connectionFactory.getProcessor(location).process(new DataAppended(argument.getRequestId(),
                                                                                  argument.getWriterId(),
                                                                                  argument.getEventNumber(),
                                                                                  argument.getEventNumber() - 1,
                                                                                  argument.getExpectedOffset() + data.readableBytes()));
                return null;
            }
        }).when(connection).send(any(ConditionalBlockEnd.class));
        answerRequest(connectionFactory, connection, location, MergeSegments.class, r -> {
            return new SegmentsMerged(r.getRequestId(), r.getSource(), r.getTarget(), -1);
        });

        LargeEventWriter writer = new LargeEventWriter(writerId, controller, connectionFactory);
        writer.writeLargeEvent(segment, events, tokenProvider, EventWriterConfig.builder().build());
        assertTrue(failed.getAndSet(false));
        assertTrue(succeeded.getAndSet(false));
        
        answerRequest(connectionFactory,
                      connection,
                      location,
                      CreateTransientSegment.class,
                      r -> new SegmentCreated(r.getRequestId(), "transient-segment"));
        answerRequest(connectionFactory,
                      connection,
                      location,
                      SetupAppend.class,
                      r -> new AppendSetup(r.getRequestId(), segment.getScopedName(), r.getWriterId(), WireCommands.NULL_ATTRIBUTE_VALUE));
        
        Mockito.doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                ConditionalBlockEnd argument = (ConditionalBlockEnd) invocation.getArgument(0);
                failed.set(true);
                connectionFactory.getProcessor(location).process(new ConditionalCheckFailed(argument.getWriterId(), argument.getEventNumber(), argument.getRequestId()));
                return null;
            }
        }).doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                ConditionalBlockEnd argument = (ConditionalBlockEnd) invocation.getArgument(0);
                ByteBuf data = argument.getData();
                succeeded.set(true);
                connectionFactory.getProcessor(location).process(new DataAppended(argument.getRequestId(),
                                                                                  argument.getWriterId(),
                                                                                  argument.getEventNumber(),
                                                                                  argument.getEventNumber() - 1,
                                                                                  argument.getExpectedOffset() + data.readableBytes()));
                return null;
            }
        }).when(connection).send(any(ConditionalBlockEnd.class));
        answerRequest(connectionFactory, connection, location, MergeSegments.class, r -> {
            return new SegmentsMerged(r.getRequestId(), r.getSource(), r.getTarget(), -1);
        });
        
        writer = new LargeEventWriter(writerId, controller, connectionFactory);
        writer.writeLargeEvent(segment, events, tokenProvider, EventWriterConfig.builder().build());
        assertTrue(failed.getAndSet(false));
        assertTrue(succeeded.getAndSet(false));
    }
    
    @Test(timeout = 15000)
    public void testEventStreamWriter() throws ConnectionFailedException, SegmentSealedException {
        String scope = "scope";
        String streamName = "stream";
        StreamImpl stream = new StreamImpl(scope, streamName);
        Segment segment = new Segment(scope, streamName, 0);
        @Cleanup
        InlineExecutor executor = new InlineExecutor();
        EventWriterConfig config = EventWriterConfig.builder().enableLargeEvents(true).build();
        SegmentOutputStreamFactory streamFactory = Mockito.mock(SegmentOutputStreamFactory.class);
        MockConnectionFactoryImpl connectionFactory = new MockConnectionFactoryImpl();
        MockController controller = new MockController("localhost", 0, connectionFactory, false);
        controller.createScope(scope).join();
        controller.createStream(scope, streamName, StreamConfiguration.builder().build());
        PravegaNodeUri location = new PravegaNodeUri("localhost", 0);
        ClientConnection connection = Mockito.mock(ClientConnection.class);
        connectionFactory.provideConnection(location, connection);

        SegmentOutputStream outputStream = Mockito.mock(SegmentOutputStream.class);
        Mockito.when(streamFactory.createOutputStreamForSegment(eq(segment), any(), any(), any())).thenReturn(outputStream);

        answerRequest(connectionFactory,
                      connection,
                      location,
                      CreateTransientSegment.class,
                      r -> new SegmentCreated(r.getRequestId(), "transient-segment"));
        answerRequest(connectionFactory,
                      connection,
                      location,
                      SetupAppend.class,
                      r -> new AppendSetup(r.getRequestId(), segment.getScopedName(), r.getWriterId(), WireCommands.NULL_ATTRIBUTE_VALUE));
        answerRequest(connectionFactory, connection, location, ConditionalBlockEnd.class, r -> {
            ByteBuf data = r.getData();
            return new DataAppended(r.getRequestId(),
                    r.getWriterId(),
                    r.getEventNumber(),
                    r.getEventNumber() - 1,
                    r.getExpectedOffset() + data.readableBytes());
        });
        answerRequest(connectionFactory, connection, location, MergeSegments.class, r -> {
            return new SegmentsMerged(r.getRequestId(), r.getSource(), r.getTarget(), -1);
        });
        @Cleanup
        EventStreamWriter<byte[]> writer = new EventStreamWriterImpl<>(stream, "id", controller, streamFactory,
                new ByteArraySerializer(), config, executor, executor, connectionFactory);
        writer.writeEvent(new byte[] { 0, 1, 2, 3, 4, 5, 6, 7, 8 });
        writer.writeEvent(new byte[Serializer.MAX_EVENT_SIZE * 2]);
        InOrder order = Mockito.inOrder(connection, outputStream);
        order.verify(outputStream).write(any(PendingEvent.class));
        order.verify(outputStream).flush();
        order.verify(connection).send(any(CreateTransientSegment.class));
        order.verify(connection).send(any(SetupAppend.class));
        order.verify(connection, times(3)).send(any(ConditionalBlockEnd.class));
        order.verify(connection).send(any(MergeSegments.class));
        order.verify(connection).close();
        order.verifyNoMoreInteractions();
    }
    
    @Test(timeout = 15000)
    public void testSegmentSealed() throws ConnectionFailedException, SegmentSealedException {
        String scope = "scope";
        String streamName = "stream";
        StreamImpl stream = new StreamImpl(scope, streamName);
        Segment segment = new Segment(scope, streamName, 0);
        @Cleanup
        InlineExecutor executor = new InlineExecutor();
        EventWriterConfig config = EventWriterConfig.builder().enableLargeEvents(true).build();
        SegmentOutputStreamFactory streamFactory = Mockito.mock(SegmentOutputStreamFactory.class);
        MockConnectionFactoryImpl connectionFactory = new MockConnectionFactoryImpl();
        MockController controller = spy(new MockController("localhost", 0, connectionFactory, false));
        controller.createScope(scope).join();
        controller.createStream(scope, streamName, StreamConfiguration.builder().build());
        PravegaNodeUri location = new PravegaNodeUri("localhost", 0);
        ClientConnection connection = Mockito.mock(ClientConnection.class);
        connectionFactory.provideConnection(location, connection);

        SegmentOutputStream outputStream = Mockito.mock(SegmentOutputStream.class);
        Mockito.when(streamFactory.createOutputStreamForSegment(any(), any(), any(), any())).thenReturn(outputStream);

        AtomicBoolean failed = new AtomicBoolean(false);
        AtomicBoolean succeeded = new AtomicBoolean(false);
        
        Mockito.doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                CreateTransientSegment argument = (CreateTransientSegment) invocation.getArgument(0);
                failed.set(true);
                connectionFactory.getProcessor(location).process(new SegmentIsSealed(argument.getRequestId(), segment.getScopedName(), "stacktrace", 0));
                return null;
            }
        }).doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                CreateTransientSegment argument = (CreateTransientSegment) invocation.getArgument(0);
                succeeded.set(true);
                connectionFactory.getProcessor(location).process(new SegmentCreated(argument.getRequestId(), "transient-segment"));
                return null;
            }
        }).when(connection).send(any(CreateTransientSegment.class));

        Mockito.doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                List<Long> predecessors = Arrays.asList(0L);

                return CompletableFuture.completedFuture(new StreamSegmentsWithPredecessors(
                        ImmutableMap.of(new SegmentWithRange(new Segment(scope, streamName, NameUtils.computeSegmentId(1, 1)), 0.0, 0.5), predecessors,
                                 new SegmentWithRange(new Segment(scope, streamName, NameUtils.computeSegmentId(2, 1)), 0.5, 1.0), predecessors),
                        ""));
            }
        }).when(controller).getSuccessors(segment);

        answerRequest(connectionFactory,
                      connection,
                      location,
                      SetupAppend.class,
                      r -> new AppendSetup(r.getRequestId(), segment.getScopedName(), r.getWriterId(), WireCommands.NULL_ATTRIBUTE_VALUE));
        answerRequest(connectionFactory, connection, location, ConditionalBlockEnd.class, r -> {
            ByteBuf data = r.getData();
            return new DataAppended(r.getRequestId(),
                    r.getWriterId(),
                    r.getEventNumber(),
                    r.getEventNumber() - 1,
                    r.getExpectedOffset() + data.readableBytes());
        });
        answerRequest(connectionFactory, connection, location, MergeSegments.class, r -> {
            return new SegmentsMerged(r.getRequestId(), r.getSource(), r.getTarget(), -1);
        });
        @Cleanup
        EventStreamWriter<byte[]> writer = new EventStreamWriterImpl<>(stream, "id", controller, streamFactory,
                new ByteArraySerializer(), config, executor, executor, connectionFactory);
        writer.writeEvent(new byte[] { 0, 1, 2, 3, 4, 5, 6, 7, 8 });
        writer.writeEvent(new byte[Serializer.MAX_EVENT_SIZE * 2]);
        assertTrue(failed.get());
        assertTrue(succeeded.get());
        InOrder order = Mockito.inOrder(connection, outputStream);
        order.verify(outputStream).write(any(PendingEvent.class));
        order.verify(outputStream).flush();
        order.verify(connection, times(2)).send(any(CreateTransientSegment.class));
        order.verify(connection).send(any(SetupAppend.class));
        order.verify(connection, times(3)).send(any(ConditionalBlockEnd.class));
        order.verify(connection).send(any(MergeSegments.class));
        order.verify(connection).close();
        order.verifyNoMoreInteractions();
    }

    @Test(timeout = 5000)
    public void testPipelining() throws NoSuchSegmentException, AuthenticationException, SegmentSealedException, ConnectionFailedException {
        Segment segment = Segment.fromScopedName("foo/bar/1");
        MockConnectionFactoryImpl connectionFactory = new MockConnectionFactoryImpl();
        MockController controller = new MockController("localhost", 0, connectionFactory, false);
        ClientConnection connection = Mockito.mock(ClientConnection.class);
        PravegaNodeUri location = new PravegaNodeUri("localhost", 0);
        connectionFactory.provideConnection(location, connection);
        
        ArrayList<ByteBuffer> buffers = new ArrayList<>();
        buffers.add(ByteBuffer.allocate(Serializer.MAX_EVENT_SIZE - WireCommands.TYPE_PLUS_LENGTH_SIZE));
        buffers.add(ByteBuffer.allocate(Serializer.MAX_EVENT_SIZE - WireCommands.TYPE_PLUS_LENGTH_SIZE));
        buffers.add(ByteBuffer.allocate(Serializer.MAX_EVENT_SIZE - WireCommands.TYPE_PLUS_LENGTH_SIZE));
        
        ArrayList<ConditionalBlockEnd> written = new ArrayList<>();

        answerRequest(connectionFactory,
                      connection,
                      location,
                      CreateTransientSegment.class,
                      r -> new SegmentCreated(r.getRequestId(), "transient-segment"));
        answerRequest(connectionFactory,
                      connection,
                      location,
                      SetupAppend.class,
                      r -> new AppendSetup(r.getRequestId(), segment.getScopedName(), r.getWriterId(), WireCommands.NULL_ATTRIBUTE_VALUE));
        
        //If appends are not pipelined, the call to writeLargeEvents will stall waiting for the first reply.
        Mockito.doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                ConditionalBlockEnd argument = (ConditionalBlockEnd) invocation.getArgument(0);
                written.add(argument);
                if (written.size() == buffers.size()) {
                    for (ConditionalBlockEnd append : written) {
                        connectionFactory.getProcessor(location)
                            .process(new DataAppended(append.getRequestId(),
                                     writerId,
                                     append.getEventNumber(),
                                     append.getEventNumber() - 1,
                                     append.getExpectedOffset() + append.getData().readableBytes()));
                    }
                }
                return null;
            }
        }).when(connection).send(any(ConditionalBlockEnd.class));
        
        answerRequest(connectionFactory, connection, location, MergeSegments.class, r -> {
            return new SegmentsMerged(r.getRequestId(), r.getSource(), r.getTarget(), -1);
        });

        LargeEventWriter writer = new LargeEventWriter(writerId, controller, connectionFactory);
        EmptyTokenProviderImpl tokenProvider = new EmptyTokenProviderImpl();

        writer.writeLargeEvent(segment, buffers, tokenProvider, EventWriterConfig.builder().build());
    }

    private <R extends WireCommand> void answerRequest(MockConnectionFactoryImpl connectionFactory,
            ClientConnection connection, PravegaNodeUri location, Class<R> request, Function<R, ? extends Reply> fReply)
            throws ConnectionFailedException {
        Mockito.doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                R argument = (R) invocation.getArgument(0);
                connectionFactory.getProcessor(location).process(fReply.apply(argument));
                return null;
            }
        }).when(connection).send(any(request));
    }

}
