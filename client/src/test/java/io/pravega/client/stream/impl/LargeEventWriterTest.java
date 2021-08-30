/**
 * Copyright Pravega Authors. Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You may obtain a copy of the License
 * at http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable law or agreed to in
 * writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific
 * language governing permissions and limitations under the License.
 */
package io.pravega.client.stream.impl;

import io.netty.buffer.ByteBuf;
import io.pravega.auth.AuthenticationException;
import io.pravega.client.connection.impl.ClientConnection;
import io.pravega.client.security.auth.EmptyTokenProviderImpl;
import io.pravega.client.segment.impl.NoSuchSegmentException;
import io.pravega.client.segment.impl.Segment;
import io.pravega.client.segment.impl.SegmentSealedException;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.Serializer;
import io.pravega.client.stream.mock.MockConnectionFactoryImpl;
import io.pravega.client.stream.mock.MockController;
import io.pravega.common.util.RetriesExhaustedException;
import io.pravega.shared.protocol.netty.ConnectionFailedException;
import io.pravega.shared.protocol.netty.PravegaNodeUri;
import io.pravega.shared.protocol.netty.Reply;
import io.pravega.shared.protocol.netty.WireCommand;
import io.pravega.shared.protocol.netty.WireCommands;
import io.pravega.shared.protocol.netty.WireCommands.AppendSetup;
import io.pravega.shared.protocol.netty.WireCommands.AuthTokenCheckFailed;
import io.pravega.shared.protocol.netty.WireCommands.AuthTokenCheckFailed.ErrorCode;
import io.pravega.shared.protocol.netty.WireCommands.ConditionalBlockEnd;
import io.pravega.shared.protocol.netty.WireCommands.CreateTransientSegment;
import io.pravega.shared.protocol.netty.WireCommands.DataAppended;
import io.pravega.shared.protocol.netty.WireCommands.MergeSegments;
import io.pravega.shared.protocol.netty.WireCommands.NoSuchSegment;
import io.pravega.shared.protocol.netty.WireCommands.OperationUnsupported;
import io.pravega.shared.protocol.netty.WireCommands.SegmentCreated;
import io.pravega.shared.protocol.netty.WireCommands.SegmentIsSealed;
import io.pravega.shared.protocol.netty.WireCommands.SegmentsMerged;
import io.pravega.shared.protocol.netty.WireCommands.SetupAppend;
import io.pravega.shared.protocol.netty.WireCommands.WrongHost;
import io.pravega.test.common.AssertExtensions;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;

public class LargeEventWriterTest {

    private final UUID writerId = UUID.randomUUID();

    @Test(timeout = 5000)
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
                      r -> new AppendSetup(r.getRequestId(), segment.getScopedName(), r.getWriterId(), 0));
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
        writer.writeLargeEvent(segment, buffers, tokenProvider, EventWriterConfig.builder().build());

        assertEquals(4, written.size());
        assertEquals(Serializer.MAX_EVENT_SIZE, written.get(0).readableBytes());
        assertEquals(Serializer.MAX_EVENT_SIZE, written.get(1).readableBytes());
        assertEquals(Serializer.MAX_EVENT_SIZE, written.get(2).readableBytes());
        assertEquals(6 + WireCommands.TYPE_PLUS_LENGTH_SIZE * 3, written.get(3).readableBytes());
    }

    @Test(timeout = 5000)
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
                      r -> new AppendSetup(r.getRequestId(), segment.getScopedName(), r.getWriterId(), 0));
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
                                                                       .initialBackoffMillis(0)
                                                                       .backoffMultiple(1)
                                                                       .retryAttempts(7)
                                                                       .build()));
        assertEquals(8, count.get());

    }

    @Test(timeout = 5000)
    public void testThrownErrors() throws ConnectionFailedException {
        Segment segment = Segment.fromScopedName("foo/bar/1");
        MockConnectionFactoryImpl connectionFactory = new MockConnectionFactoryImpl();
        MockController controller = new MockController("localhost", 0, connectionFactory, false);
        ClientConnection connection = Mockito.mock(ClientConnection.class);
        PravegaNodeUri location = new PravegaNodeUri("localhost", 0);
        connectionFactory.provideConnection(location, connection);
        EmptyTokenProviderImpl tokenProvider = new EmptyTokenProviderImpl();
        EventWriterConfig config = EventWriterConfig.builder().build();
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
    

    @Test(timeout = 5000)
    public void testRetriedErrors() throws ConnectionFailedException, NoSuchSegmentException, AuthenticationException, SegmentSealedException {
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
                      SetupAppend.class,
                      r -> new AppendSetup(r.getRequestId(), segment.getScopedName(), r.getWriterId(), 0));
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

    @Test(timeout = 5000)
    public void testPipelining() throws NoSuchSegmentException, AuthenticationException, SegmentSealedException, ConnectionFailedException {
        Segment segment = Segment.fromScopedName("foo/bar/1");
        MockConnectionFactoryImpl connectionFactory = new MockConnectionFactoryImpl();
        MockController controller = new MockController("localhost", 0, connectionFactory, false);
        ClientConnection connection = Mockito.mock(ClientConnection.class);
        PravegaNodeUri location = new PravegaNodeUri("localhost", 0);
        connectionFactory.provideConnection(location, connection);
        
        ArrayList<ByteBuffer> buffers = new ArrayList<>();
        buffers.add(ByteBuffer.allocate(Serializer.MAX_EVENT_SIZE));
        buffers.add(ByteBuffer.allocate(Serializer.MAX_EVENT_SIZE));
        buffers.add(ByteBuffer.allocate(Serializer.MAX_EVENT_SIZE));
        
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
                      r -> new AppendSetup(r.getRequestId(), segment.getScopedName(), r.getWriterId(), 0));
        
        //If appends are not pipelined, the call to writeLargeEvents will stall waiting for the first reply.
        Mockito.doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                ConditionalBlockEnd argument = (ConditionalBlockEnd) invocation.getArgument(0);
                written.add(argument);
                if (written.size() == buffers.size()) {
                    for (ConditionalBlockEnd append : written) {
                        connectionFactory.getProcessor(location)
                            .dataAppended(new DataAppended(append.getRequestId(),
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
