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

import io.pravega.auth.AuthenticationException;
import io.pravega.auth.TokenExpiredException;
import io.pravega.client.connection.impl.ClientConnection;
import io.pravega.client.security.auth.DelegationTokenProviderFactory;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.mock.MockConnectionFactoryImpl;
import io.pravega.client.stream.mock.MockController;
import io.pravega.shared.security.auth.AccessOperation;
import io.pravega.common.util.ByteBufferUtils;
import io.pravega.shared.protocol.netty.ConnectionFailedException;
import io.pravega.shared.protocol.netty.PravegaNodeUri;
import io.pravega.shared.protocol.netty.ReplyProcessor;
import io.pravega.shared.protocol.netty.WireCommands;
import io.pravega.shared.protocol.netty.WireCommands.AppendSetup;
import io.pravega.shared.protocol.netty.WireCommands.ConditionalAppend;
import io.pravega.shared.protocol.netty.WireCommands.SetupAppend;
import io.pravega.test.common.AssertExtensions;
import java.nio.ByteBuffer;
import java.util.UUID;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

import lombok.Cleanup;
import lombok.SneakyThrows;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;

public class ConditionalOutputStreamTest {

    @Test(timeout = 10000)
    public void testWrite() throws ConnectionFailedException, SegmentSealedException {
        @Cleanup
        MockConnectionFactoryImpl connectionFactory = new MockConnectionFactoryImpl();
        @Cleanup
        MockController controller = new MockController("localhost", 0, connectionFactory, true);
        ConditionalOutputStreamFactory factory = new ConditionalOutputStreamFactoryImpl(controller, connectionFactory);
        Segment segment = new Segment("scope", "testWrite", 1);
        @Cleanup
        ConditionalOutputStream cOut = factory.createConditionalOutputStream(segment,
                DelegationTokenProviderFactory.create("token", controller, segment, AccessOperation.ANY),
                EventWriterConfig.builder().build());
        ByteBuffer data = ByteBuffer.allocate(10);
        
        ClientConnection mock = Mockito.mock(ClientConnection.class);
        PravegaNodeUri location = new PravegaNodeUri("localhost", 0);
        connectionFactory.provideConnection(location, mock);
        setupAppend(connectionFactory, segment, mock, location);
        Mockito.doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                ConditionalAppend argument = (ConditionalAppend) invocation.getArgument(0);
                ReplyProcessor processor = connectionFactory.getProcessor(location);
                if (argument.getExpectedOffset() == 0 || argument.getExpectedOffset() == 2) {
                    processor.process(new WireCommands.DataAppended(argument.getRequestId(), argument.getWriterId(), argument.getEventNumber(), 0, -1));
                } else { 
                    processor.process(new WireCommands.ConditionalCheckFailed(argument.getWriterId(), argument.getEventNumber(), argument.getRequestId()));
                }
                return null;
            }
        }).when(mock).send(any(ConditionalAppend.class));
        assertTrue(cOut.write(data, 0));
        assertFalse(cOut.write(data, 1));
        assertTrue(cOut.write(data, 2));
        assertFalse(cOut.write(data, 3));
    }

    @Test(timeout = 10000)
    public void testClose() {
        @Cleanup
        MockConnectionFactoryImpl connectionFactory = new MockConnectionFactoryImpl();
        @Cleanup
        MockController controller = new MockController("localhost", 0, connectionFactory, true);
        ConditionalOutputStreamFactory factory = new ConditionalOutputStreamFactoryImpl(controller, connectionFactory);
        Segment segment = new Segment("scope", "testWrite", 1);
        @Cleanup
        ConditionalOutputStream cOut = factory.createConditionalOutputStream(segment,
                DelegationTokenProviderFactory.create("token", controller, segment, AccessOperation.ANY),
                EventWriterConfig.builder().build());
        cOut.close();
        AssertExtensions.assertThrows(IllegalStateException.class, () -> cOut.write(ByteBufferUtils.EMPTY, 0));
    }

    @Test(timeout = 10000)
    public void testRetries() throws ConnectionFailedException, SegmentSealedException {
        @Cleanup
        MockConnectionFactoryImpl connectionFactory = new MockConnectionFactoryImpl();
        @Cleanup
        MockController controller = new MockController("localhost", 0, connectionFactory, true);
        ConditionalOutputStreamFactory factory = new ConditionalOutputStreamFactoryImpl(controller, connectionFactory);
        Segment segment = new Segment("scope", "testWrite", 1);
        @Cleanup
        ConditionalOutputStream cOut = factory.createConditionalOutputStream(segment,
                DelegationTokenProviderFactory.create("token", controller, segment, AccessOperation.ANY),
                EventWriterConfig.builder().build());
        ByteBuffer data = ByteBuffer.allocate(10);
        
        ClientConnection mock = Mockito.mock(ClientConnection.class);
        PravegaNodeUri location = new PravegaNodeUri("localhost", 0);
        connectionFactory.provideConnection(location, mock);
        setupAppend(connectionFactory, segment, mock, location);
        final AtomicLong count = new AtomicLong(0);
        Mockito.doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                ConditionalAppend argument = (ConditionalAppend) invocation.getArgument(0);
                ReplyProcessor processor = connectionFactory.getProcessor(location);
                if (count.getAndIncrement() < 2) {
                    processor.connectionDropped();
                } else {
                    processor.process(new WireCommands.DataAppended(argument.getRequestId(), argument.getWriterId(), argument.getEventNumber(), 0, -1));
                }                
                return null;
            }
        }).when(mock).send(any(ConditionalAppend.class));
        assertTrue(cOut.write(data, 0));
        assertEquals(3, count.get());
    }

    private void setupAppend(MockConnectionFactoryImpl connectionFactory, Segment segment, ClientConnection mock,
                             PravegaNodeUri location) throws ConnectionFailedException {
        Mockito.doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                SetupAppend argument = (SetupAppend) invocation.getArgument(0);
                connectionFactory.getProcessor(location)
                                 .process(new AppendSetup(argument.getRequestId(), segment.getScopedName(),
                                                              argument.getWriterId(), 0));
                return null;
            }
        }).when(mock).send(any(SetupAppend.class));
    }

    @Test(timeout = 10000)
    public void testSegmentSealed() throws ConnectionFailedException {
        @Cleanup
        MockConnectionFactoryImpl connectionFactory = new MockConnectionFactoryImpl();
        @Cleanup
        MockController controller = new MockController("localhost", 0, connectionFactory, true);
        ConditionalOutputStreamFactory factory = new ConditionalOutputStreamFactoryImpl(controller, connectionFactory);
        Segment segment = new Segment("scope", "testWrite", 1);
        @Cleanup
        ConditionalOutputStream cOut = factory.createConditionalOutputStream(segment,
                DelegationTokenProviderFactory.create("token", controller, segment, AccessOperation.ANY),
                EventWriterConfig.builder().build());
        ByteBuffer data = ByteBuffer.allocate(10);

        String mockClientReplyStackTrace = "SomeException";
        ClientConnection mock = Mockito.mock(ClientConnection.class);
        PravegaNodeUri location = new PravegaNodeUri("localhost", 0);
        connectionFactory.provideConnection(location, mock);
        setupAppend(connectionFactory, segment, mock, location);
        Mockito.doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                ConditionalAppend argument = (ConditionalAppend) invocation.getArgument(0);
                ReplyProcessor processor = connectionFactory.getProcessor(location);
                processor.process(new WireCommands.SegmentIsSealed(argument.getRequestId(), segment.getScopedName(), mockClientReplyStackTrace,
                                                                   argument.getEventNumber()));
                return null;
            }
        }).when(mock).send(any(ConditionalAppend.class));
        AssertExtensions.assertThrows(SegmentSealedException.class, () -> cOut.write(data, 0));
    }

    @SneakyThrows
    @Test(timeout = 10000)
    public void testRetriesOnTokenExpiry() {
        @Cleanup
        MockConnectionFactoryImpl connectionFactory = new MockConnectionFactoryImpl();
        @Cleanup
        MockController controller = new MockController("localhost", 0, connectionFactory, true);
        ConditionalOutputStreamFactory factory = new ConditionalOutputStreamFactoryImpl(controller, connectionFactory);
        Segment segment = new Segment("scope", "testWrite", 1);
        @Cleanup
        ConditionalOutputStream objectUnderTest = factory.createConditionalOutputStream(segment,
                DelegationTokenProviderFactory.create("token", controller, segment, AccessOperation.ANY),
                EventWriterConfig.builder().build());
        ByteBuffer data = ByteBuffer.allocate(10);

        ClientConnection clientConnection = Mockito.mock(ClientConnection.class);
        PravegaNodeUri location = new PravegaNodeUri("localhost", 0);
        connectionFactory.provideConnection(location, clientConnection);
        setupAppend(connectionFactory, segment, clientConnection, location);

        final AtomicLong retryCounter = new AtomicLong(0);
        Mockito.doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                ConditionalAppend argument = (ConditionalAppend) invocation.getArgument(0);
                ReplyProcessor processor = connectionFactory.getProcessor(location);

                if (retryCounter.getAndIncrement() < 2) {
                    processor.process(new WireCommands.AuthTokenCheckFailed(argument.getRequestId(), "SomeException",
                            WireCommands.AuthTokenCheckFailed.ErrorCode.TOKEN_EXPIRED));
                } else {
                    processor.process(new WireCommands.DataAppended(argument.getRequestId(),
                            argument.getWriterId(), argument.getEventNumber(), 0, -1));
                }
                return null;
            }
        }).when(clientConnection).send(any(ConditionalAppend.class));
        assertTrue(objectUnderTest.write(data, 0));
        assertEquals(3, retryCounter.get());
    }
    
    @SneakyThrows
    @Test(timeout = 10000)
    public void testRetriesOnInvalidEventNumber() {
        @Cleanup
        MockConnectionFactoryImpl connectionFactory = new MockConnectionFactoryImpl();
        @Cleanup
        MockController controller = new MockController("localhost", 0, connectionFactory, true);
        ConditionalOutputStreamFactory factory = new ConditionalOutputStreamFactoryImpl(controller, connectionFactory);
        Segment segment = new Segment("scope", "testWrite", 1);
        @Cleanup
        ConditionalOutputStream objectUnderTest = factory.createConditionalOutputStream(segment,
                DelegationTokenProviderFactory.create("token", controller, segment, AccessOperation.ANY),
                EventWriterConfig.builder().build());
        ByteBuffer data = ByteBuffer.allocate(10);

        ClientConnection clientConnection = Mockito.mock(ClientConnection.class);
        PravegaNodeUri location = new PravegaNodeUri("localhost", 0);
        connectionFactory.provideConnection(location, clientConnection);
        setupAppend(connectionFactory, segment, clientConnection, location);

        final AtomicLong retryCounter = new AtomicLong(0);
        Mockito.doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                ConditionalAppend argument = (ConditionalAppend) invocation.getArgument(0);
                ReplyProcessor processor = connectionFactory.getProcessor(location);

                if (retryCounter.getAndIncrement() < 2) {
                    processor.process(new WireCommands.InvalidEventNumber(argument.getWriterId(), argument.getRequestId(), "", argument.getEventNumber()));
                } else {
                    processor.process(new WireCommands.DataAppended(argument.getRequestId(),
                            argument.getWriterId(), argument.getEventNumber(), 0, -1));
                }
                return null;
            }
        }).when(clientConnection).send(any(ConditionalAppend.class));
        assertTrue(objectUnderTest.write(data, 0));
        assertEquals(3, retryCounter.get());
    }

    @Test(timeout = 10000)
    public void testNonExpiryTokenCheckFailure() throws ConnectionFailedException {
        @Cleanup
        MockConnectionFactoryImpl connectionFactory = new MockConnectionFactoryImpl();
        @Cleanup
        MockController controller = new MockController("localhost", 0, connectionFactory, true);
        ConditionalOutputStreamFactory factory = new ConditionalOutputStreamFactoryImpl(controller, connectionFactory);
        Segment segment = new Segment("scope", "testWrite", 1);
        @Cleanup
        ConditionalOutputStream objectUnderTest = factory.createConditionalOutputStream(segment,
                DelegationTokenProviderFactory.create("token", controller, segment, AccessOperation.ANY),
                EventWriterConfig.builder().build());
        ByteBuffer data = ByteBuffer.allocate(10);

        ClientConnection clientConnection = Mockito.mock(ClientConnection.class);
        PravegaNodeUri location = new PravegaNodeUri("localhost", 0);
        connectionFactory.provideConnection(location, clientConnection);
        setupAppend(connectionFactory, segment, clientConnection, location);

        Mockito.doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                ConditionalAppend argument = (ConditionalAppend) invocation.getArgument(0);
                ReplyProcessor processor = connectionFactory.getProcessor(location);
                processor.process(new WireCommands.AuthTokenCheckFailed(argument.getRequestId(), "SomeException",
                                WireCommands.AuthTokenCheckFailed.ErrorCode.TOKEN_CHECK_FAILED));
                return null;
            }
        }).when(clientConnection).send(any(ConditionalAppend.class));
        AssertExtensions.assertThrows(AuthenticationException.class, () -> objectUnderTest.write(data, 0));
    }

    @Test
    public void handleUnexpectedReplythrowsAppropriateTokenExceptions() {
        @Cleanup
        MockConnectionFactoryImpl connectionFactory = new MockConnectionFactoryImpl();
        @Cleanup
        MockController controller = new MockController("localhost", 0, connectionFactory, true);
        ConditionalOutputStreamFactory factory = new ConditionalOutputStreamFactoryImpl(controller, connectionFactory);
        Segment segment = new Segment("scope", "testWrite", 1);
        @Cleanup
        ConditionalOutputStreamImpl objectUnderTest = (ConditionalOutputStreamImpl) factory.createConditionalOutputStream(segment,
                DelegationTokenProviderFactory.create("token", controller, segment, AccessOperation.ANY),
                EventWriterConfig.builder().build());

        AssertExtensions.assertThrows("AuthenticationException wasn't thrown",
                () ->  objectUnderTest.handleUnexpectedReply(new WireCommands.AuthTokenCheckFailed(1L, "SomeException",
                        WireCommands.AuthTokenCheckFailed.ErrorCode.TOKEN_CHECK_FAILED), "test"),
                e -> e instanceof AuthenticationException);

        AssertExtensions.assertThrows("AuthenticationException wasn't thrown",
                () ->  objectUnderTest.handleUnexpectedReply(new WireCommands.AuthTokenCheckFailed(1L, "SomeException",
                        WireCommands.AuthTokenCheckFailed.ErrorCode.UNSPECIFIED), "test"),
                e -> e instanceof AuthenticationException);

        AssertExtensions.assertThrows("TokenExpiredException wasn't thrown",
                () ->  objectUnderTest.handleUnexpectedReply(new WireCommands.AuthTokenCheckFailed(1L, "SomeException",
                        WireCommands.AuthTokenCheckFailed.ErrorCode.TOKEN_EXPIRED), "test"),
                e -> e instanceof TokenExpiredException);
        
        AssertExtensions.assertThrows("InvalidEventNumber wasn't treated as a connection failure",
                () ->  objectUnderTest.handleUnexpectedReply(new WireCommands.InvalidEventNumber(UUID.randomUUID(), 1, "SomeException", 1L), "test"),
                e -> e instanceof ConnectionFailedException);
        
        AssertExtensions.assertThrows("Hello wasn't treated as a connection failure",
                                      () ->  objectUnderTest.handleUnexpectedReply(new WireCommands.Hello(1, 1), "test"),
                                      e -> e instanceof ConnectionFailedException);
    }
    
    /**
     * It is necessary to only have one outstanding conditional append per writerId to make sure the status can be resolved in the event of a reconnect.
     */
    @Test(timeout = 10000)
    public void testOnlyOneWriteAtATime() throws ConnectionFailedException, SegmentSealedException {
        @Cleanup
        MockConnectionFactoryImpl connectionFactory = new MockConnectionFactoryImpl();
        @Cleanup
        MockController controller = new MockController("localhost", 0, connectionFactory, true);
        ConditionalOutputStreamFactory factory = new ConditionalOutputStreamFactoryImpl(controller, connectionFactory);
        Segment segment = new Segment("scope", "testWrite", 1);
        @Cleanup
        ConditionalOutputStream cOut = factory.createConditionalOutputStream(segment,
                DelegationTokenProviderFactory.create("token", controller, segment, AccessOperation.ANY), EventWriterConfig.builder().build());
        ByteBuffer data = ByteBuffer.allocate(10);
        ClientConnection mock = Mockito.mock(ClientConnection.class);
        PravegaNodeUri location = new PravegaNodeUri("localhost", 0);
        connectionFactory.provideConnection(location, mock);
        setupAppend(connectionFactory, segment, mock, location);
        LinkedBlockingQueue<Boolean> replies = new LinkedBlockingQueue<>();
        Mockito.doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                ConditionalAppend argument = (ConditionalAppend) invocation.getArgument(0);
                ReplyProcessor processor = connectionFactory.getProcessor(location);
                if (replies.take()) {                    
                    processor.process(new WireCommands.DataAppended(argument.getRequestId(), argument.getWriterId(), argument.getEventNumber(), 0, -1));
                } else {
                    processor.process(new WireCommands.ConditionalCheckFailed(argument.getWriterId(), argument.getEventNumber(),
                                                                              argument.getRequestId()));
                }
                return null;
            }
        }).when(mock).send(any(ConditionalAppend.class));
        replies.add(true);
        replies.add(false);
        assertTrue(cOut.write(data, 0));
        assertFalse(cOut.write(data, 1));
        AssertExtensions.assertBlocks(() -> {
            assertTrue(cOut.write(data, 2));
        }, () -> {
            replies.add(true);
        });
        AssertExtensions.assertBlocks(() -> {
            assertFalse(cOut.write(data, 3));
        }, () -> {
            replies.add(false);
        });
        AssertExtensions.assertBlocks(() -> {
            assertTrue(cOut.write(data, 4));
        }, () -> {
            AssertExtensions.assertBlocks(() -> {
                assertFalse(cOut.write(data, 5));
            }, () -> {
                replies.add(true);
                replies.add(false);
            });
        });
        AssertExtensions.assertBlocks(() -> {
            assertFalse(cOut.write(data, 6));
        }, () -> {
            AssertExtensions.assertBlocks(() -> {
                assertTrue(cOut.write(data, 7));
            }, () -> {
                replies.add(false);
                replies.add(true);
            });
        });
    }
    
}
