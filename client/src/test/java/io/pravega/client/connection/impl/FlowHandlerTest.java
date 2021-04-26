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
package io.pravega.client.connection.impl;


import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelPromise;
import io.pravega.client.ClientConfig;
import io.pravega.client.connection.impl.FlowHandler.KeepAliveTask;
import io.pravega.common.ObjectClosedException;
import io.pravega.shared.metrics.ClientMetricKeys;
import io.pravega.shared.metrics.MetricNotifier;
import io.pravega.shared.protocol.netty.Append;
import io.pravega.shared.protocol.netty.ConnectionFailedException;
import io.pravega.shared.protocol.netty.FailingReplyProcessor;
import io.pravega.shared.protocol.netty.PravegaNodeUri;
import io.pravega.shared.protocol.netty.Reply;
import io.pravega.shared.protocol.netty.ReplyProcessor;
import io.pravega.shared.protocol.netty.WireCommands;
import io.pravega.shared.protocol.netty.WireCommands.KeepAlive;
import io.pravega.test.common.InlineExecutor;
import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.Cleanup;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.junit.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;

import static io.pravega.test.common.AssertExtensions.assertThrows;
import static java.lang.String.valueOf;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class FlowHandlerTest {

    @Rule
    public Timeout globalTimeout = Timeout.seconds(15);

    private Flow flow;
    private FlowHandler flowHandler;
    @Mock
    private ReplyProcessor processor;
    @Mock
    private Append appendCmd;
    @Mock
    private ByteBuf buffer;
    @Mock
    private ConnectionFactory connectionFactory;
    @Mock
    private ClientConnection connection;
    private ScheduledExecutorService loop;
    @Mock
    private ChannelFuture completedFuture;
    @Mock
    private ChannelPromise promise;

    @BeforeClass
    public static void beforeClass() {
        System.setProperty("pravega.client.netty.channel.timeout.millis", valueOf(SECONDS.toMillis(5)));
    }

    @Before
    public void setUp() throws Exception {
        flow = new Flow(10, 0);
        loop = new InlineExecutor();
        appendCmd = new Append("segment0", UUID.randomUUID(), 2, 1, buffer, 10L, flow.asLong());

        when(connectionFactory.establishConnection(any(PravegaNodeUri.class), any(ReplyProcessor.class)))
            .thenReturn(CompletableFuture.completedFuture(connection));
        when(connectionFactory.getInternalExecutor()).thenReturn(loop);
        
        ClientConfig clientConfig = ClientConfig.builder().build();
        PravegaNodeUri pravegaNodeUri = new PravegaNodeUri("testConnection", 0);
        flowHandler = FlowHandler.openConnection(pravegaNodeUri, new TestMetricNotifier(), connectionFactory).join();
    }
    
    @After 
    public void tearDown() {
        loop.shutdown();
    }

    @Test
    public void sendNormal() throws Exception {
        @Cleanup
        ClientConnection clientConnection = flowHandler.createFlow(flow, processor);
        clientConnection.send(appendCmd);
    }

    @Test(expected = ConnectionFailedException.class)
    public void sendError() throws Exception {
        @Cleanup
        ClientConnection clientConnection = flowHandler.createFlow(flow, processor);
        Mockito.doThrow(new ConnectionFailedException()).when(connection).send(appendCmd);
        clientConnection.send(appendCmd);
    }

    @Test
    public void sendErrorUnRegistered() throws Exception {
        AtomicBoolean dropped = new AtomicBoolean(false);
        ReplyProcessor rp = new FailingReplyProcessor() {
            @Override
            public void processingFailure(Exception error) {
                //nothing
            }
            
            @Override
            public void connectionDropped() {
                dropped.set(true);
            }
        };
        @Cleanup
        ClientConnection clientConnection = flowHandler.createFlow(flow, rp);
        //any send after channelUnregistered should throw a ConnectionFailedException.
        flowHandler.connectionDropped();
        assertTrue(dropped.get());
    }

    @Test(expected = IllegalArgumentException.class)
    public void createDuplicateSession() throws Exception {
        Flow flow = new Flow(10, 0);
        ClientConnection connection1 = flowHandler.createFlow(flow, processor);
        connection1.send(appendCmd);
        // Creating a flow with the same flow id.
        flowHandler.createFlow(flow, processor);
    }

    @Test
    public void testCloseSession() throws Exception {
        @Cleanup
        ClientConnection clientConnection = flowHandler.createFlow(flow, processor);
        clientConnection.send(appendCmd);
        clientConnection.close();
        assertEquals(0, flowHandler.getFlowIdReplyProcessorMap().size());
    }

    @Test
    public void testCloseSessionHandler() throws Exception {
        @Cleanup
        ClientConnection clientConnection = flowHandler.createFlow(flow, processor);
        WireCommands.GetSegmentAttribute cmd = new WireCommands.GetSegmentAttribute(flow.asLong(), "seg", UUID.randomUUID(), "");
        clientConnection.send(cmd);
        flowHandler.close();
        // verify that the Channel.close is invoked.
        Mockito.verify(connection, times(1)).close();
        assertThrows(ObjectClosedException.class, () -> flowHandler.createFlow(flow, processor));
        assertThrows(ObjectClosedException.class, () -> flowHandler.createConnectionWithFlowDisabled(processor));
    }

    @Test
    public void testCreateConnectionWithSessionDisabled() throws Exception {
        flow = new Flow(0, 10);
        ClientConnection connection = flowHandler.createConnectionWithFlowDisabled(processor);
        connection.send(new Append("segment0", UUID.randomUUID(), 2, 1, buffer, 10L, flow.asLong()));
        assertThrows(IllegalStateException.class, () -> flowHandler.createFlow(flow, processor));
    }

    @Test
    public void testChannelUnregistered() throws Exception {
        @Cleanup
        ClientConnection clientConnection = flowHandler.createFlow(flow, processor);
        clientConnection.send(appendCmd);
        //simulate a connection dropped
        flowHandler.connectionDropped();
        assertTrue(flowHandler.isClosed());
    }

    @Test
    public void testHelloPassed() throws Exception {
        @Cleanup
        ClientConnection clientConnection = flowHandler.createFlow(flow, processor);
        InOrder order = inOrder(connection);
        order.verify(connection).send(any(WireCommands.Hello.class));
        order.verifyNoMoreInteractions();
    }

    @Test
    public void testChannelReadDataAppended() throws Exception {
        @Cleanup
        ClientConnection clientConnection = flowHandler.createFlow(flow, processor);
        WireCommands.DataAppended dataAppendedCmd = new WireCommands.DataAppended(flow.asLong(), UUID.randomUUID(), 2, 1, 0);
        InOrder order = inOrder(processor);
        flowHandler.process(dataAppendedCmd);
        order.verify(processor, times(1)).process(dataAppendedCmd);
    }

    @Test
    public void testHelloWithErrorReplyProcessor() throws Exception {
        ReplyProcessor errorProcessor = mock(ReplyProcessor.class);
        @Cleanup
        ClientConnection connection1 = flowHandler.createFlow(new Flow(11, 0), errorProcessor);
        @Cleanup
        ClientConnection connection2 = flowHandler.createFlow(flow, processor);
        doAnswer((Answer<Void>) invocation -> {
            throw new RuntimeException("Reply processor error");
        }).when(errorProcessor).hello(any(WireCommands.Hello.class));

        final WireCommands.Hello msg = new WireCommands.Hello(5, 4);
        flowHandler.process(msg);
        verify(processor).hello(msg);
        verify(errorProcessor).hello(msg);
    }

    @Test
    public void testKeepAlive() {
        final WireCommands.KeepAlive msg = new WireCommands.KeepAlive();
        flowHandler.process(msg);
        // ensure none of the Replyprocessors are bothered with this msg.
        verifyNoInteractions(processor);
    }

    @Test
    public void testProcessWithErrorReplyProcessor() throws Exception {
        @Cleanup
        ClientConnection connection = flowHandler.createFlow(flow, processor);
        doAnswer((Answer<Void>) invocation -> {
            throw new RuntimeException("ReplyProcessorError");
        }).when(processor).process(any(Reply.class));
 
        WireCommands.DataAppended msg = new WireCommands.DataAppended(flow.asLong(), UUID.randomUUID(), 2, 1, 0);
        flowHandler.process(msg);
        verify(processor).process(msg);
        verify(processor).processingFailure(any(RuntimeException.class));
    }

    @Test
    public void testExceptionCaughtWithErrorReplyProcessor() throws Exception {
        ReplyProcessor errorProcessor = mock(ReplyProcessor.class);
        @Cleanup
        ClientConnection connection1 = flowHandler.createFlow(new Flow(11, 0), errorProcessor);
        @Cleanup
        ClientConnection connection2 = flowHandler.createFlow(flow, processor);
        doAnswer((Answer<Void>) invocation -> {
            throw new RuntimeException("Reply processor error");
        }).when(errorProcessor).processingFailure(any(ConnectionFailedException.class));

        flowHandler.processingFailure(new IOException("netty error"));
        verify(processor).processingFailure(any(ConnectionFailedException.class));
        verify(errorProcessor).processingFailure(any(ConnectionFailedException.class));
    }

    @Test
    public void testChannelUnregisteredWithErrorReplyProcessor() throws Exception {
        ReplyProcessor errorProcessor = mock(ReplyProcessor.class);
        @Cleanup
        ClientConnection connection1 = flowHandler.createFlow(new Flow(11, 0), errorProcessor);
        @Cleanup
        ClientConnection connection2 = flowHandler.createFlow(flow, processor);
        doAnswer((Answer<Void>) invocation -> {
            throw new RuntimeException("Reply processor error");
        }).when(errorProcessor).connectionDropped();

        flowHandler.connectionDropped();
        verify(processor).connectionDropped();
        verify(errorProcessor).connectionDropped();
    }

    @Test
    public void keepAliveFailureTest() throws Exception {
        ReplyProcessor replyProcessor = mock(ReplyProcessor.class);
        @Cleanup
        ClientConnection connection1 = flowHandler.createFlow(flow, processor);
        @Cleanup
        ClientConnection connection2 = flowHandler.createFlow(new Flow(11, 0), replyProcessor);
        KeepAliveTask keepAlive = flowHandler.getKeepAliveTask();
        keepAlive.run();

        // simulate a KeepAlive connection failure.
        keepAlive.handleError(new RuntimeException("Induced error"));

        // ensure all the reply processors are informed immediately of the channel being closed due to KeepAlive Failure.
        verify(processor).connectionDropped();
        verify(replyProcessor).connectionDropped();
    }
    
    @Test
    public void keepAliveWriteFailureTest() throws Exception {
        ReplyProcessor replyProcessor = mock(ReplyProcessor.class);
        @Cleanup
        ClientConnection connection1 = flowHandler.createFlow(flow, processor);
        @Cleanup
        ClientConnection connection2 = flowHandler.createFlow(new Flow(11, 0), replyProcessor);

        KeepAliveTask keepAlive = flowHandler.getKeepAliveTask();
        Mockito.doThrow(new RuntimeException("Induced error")).when(connection).send(any(KeepAlive.class));
        keepAlive.run();

        // ensure all the reply processors are informed immediately of the channel being closed due to KeepAlive Failure.
        verify(processor).connectionDropped();
        verify(replyProcessor).connectionDropped();
    }
    
    @Test
    public void keepAliveTimeoutTest() throws Exception {
        ReplyProcessor replyProcessor = mock(ReplyProcessor.class);
        @Cleanup
        ClientConnection connection1 = flowHandler.createFlow(flow, processor);
        @Cleanup
        ClientConnection connection2 = flowHandler.createFlow(new Flow(11, 0), replyProcessor);
        KeepAliveTask keepAlive = flowHandler.getKeepAliveTask();
        Mockito.doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                keepAlive.run();  //Triggers a timeout.
                return null;
            }
        }).when(connection).send(any(KeepAlive.class));
        keepAlive.run();
        
        // ensure all the reply processors are informed immediately of the channel being closed due to KeepAlive Failure.
        verify(processor).connectionDropped();
        verify(replyProcessor).connectionDropped();
    }

    
    /**
     * Added a mock MetricNotifier different from the default one to exercise reporting metrics from client side.
     */
    static class TestMetricNotifier implements MetricNotifier {
        @Override
        public void updateSuccessMetric(ClientMetricKeys metricKey, String[] metricTags, long value) {
            NO_OP_METRIC_NOTIFIER.updateSuccessMetric(metricKey, metricTags, value);
            assertNotNull(metricKey);
        }

        @Override
        public void updateFailureMetric(ClientMetricKeys metricKey, String[] metricTags, long value) {
            NO_OP_METRIC_NOTIFIER.updateFailureMetric(metricKey, metricTags, value);
            assertNotNull(metricKey);
        }

        @Override
        public void close() {
            NO_OP_METRIC_NOTIFIER.close();
        }
    }
}
