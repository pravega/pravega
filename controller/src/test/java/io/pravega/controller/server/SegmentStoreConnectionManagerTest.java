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
package io.pravega.controller.server;

import io.pravega.client.connection.impl.ClientConnection;
import io.pravega.client.connection.impl.ConnectionFactory;
import io.pravega.common.Exceptions;
import io.pravega.controller.server.SegmentStoreConnectionManager.ConnectionWrapper;
import io.pravega.controller.server.SegmentStoreConnectionManager.ReusableReplyProcessor;
import io.pravega.controller.server.SegmentStoreConnectionManager.SegmentStoreConnectionPool;
import io.pravega.shared.protocol.netty.Append;
import io.pravega.shared.protocol.netty.ConnectionFailedException;
import io.pravega.shared.protocol.netty.PravegaNodeUri;
import io.pravega.shared.protocol.netty.ReplyProcessor;
import io.pravega.shared.protocol.netty.WireCommand;
import io.pravega.shared.protocol.netty.WireCommands;
import io.pravega.test.common.AssertExtensions;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.Getter;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class SegmentStoreConnectionManagerTest {
    private AtomicInteger replyProcCounter;
    private AtomicInteger connectionCounter;

    @Before
    public void setUp() {
        replyProcCounter = new AtomicInteger();
        connectionCounter = new AtomicInteger();
    }

    @Test(timeout = 30000)
    public void connectionTest() {
        PravegaNodeUri uri = new PravegaNodeUri("pravega", 1234);
        ConnectionFactory cf = spy(new MockConnectionFactory());

        SegmentStoreConnectionPool pool = new SegmentStoreConnectionPool(uri, cf, 2, 1);
        ReplyProcessor myReplyProc = getReplyProcessor();

        // we should be able to establish two connections safely
        ConnectionWrapper connection1 = pool.getConnection(myReplyProc).join();
        // verify that the connection returned is of type MockConnection
        assertTrue(connection1.getConnection() instanceof MockConnection);
        assertTrue(((MockConnection) connection1.getConnection()).getRp() instanceof ReusableReplyProcessor);
        assertEquals(connection1.getReplyProcessor(), myReplyProc);
        verify(cf, times(1)).establishConnection(any(), any());

        ReplyProcessor myReplyProc2 = getReplyProcessor();

        ConnectionWrapper connection2 = pool.getConnection(myReplyProc2).join();
        assertEquals(connection2.getReplyProcessor(), myReplyProc2);
        verify(cf, times(2)).establishConnection(any(), any());

        // return these connections
        connection1.close();
        connection2.close();
 
        // idempotent connection close
        connection1.close();
        connection2.close();
        
        // verify that connections are reset 
        assertNull(connection1.getReplyProcessor());
        assertNull(connection2.getReplyProcessor());

        // verify that one connection was closed
        connection2.getConnection().close();

        assertTrue(((MockConnection) connection2.getConnection()).isClosed.get());

        // now create two more connections
        // 1st should be delivered from available connections.
        connection1 = pool.getConnection(getReplyProcessor()).join();
        // we should get back first connection
        assertEquals(((MockConnection) connection1.getConnection()).uniqueId, 1);

        // 2nd request should result in creation of new connection
        connection2 = pool.getConnection(getReplyProcessor()).join();
        assertEquals(((MockConnection) connection2.getConnection()).uniqueId, 3);

        // attempt to create a third connection
        CompletableFuture<ConnectionWrapper> connection3Future = pool.getConnection(getReplyProcessor());
        // this would not have completed. the waiting queue should have this entry
        assertFalse(connection3Future.isDone());

        CompletableFuture<ConnectionWrapper> connection4Future = pool.getConnection(myReplyProc);
        assertFalse(connection4Future.isDone());

        // return connection1. it should be assigned to first waiting connection (connection3)
        connection1.close();
        ConnectionWrapper connection3 = connection3Future.join();
        // verify that connection 3 received a connection object
        assertEquals(((MockConnection) connection3.getConnection()).uniqueId, 1);

        // now fail connection 2 and return it.
        connection2.failConnection();
        connection2.close();
        assertTrue(((MockConnection) connection2.getConnection()).isClosed.get());

        // this should not be given to the waiting request. instead a new connection should be established. 
        ConnectionWrapper connection4 = connection4Future.join();
        assertEquals(((MockConnection) connection4.getConnection()).uniqueId, 4);

        // create another waiting request
        CompletableFuture<ConnectionWrapper> connection5Future = pool.getConnection(myReplyProc);

        // test shutdown
        pool.shutdown();
        connection3.close();
        assertFalse(((MockConnection) connection3.getConnection()).isClosed.get());

        // connection 5 should have been returned by using connection3
        ConnectionWrapper connection5 = connection5Future.join();
        // since returned connection served the waiting request no new connection should have been established
        assertEquals(((MockConnection) connection5.getConnection()).uniqueId, 1);

        // return connection 4.. this should be closed as there is no one waiting
        connection4.close();
        assertTrue(((MockConnection) connection4.getConnection()).isClosed.get());

        // we should still be able to request new connections.. request connection 6.. this should be served immediately 
        // by way of new connection
        ConnectionWrapper connection6 = pool.getConnection(myReplyProc).join();
        assertEquals(((MockConnection) connection6.getConnection()).uniqueId, 5);

        // request connect 7. this should wait as connection could is 2. 
        CompletableFuture<ConnectionWrapper> connection7Future = pool.getConnection(myReplyProc);
        assertFalse(connection7Future.isDone());

        // return connection 5.. connection7 should get connection5's object and no new connection should be established
        connection5.close();
        ConnectionWrapper connection7 = connection7Future.join();
        assertEquals(((MockConnection) connection7.getConnection()).uniqueId, 1);

        // return connection 6 and 7. they should be closed. 
        connection6.close();
        assertTrue(((MockConnection) connection6.getConnection()).isClosed.get());

        connection7.close();
        assertTrue(((MockConnection) connection7.getConnection()).isClosed.get());

        // create connection 8
        // close the connection explicitly
        ConnectionWrapper connection8 = pool.getConnection(myReplyProc).join();
        assertEquals(((MockConnection) connection8.getConnection()).uniqueId, 6);
        connection8.getConnection().close();

        CompletableFuture<Void> future = new CompletableFuture<>();
        connection8.sendAsync(new WireCommands.Hello(0, 0), future);
        AssertExtensions.assertFutureThrows("Connection should fail",
                future, e -> {
                    Throwable unwrap = Exceptions.unwrap(e);
                    return unwrap instanceof WireCommandFailedException &&
                            ((WireCommandFailedException) unwrap).getReason().equals(WireCommandFailedException.Reason.ConnectionFailed);
                });
    }

    private ReplyProcessor getReplyProcessor() {
        int uniqueId = replyProcCounter.incrementAndGet();
        return new ReplyProcessor() {
            @Override
            public void hello(WireCommands.Hello hello) {

            }

            @Override
            public void wrongHost(WireCommands.WrongHost wrongHost) {

            }

            @Override
            public void segmentAlreadyExists(WireCommands.SegmentAlreadyExists segmentAlreadyExists) {

            }

            @Override
            public void segmentIsSealed(WireCommands.SegmentIsSealed segmentIsSealed) {

            }

            @Override
            public void segmentIsTruncated(WireCommands.SegmentIsTruncated segmentIsTruncated) {

            }

            @Override
            public void noSuchSegment(WireCommands.NoSuchSegment noSuchSegment) {

            }

            @Override
            public void tableSegmentNotEmpty(WireCommands.TableSegmentNotEmpty tableSegmentNotEmpty) {

            }

            @Override
            public void invalidEventNumber(WireCommands.InvalidEventNumber invalidEventNumber) {

            }

            @Override
            public void appendSetup(WireCommands.AppendSetup appendSetup) {

            }

            @Override
            public void dataAppended(WireCommands.DataAppended dataAppended) {

            }

            @Override
            public void conditionalCheckFailed(WireCommands.ConditionalCheckFailed dataNotAppended) {

            }

            @Override
            public void segmentRead(WireCommands.SegmentRead segmentRead) {

            }

            @Override
            public void segmentAttributeUpdated(WireCommands.SegmentAttributeUpdated segmentAttributeUpdated) {

            }

            @Override
            public void segmentAttribute(WireCommands.SegmentAttribute segmentAttribute) {

            }

            @Override
            public void streamSegmentInfo(WireCommands.StreamSegmentInfo streamInfo) {

            }

            @Override
            public void segmentCreated(WireCommands.SegmentCreated segmentCreated) {

            }

            @Override
            public void segmentsMerged(WireCommands.SegmentsMerged segmentsMerged) {

            }

            @Override
            public void segmentSealed(WireCommands.SegmentSealed segmentSealed) {

            }

            @Override
            public void segmentTruncated(WireCommands.SegmentTruncated segmentTruncated) {

            }

            @Override
            public void segmentDeleted(WireCommands.SegmentDeleted segmentDeleted) {

            }

            @Override
            public void operationUnsupported(WireCommands.OperationUnsupported operationUnsupported) {

            }

            @Override
            public void keepAlive(WireCommands.KeepAlive keepAlive) {

            }

            @Override
            public void connectionDropped() {

            }

            @Override
            public void segmentPolicyUpdated(WireCommands.SegmentPolicyUpdated segmentPolicyUpdated) {

            }

            @Override
            public void processingFailure(Exception error) {

            }

            @Override
            public void authTokenCheckFailed(WireCommands.AuthTokenCheckFailed authTokenCheckFailed) {

            }

            @Override
            public void tableEntriesUpdated(WireCommands.TableEntriesUpdated tableEntriesUpdated) {

            }

            @Override
            public void tableKeysRemoved(WireCommands.TableKeysRemoved tableKeysRemoved) {

            }

            @Override
            public void tableRead(WireCommands.TableRead tableRead) {

            }

            @Override
            public void tableKeyDoesNotExist(WireCommands.TableKeyDoesNotExist tableKeyDoesNotExist) {

            }

            @Override
            public void tableKeyBadVersion(WireCommands.TableKeyBadVersion tableKeyBadVersion) {

            }

            @Override
            public void tableKeysRead(WireCommands.TableKeysRead tableKeysRead) {

            }

            @Override
            public void tableEntriesRead(WireCommands.TableEntriesRead tableEntriesRead) {

            }

            @Override
            public void tableEntriesDeltaRead(WireCommands.TableEntriesDeltaRead tableEntriesDeltaRead) {

            }

            @Override
            public void errorMessage(WireCommands.ErrorMessage errorMessage) {

            }
        };
    }

    private class MockConnectionFactory implements ConnectionFactory {
        @Getter
        private ReplyProcessor rp;

        @Override
        public CompletableFuture<ClientConnection> establishConnection(PravegaNodeUri endpoint, ReplyProcessor rp) {
            this.rp = rp;
            ClientConnection connection = new MockConnection(rp);
            return CompletableFuture.completedFuture(connection);
        }

        @Override
        public ScheduledExecutorService getInternalExecutor() {
            return null;
        }

        @Override
        public void close() {

        }
    }

    private class MockConnection implements ClientConnection {
        int uniqueId = connectionCounter.incrementAndGet();

        @Getter
        private final ReplyProcessor rp;
        @Getter
        private AtomicBoolean isClosed = new AtomicBoolean(false);

        public MockConnection(ReplyProcessor rp) {
            this.rp = rp;
        }

        @Override
        public void send(WireCommand cmd) throws ConnectionFailedException {
            if (isClosed.get()) {
                throw new ConnectionFailedException();
            }
        }

        @Override
        public void send(Append append) throws ConnectionFailedException {

        }


        @Override
        public void sendAsync(List<Append> appends, CompletedCallback callback) {

        }

        @Override
        public void close() {
            isClosed.set(true);
        }
    }
}
