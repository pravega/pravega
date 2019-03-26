/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.server;


import io.pravega.client.netty.impl.ClientConnection;
import io.pravega.client.netty.impl.ConnectionFactory;
import io.pravega.shared.protocol.netty.Append;
import io.pravega.shared.protocol.netty.ConnectionFailedException;
import io.pravega.shared.protocol.netty.PravegaNodeUri;
import io.pravega.shared.protocol.netty.ReplyProcessor;
import io.pravega.shared.protocol.netty.WireCommand;
import io.pravega.shared.protocol.netty.WireCommands;
import lombok.Getter;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;

import static io.pravega.controller.server.SegmentStoreConnectionManager.SegmentStoreConnectionPool;
import static io.pravega.controller.server.SegmentStoreConnectionManager.ConnectionObject;
import static io.pravega.controller.server.SegmentStoreConnectionManager.ConnectionListener.ConnectionEvent;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

public class SegmentStoreConnectionTest {
    @Test(timeout = 30000)
    public void connectionTest() throws InterruptedException {
        PravegaNodeUri uri = new PravegaNodeUri("pravega", 1234);
        ConnectionFactory cf = new MockConnectionFactory();
        LinkedBlockingQueue<ConnectionEvent> eventQueue = new LinkedBlockingQueue<>();
        SegmentStoreConnectionPool pool = 
                new SegmentStoreConnectionPool(uri, cf, 2, 1, eventQueue::offer);
        ReplyProcessor myReplyProc = getReplyProcessor();
        
        // we should be able to establish two connections safely
        ConnectionObject connection1 = pool.getConnection(myReplyProc).join();
        assertEquals(ConnectionEvent.NewConnection, eventQueue.take());
        assertEquals(pool.connectionCount(), 1);

        ConnectionObject connection2 = pool.getConnection(myReplyProc).join();
        assertEquals(ConnectionEvent.NewConnection, eventQueue.take());
        assertEquals(pool.connectionCount(), 2);
        
        // return these connections
        pool.returnConnection(connection1);
        // verify that available connections is 1
        assertEquals(pool.availableCount(), 1);
        assertEquals(pool.connectionCount(), 2);

        pool.returnConnection(connection2);
        // connection manager should only have one connection as available connection. 
        // it should have destroyed the second connection.
        // verify that available connections is still 1
        assertEquals(pool.availableCount(), 1);
        assertEquals(pool.connectionCount(), 1);

        // verify that one connection was closed
        assertEquals(ConnectionEvent.ConnectionClosed, eventQueue.take());
        assertTrue(eventQueue.isEmpty());
        
        // now create two more connections
        // 1st should be delivered from available connections. 
        connection1 = pool.getConnection(myReplyProc).join();
        // verify its delivered from available connections 
        assertEquals(pool.connectionCount(), 1);
        assertEquals(pool.availableCount(), 0);
        // verify that no new connection was established
        assertTrue(eventQueue.isEmpty());

        // 2nd request should result in creation of new connection
        connection2 = pool.getConnection(myReplyProc).join();
        assertEquals(ConnectionEvent.NewConnection, eventQueue.take());
        assertEquals(pool.availableCount(), 0);
        // verify that there are two created connections
        assertEquals(pool.connectionCount(), 2);

        // attempt to create a third connection
        CompletableFuture<ConnectionObject> connection3Future = pool.getConnection(myReplyProc);
        // this would not have completed. the waiting queue should have this entry
        assertEquals(pool.connectionCount(), 2);
        assertEquals(pool.waitingCount(), 1);
        assertEquals(pool.availableCount(), 0);
        assertFalse(connection3Future.isDone());
        assertTrue(eventQueue.isEmpty());

        CompletableFuture<ConnectionObject> connection4Future = pool.getConnection(myReplyProc);
        assertEquals(pool.connectionCount(), 2);
        assertEquals(pool.waitingCount(), 2);
        assertEquals(pool.availableCount(), 0);
        assertTrue(eventQueue.isEmpty());

        // return connection1. it should be assigned to first waiting connection (connection3)
        pool.returnConnection(connection1);
        ConnectionObject connection3 = connection3Future.join();
        assertEquals(pool.connectionCount(), 2);
        assertEquals(pool.waitingCount(), 1);
        assertEquals(pool.availableCount(), 0);
        // verify that connection 3 received a connection object
        assertTrue(connection3Future.isDone());
        assertTrue(eventQueue.isEmpty());

        // now fail connection 3 and return it.
        connection2.failConnection();
        pool.returnConnection(connection2);
        // this should not be given to the waiting request. instead a new connection should be established. 
        assertEquals(ConnectionEvent.ConnectionClosed, eventQueue.take());

        ConnectionObject connection4 = connection4Future.join();
        assertEquals(ConnectionEvent.NewConnection, eventQueue.take());
        assertEquals(pool.connectionCount(), 2);
        assertEquals(pool.waitingCount(), 0);
        assertEquals(pool.availableCount(), 0);

        // create another waiting request
        CompletableFuture<ConnectionObject> connection5Future = pool.getConnection(myReplyProc);
        assertEquals(pool.connectionCount(), 2);
        assertEquals(pool.waitingCount(), 1);
        assertEquals(pool.availableCount(), 0);
        assertFalse(connection5Future.isDone());
        assertTrue(eventQueue.isEmpty());

        // test shutdown
        pool.shutdown();
        pool.returnConnection(connection3);
        assertEquals(pool.connectionCount(), 2);
        assertEquals(pool.waitingCount(), 0);
        assertEquals(pool.availableCount(), 0);

        // connection 5 should have been returned by using connection3
        ConnectionObject connection5 = connection5Future.join();
        // since returned connection served the waiting request no new event should have been generated
        assertTrue(eventQueue.isEmpty());

        // return connection 4
        pool.returnConnection(connection4);
        assertEquals(pool.connectionCount(), 1);
        assertEquals(pool.waitingCount(), 0);
        assertEquals(pool.availableCount(), 0);
        // returned connection should be closed
        assertEquals(ConnectionEvent.ConnectionClosed, eventQueue.take());

        // we should still be able to request new connections.. request connection 6.. this should be served immediately 
        // by way of new connection
        ConnectionObject connection6 = pool.getConnection(myReplyProc).join();
        assertEquals(pool.connectionCount(), 2);
        assertEquals(pool.waitingCount(), 0);
        assertEquals(pool.availableCount(), 0);
        assertEquals(ConnectionEvent.NewConnection, eventQueue.take());

        // request connect 7. this should wait as connection could is 2. 
        CompletableFuture<ConnectionObject> connection7Future = pool.getConnection(myReplyProc);
        assertEquals(pool.connectionCount(), 2);
        assertEquals(pool.waitingCount(), 1);
        assertEquals(pool.availableCount(), 0);
        
        // return connection 5.. connection7 should get connection5's object and no new connection should be established
        pool.returnConnection(connection5);
        ConnectionObject connection7 = connection7Future.join();
        assertEquals(pool.connectionCount(), 2);
        assertEquals(pool.waitingCount(), 0);
        assertEquals(pool.availableCount(), 0);
        assertTrue(eventQueue.isEmpty());

        pool.returnConnection(connection6);
        assertEquals(pool.connectionCount(), 1);
        // verify that returned connection is not included in available connection.
        assertEquals(pool.waitingCount(), 0);
        assertEquals(pool.availableCount(), 0);
        // also the returned connection is closed
        assertEquals(ConnectionEvent.ConnectionClosed, eventQueue.take());

        pool.returnConnection(connection7);
        assertEquals(pool.connectionCount(), 0);
        assertEquals(pool.waitingCount(), 0);
        assertEquals(pool.availableCount(), 0);
        assertEquals(ConnectionEvent.ConnectionClosed, eventQueue.take());
    }
    
    private ReplyProcessor getReplyProcessor() {
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
        @Getter
        private final ReplyProcessor rp;

        public MockConnection(ReplyProcessor rp) {
            this.rp = rp;
        }

        @Override
        public void send(WireCommand cmd) throws ConnectionFailedException {

        }

        @Override
        public void send(Append append) throws ConnectionFailedException {

        }

        @Override
        public void sendAsync(WireCommand cmd, CompletedCallback callback) {

        }

        @Override
        public void sendAsync(List<Append> appends, CompletedCallback callback) {

        }

        @Override
        public void close() {

        }
    }
}