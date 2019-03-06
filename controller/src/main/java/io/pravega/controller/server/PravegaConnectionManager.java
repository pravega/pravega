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
import io.pravega.common.Exceptions;
import io.pravega.shared.protocol.netty.ConnectionFailedException;
import io.pravega.shared.protocol.netty.PravegaNodeUri;
import io.pravega.shared.protocol.netty.ReplyProcessor;
import io.pravega.shared.protocol.netty.WireCommand;
import io.pravega.shared.protocol.netty.WireCommands;

import javax.annotation.concurrent.GuardedBy;
import java.util.LinkedList;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Users can request for connection from this class and it will opportunistically use existing connections or create new 
 * connections to a given segment store server and return the connection. 
 * It also opportunistically tries to reclaim and close unused connections and tries to maintain at max a pre determined 
 * number of available connections.  
 * This is not a connection pooling class. It simply reuses already created connections to send additional commands over it. 
 * As users finish their processing, they  
 * This class does not guarantee that there will be a limited number of connections to pick from. 
 * However, everytime the number of connections go beyond a certain capacity
 */
class PravegaConnectionManager {
    public static final int MAX_AVAILABLE_CONNECTION = 10;
    private final PravegaNodeUri uri;
    private final Object lock = new Object();
    @GuardedBy("lock")
    private final LinkedList<ConnectionObject> availableConnections;
    @GuardedBy("lock")
    private boolean isRunning;
    private final ConnectionFactory clientCF;

    PravegaConnectionManager(PravegaNodeUri pravegaNodeUri, ConnectionFactory clientCF) {
        this.uri = pravegaNodeUri;
        this.clientCF = clientCF;
        this.availableConnections = new LinkedList<>();
        this.isRunning = true;
    }

    // This will not make any new connection request wait for available connection object. 
    CompletableFuture<ConnectionObject> getConnection(ReplyProcessor processor) {
        ConnectionObject obj = poll();
        if (obj != null) {
            // return the object from the queue
            obj.processor.initialize(processor);
            return CompletableFuture.completedFuture(obj);
        } else {
            // dont create a new one.. return a created one
            ReusableReplyProcessor rp = new ReusableReplyProcessor();
            rp.initialize(processor);
            return clientCF.establishConnection(uri, rp)
                           .thenApply(connection1 -> {
                               ConnectionObject p = new ConnectionObject(connection1, rp);
                               return p;
                           });
        }
    }
    
    /**
     * Users use this method to return the connection back to the Connection Manager for reuse. 
     */
    void returnConnection(ConnectionObject pair) {
        pair.processor.uninitialize();
        synchronized (lock) {
            if (!isRunning) {
                // The connection will be closed if returned anytime after the shutdown has been initiated.
                pair.connection.close();
            } else {
                // as connections are returned to us, we put them in queue to be reused
                if (availableConnections.size() < MAX_AVAILABLE_CONNECTION) {
                    // if returned connection increases our available connection count, do not include it
                    availableConnections.offer(pair);
                } else {
                    pair.connection.close();
                }
            }
        }
    }

    /**
     * Shutdown the connection manager where all returned connections are closed and not put back into the 
     * available queue of connections.  
     * It is important to note that even after shutdown is initiated, if `getConnection` is invoked, it will return a connection. 
     */
    void shutdown() {
        // as connections are returned we need to shut them down
        synchronized (lock) {
            isRunning = false;
        }
        ConnectionObject connection = poll();
        while (connection != null) {
            returnConnection(connection);
            connection = poll();
        }
    }

    private ConnectionObject poll() {
        synchronized (lock) {
            return availableConnections.poll();
        }
    }
    
    static class ConnectionObject {
        private final ClientConnection connection;
        private final ReusableReplyProcessor processor;

        ConnectionObject(ClientConnection connection, ReusableReplyProcessor processor) {
            this.connection = connection;
            this.processor = processor;
        }

        <T> void sendAsync(WireCommand request, CompletableFuture<T> resultFuture) {
            connection.sendAsync(request, cfe -> {
                if (cfe != null) {
                    Throwable cause = Exceptions.unwrap(cfe);
                    if (cause instanceof ConnectionFailedException) {
                        resultFuture.completeExceptionally(new WireCommandFailedException(cause, request.getType(), 
                                WireCommandFailedException.Reason.ConnectionFailed));
                    } else {
                        resultFuture.completeExceptionally(new RuntimeException(cause));
                    }
                }
            });
        }
    }

    // A reusable reply processor class which can be initialized and uninitialized with new ReplyProcessor. 
    // This same replyProcessor can be reused with the same connection for handling different replies from servers for
    // different calls.
    private static class ReusableReplyProcessor implements ReplyProcessor {
        private final AtomicReference<ReplyProcessor> replyProcessor = new AtomicReference<>();

        // initialize the reusable reply processor class with a new reply processor
        void initialize(ReplyProcessor replyProcessor) {
            this.replyProcessor.set(replyProcessor);
        }

        // unset reply processor
        void uninitialize() {
            replyProcessor.set(null);
        }

        @Override
        public void hello(WireCommands.Hello hello) {
            replyProcessor.get().hello(hello);
        }

        @Override
        public void wrongHost(WireCommands.WrongHost wrongHost) {
            replyProcessor.get().wrongHost(wrongHost);
        }

        @Override
        public void segmentAlreadyExists(WireCommands.SegmentAlreadyExists segmentAlreadyExists) {
            replyProcessor.get().segmentAlreadyExists(segmentAlreadyExists);
        }

        @Override
        public void segmentIsSealed(WireCommands.SegmentIsSealed segmentIsSealed) {
            replyProcessor.get().segmentIsSealed(segmentIsSealed);
        }

        @Override
        public void segmentIsTruncated(WireCommands.SegmentIsTruncated segmentIsTruncated) {
            replyProcessor.get().segmentIsTruncated(segmentIsTruncated);
        }

        @Override
        public void noSuchSegment(WireCommands.NoSuchSegment noSuchSegment) {
            replyProcessor.get().noSuchSegment(noSuchSegment);
        }

        @Override
        public void tableSegmentNotEmpty(WireCommands.TableSegmentNotEmpty tableSegmentNotEmpty) {
            replyProcessor.get().tableSegmentNotEmpty(tableSegmentNotEmpty);
        }

        @Override
        public void invalidEventNumber(WireCommands.InvalidEventNumber invalidEventNumber) {
            replyProcessor.get().invalidEventNumber(invalidEventNumber);
        }

        @Override
        public void appendSetup(WireCommands.AppendSetup appendSetup) {
            replyProcessor.get().appendSetup(appendSetup);
        }

        @Override
        public void dataAppended(WireCommands.DataAppended dataAppended) {
            replyProcessor.get().dataAppended(dataAppended);
        }

        @Override
        public void conditionalCheckFailed(WireCommands.ConditionalCheckFailed dataNotAppended) {
            replyProcessor.get().conditionalCheckFailed(dataNotAppended);
        }

        @Override
        public void segmentRead(WireCommands.SegmentRead segmentRead) {
            replyProcessor.get().segmentRead(segmentRead);
        }

        @Override
        public void segmentAttributeUpdated(WireCommands.SegmentAttributeUpdated segmentAttributeUpdated) {
            replyProcessor.get().segmentAttributeUpdated(segmentAttributeUpdated);
        }

        @Override
        public void segmentAttribute(WireCommands.SegmentAttribute segmentAttribute) {
            replyProcessor.get().segmentAttribute(segmentAttribute);
        }

        @Override
        public void streamSegmentInfo(WireCommands.StreamSegmentInfo streamInfo) {
            replyProcessor.get().streamSegmentInfo(streamInfo);
        }

        @Override
        public void segmentCreated(WireCommands.SegmentCreated segmentCreated) {
            replyProcessor.get().segmentCreated(segmentCreated);
        }

        @Override
        public void segmentsMerged(WireCommands.SegmentsMerged segmentsMerged) {
            replyProcessor.get().segmentsMerged(segmentsMerged);
        }

        @Override
        public void segmentSealed(WireCommands.SegmentSealed segmentSealed) {
            replyProcessor.get().segmentSealed(segmentSealed);
        }

        @Override
        public void segmentTruncated(WireCommands.SegmentTruncated segmentTruncated) {
            replyProcessor.get().segmentTruncated(segmentTruncated);
        }

        @Override
        public void segmentDeleted(WireCommands.SegmentDeleted segmentDeleted) {
            replyProcessor.get().segmentDeleted(segmentDeleted);
        }

        @Override
        public void operationUnsupported(WireCommands.OperationUnsupported operationUnsupported) {
            replyProcessor.get().operationUnsupported(operationUnsupported);
        }

        @Override
        public void keepAlive(WireCommands.KeepAlive keepAlive) {
            replyProcessor.get().keepAlive(keepAlive);
        }

        @Override
        public void connectionDropped() {
            replyProcessor.get().connectionDropped();
        }

        @Override
        public void segmentPolicyUpdated(WireCommands.SegmentPolicyUpdated segmentPolicyUpdated) {
            replyProcessor.get().segmentPolicyUpdated(segmentPolicyUpdated);
        }

        @Override
        public void processingFailure(Exception error) {
            replyProcessor.get().processingFailure(error);
        }

        @Override
        public void authTokenCheckFailed(WireCommands.AuthTokenCheckFailed authTokenCheckFailed) {
            replyProcessor.get().authTokenCheckFailed(authTokenCheckFailed);
        }

        @Override
        public void tableEntriesUpdated(WireCommands.TableEntriesUpdated tableEntriesUpdated) {
            replyProcessor.get().tableEntriesUpdated(tableEntriesUpdated);
        }

        @Override
        public void tableKeysRemoved(WireCommands.TableKeysRemoved tableKeysRemoved) {
            replyProcessor.get().tableKeysRemoved(tableKeysRemoved);
        }

        @Override
        public void tableRead(WireCommands.TableRead tableRead) {
            replyProcessor.get().tableRead(tableRead);
        }

        @Override
        public void tableKeyDoesNotExist(WireCommands.TableKeyDoesNotExist tableKeyDoesNotExist) {
            replyProcessor.get().tableKeyDoesNotExist(tableKeyDoesNotExist);
        }

        @Override
        public void tableKeyBadVersion(WireCommands.TableKeyBadVersion tableKeyBadVersion) {
            replyProcessor.get().tableKeyBadVersion(tableKeyBadVersion);
        }

        @Override
        public void tableKeysRead(WireCommands.TableKeysRead tableKeysRead) {
            replyProcessor.get().tableKeysRead(tableKeysRead);
        }

        @Override
        public void tableEntriesRead(WireCommands.TableEntriesRead tableEntriesRead) {
            replyProcessor.get().tableEntriesRead(tableEntriesRead);
        }
    }
}
