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
import lombok.extern.slf4j.Slf4j;

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
@Slf4j
class SegmentHelperConnectionManager {
    public static final int MAX_AVAILABLE_CONNECTION = 100;
    private final PravegaNodeUri uri;
    private final Object lock = new Object();
    @GuardedBy("lock")
    private final LinkedList<ConnectionObject> availableConnections;
    @GuardedBy("lock")
    private boolean isRunning;
    private final ConnectionFactory clientCF;

    SegmentHelperConnectionManager(PravegaNodeUri pravegaNodeUri, ConnectionFactory clientCF) {
        this.uri = pravegaNodeUri;
        this.clientCF = clientCF;
        this.availableConnections = new LinkedList<>();
        this.isRunning = true;
    }

    // This will not make any new connection request wait for available connection object. 
    CompletableFuture<ConnectionObject> getConnection(ReplyProcessor processor) {
        ConnectionObject obj = poll();
        if (obj != null) {
            log.debug("Returning existing connection for {}", uri);
            // return the object from the queue
            obj.processor.initialize(processor);
            return CompletableFuture.completedFuture(obj);
        } else {
            log.debug("Creating new connection for {}", uri);
            // dont create a new one.. return a created one
            ReusableReplyProcessor rp = new ReusableReplyProcessor();
            rp.initialize(processor);
            return clientCF.establishConnection(uri, rp)
                           .thenApply(connection -> new ConnectionObject(connection, rp));
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
                log.debug("ConnectionManager is shutdown");
                pair.connection.close();
            } else {
                // as connections are returned to us, we put them in queue to be reused
                if (availableConnections.size() < MAX_AVAILABLE_CONNECTION) {
                    // if returned connection increases our available connection count, do not include it
                    log.debug("Returned connection object is included in the available list");
                    availableConnections.offer(pair);
                } else {
                    log.debug("Returned connection object is discarded as available list is full");
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
        log.debug("ConnectionManager shutdown initiated");

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
            ConnectionObject polled = availableConnections.poll();
            while (polled != null) {
                if (polled.state.get().equals(ConnectionObject.ConnectionState.DISCONNECTED)) {
                    // discard.. call poll again
                    polled = availableConnections.poll();
                } else {
                    return polled;
                }
            }
            return null;
        }
    }

    static class ConnectionObject {
        private final ClientConnection connection;
        private final ReusableReplyProcessor processor;
        private final AtomicReference<ConnectionState> state;

        ConnectionObject(ClientConnection connection, ReusableReplyProcessor processor) {
            this.connection = connection;
            this.processor = processor;
            state = new AtomicReference<>(ConnectionState.CONNECTED);
        }

        ConnectionState getState() {
            return state.get();
        }

        <T> void sendAsync(WireCommand request, CompletableFuture<T> resultFuture) {
            connection.sendAsync(request, cfe -> {
                if (cfe != null) {
                    Throwable cause = Exceptions.unwrap(cfe);
                    if (cause instanceof ConnectionFailedException) {
                        resultFuture.completeExceptionally(new WireCommandFailedException(cause, request.getType(),
                                WireCommandFailedException.Reason.ConnectionFailed));
                        state.set(ConnectionState.DISCONNECTED);
                    } else {
                        resultFuture.completeExceptionally(new RuntimeException(cause));
                    }
                }
            });
        }

        private enum ConnectionState {
            CONNECTED,
            DISCONNECTED
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
            ReplyProcessor rp = replyProcessor.get();
            if (rp != null) {
                rp.hello(hello);
            }
        }

        @Override
        public void wrongHost(WireCommands.WrongHost wrongHost) {
            ReplyProcessor rp = replyProcessor.get();
            if (rp != null) {
                rp.wrongHost(wrongHost);
            }
        }

        @Override
        public void segmentAlreadyExists(WireCommands.SegmentAlreadyExists segmentAlreadyExists) {
            ReplyProcessor rp = replyProcessor.get();
            if (rp != null) {
                rp.segmentAlreadyExists(segmentAlreadyExists);
            }
        }

        @Override
        public void segmentIsSealed(WireCommands.SegmentIsSealed segmentIsSealed) {
            ReplyProcessor rp = replyProcessor.get();
            if (rp != null) {
                rp.segmentIsSealed(segmentIsSealed);
            }
        }

        @Override
        public void segmentIsTruncated(WireCommands.SegmentIsTruncated segmentIsTruncated) {
            ReplyProcessor rp = replyProcessor.get();
            if (rp != null) {
                rp.segmentIsTruncated(segmentIsTruncated);
            }
        }

        @Override
        public void noSuchSegment(WireCommands.NoSuchSegment noSuchSegment) {
            ReplyProcessor rp = replyProcessor.get();
            if (rp != null) {
                rp.noSuchSegment(noSuchSegment);
            }
        }

        @Override
        public void tableSegmentNotEmpty(WireCommands.TableSegmentNotEmpty tableSegmentNotEmpty) {
            ReplyProcessor rp = replyProcessor.get();
            if (rp != null) {
                rp.tableSegmentNotEmpty(tableSegmentNotEmpty);
            }
        }

        @Override
        public void invalidEventNumber(WireCommands.InvalidEventNumber invalidEventNumber) {
            ReplyProcessor rp = replyProcessor.get();
            if (rp != null) {
                rp.invalidEventNumber(invalidEventNumber);
            }
        }

        @Override
        public void appendSetup(WireCommands.AppendSetup appendSetup) {
            ReplyProcessor rp = replyProcessor.get();
            if (rp != null) {
                rp.appendSetup(appendSetup);
            }
        }

        @Override
        public void dataAppended(WireCommands.DataAppended dataAppended) {
            ReplyProcessor rp = replyProcessor.get();
            if (rp != null) {
                rp.dataAppended(dataAppended);
            }
        }

        @Override
        public void conditionalCheckFailed(WireCommands.ConditionalCheckFailed dataNotAppended) {
            ReplyProcessor rp = replyProcessor.get();
            if (rp != null) {
                rp.conditionalCheckFailed(dataNotAppended);
            }
        }

        @Override
        public void segmentRead(WireCommands.SegmentRead segmentRead) {
            ReplyProcessor rp = replyProcessor.get();
            if (rp != null) {
                rp.segmentRead(segmentRead);
            }
        }

        @Override
        public void segmentAttributeUpdated(WireCommands.SegmentAttributeUpdated segmentAttributeUpdated) {
            ReplyProcessor rp = replyProcessor.get();
            if (rp != null) {
                rp.segmentAttributeUpdated(segmentAttributeUpdated);
            }
        }

        @Override
        public void segmentAttribute(WireCommands.SegmentAttribute segmentAttribute) {
            ReplyProcessor rp = replyProcessor.get();
            if (rp != null) {
                rp.segmentAttribute(segmentAttribute);
            }
        }

        @Override
        public void streamSegmentInfo(WireCommands.StreamSegmentInfo streamInfo) {
            ReplyProcessor rp = replyProcessor.get();
            if (rp != null) {
                rp.streamSegmentInfo(streamInfo);
            }
        }

        @Override
        public void segmentCreated(WireCommands.SegmentCreated segmentCreated) {
            ReplyProcessor rp = replyProcessor.get();
            if (rp != null) {
                rp.segmentCreated(segmentCreated);
            }
        }

        @Override
        public void segmentsMerged(WireCommands.SegmentsMerged segmentsMerged) {
            ReplyProcessor rp = replyProcessor.get();
            if (rp != null) {
                rp.segmentsMerged(segmentsMerged);
            }
        }

        @Override
        public void segmentSealed(WireCommands.SegmentSealed segmentSealed) {
            ReplyProcessor rp = replyProcessor.get();
            if (rp != null) {
                rp.segmentSealed(segmentSealed);
            }
        }

        @Override
        public void segmentTruncated(WireCommands.SegmentTruncated segmentTruncated) {
            ReplyProcessor rp = replyProcessor.get();
            if (rp != null) {
                rp.segmentTruncated(segmentTruncated);
            }
        }

        @Override
        public void segmentDeleted(WireCommands.SegmentDeleted segmentDeleted) {
            ReplyProcessor rp = replyProcessor.get();
            if (rp != null) {
                rp.segmentDeleted(segmentDeleted);
            }
        }

        @Override
        public void operationUnsupported(WireCommands.OperationUnsupported operationUnsupported) {
            ReplyProcessor rp = replyProcessor.get();
            if (rp != null) {
                rp.operationUnsupported(operationUnsupported);
            }
        }

        @Override
        public void keepAlive(WireCommands.KeepAlive keepAlive) {
            ReplyProcessor rp = replyProcessor.get();
            if (rp != null) {
                rp.keepAlive(keepAlive);
            }
        }

        @Override
        public void connectionDropped() {
            
            ReplyProcessor rp = replyProcessor.get();
            if (rp != null) {
                rp.connectionDropped();
            }
        }

        @Override
        public void segmentPolicyUpdated(WireCommands.SegmentPolicyUpdated segmentPolicyUpdated) {
            ReplyProcessor rp = replyProcessor.get();
            if (rp != null) {
                rp.segmentPolicyUpdated(segmentPolicyUpdated);
            }
        }

        @Override
        public void processingFailure(Exception error) {
            ReplyProcessor rp = replyProcessor.get();
            if (rp != null) {
                rp.processingFailure(error);
            }
        }

        @Override
        public void authTokenCheckFailed(WireCommands.AuthTokenCheckFailed authTokenCheckFailed) {
            ReplyProcessor rp = replyProcessor.get();
            if (rp != null) {
                rp.authTokenCheckFailed(authTokenCheckFailed);
            }
        }

        @Override
        public void tableEntriesUpdated(WireCommands.TableEntriesUpdated tableEntriesUpdated) {
            ReplyProcessor rp = replyProcessor.get();
            if (rp != null) {
                rp.tableEntriesUpdated(tableEntriesUpdated);
            }
        }

        @Override
        public void tableKeysRemoved(WireCommands.TableKeysRemoved tableKeysRemoved) {
            ReplyProcessor rp = replyProcessor.get();
            if (rp != null) {
                rp.tableKeysRemoved(tableKeysRemoved);
            }
        }

        @Override
        public void tableRead(WireCommands.TableRead tableRead) {
            ReplyProcessor rp = replyProcessor.get();
            if (rp != null) {
                rp.tableRead(tableRead);
            }
        }

        @Override
        public void tableKeyDoesNotExist(WireCommands.TableKeyDoesNotExist tableKeyDoesNotExist) {
            ReplyProcessor rp = replyProcessor.get();
            if (rp != null) {
                rp.tableKeyDoesNotExist(tableKeyDoesNotExist);
            }
        }

        @Override
        public void tableKeyBadVersion(WireCommands.TableKeyBadVersion tableKeyBadVersion) {
            ReplyProcessor rp = replyProcessor.get();
            if (rp != null) {
                rp.tableKeyBadVersion(tableKeyBadVersion);
            }
        }

        @Override
        public void tableKeysRead(WireCommands.TableKeysRead tableKeysRead) {
            ReplyProcessor rp = replyProcessor.get();
            if (rp != null) {
                rp.tableKeysRead(tableKeysRead);
            }
        }

        @Override
        public void tableEntriesRead(WireCommands.TableEntriesRead tableEntriesRead) {
            ReplyProcessor rp = replyProcessor.get();
            if (rp != null) {
                rp.tableEntriesRead(tableEntriesRead);
            }
        }
    }
}
