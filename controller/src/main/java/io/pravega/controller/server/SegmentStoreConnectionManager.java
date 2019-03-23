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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import io.pravega.client.netty.impl.ClientConnection;
import io.pravega.client.netty.impl.ConnectionFactory;
import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.Futures;
import io.pravega.controller.util.Config;
import io.pravega.shared.protocol.netty.ConnectionFailedException;
import io.pravega.shared.protocol.netty.PravegaNodeUri;
import io.pravega.shared.protocol.netty.ReplyProcessor;
import io.pravega.shared.protocol.netty.WireCommand;
import io.pravega.shared.protocol.netty.WireCommands;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.ParametersAreNonnullByDefault;
import javax.annotation.concurrent.GuardedBy;
import java.util.ArrayDeque;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

@Slf4j
/**
 * Connection Manager class that maintains a cache of connection pools to connection to different segment stores. 
 * Users of connection manager class can request for a connection pool and then request connections on the pool and return 
 * the connections when done. 
 * This maintains a cache of connection pools to segment stores. Any segmentstore for which no new connection request
 * comes for a while, its pool is evicted from the cache which triggers a shutdown on the pool. 
 * If newer requests are received for the said segmentstore, a new pool gets created. However, any callers using the existing 
 * pool can continue to do so. The pool in shutdown mode simply drains all available connections. And when it has no references left
 * it can be garbage collected. 
 */
class SegmentStoreConnectionManager {
    // cache of connection manager for segment store nodes.
    // Pravega Connection Manager maintains a pool of connection for a segment store and returns a connection from 
    // the pool on the need basis. 
    private final LoadingCache<PravegaNodeUri, SegmentStoreConnectionPool> cache;

    public SegmentStoreConnectionManager(final ConnectionFactory clientCF) {
        this.cache = CacheBuilder.newBuilder()
                            .maximumSize(Config.HOST_STORE_CONTAINER_COUNT)
                            // if a host is not accessed for 5 minutes, remove it from the cache
                            .expireAfterAccess(5, TimeUnit.MINUTES)
                            .removalListener((RemovalListener<PravegaNodeUri, SegmentStoreConnectionPool>) removalNotification -> {
                                // Whenever a connection manager is evicted from the cache call shutdown on it. 
                                removalNotification.getValue().shutdown();
                            })
                            .build(new CacheLoader<PravegaNodeUri, SegmentStoreConnectionPool>() {
                                @Override
                                @ParametersAreNonnullByDefault
                                public SegmentStoreConnectionPool load(PravegaNodeUri nodeUri) {
                                    return new SegmentStoreConnectionPool(nodeUri, clientCF);
                                }
                            });

    }

    public SegmentStoreConnectionPool getPool(PravegaNodeUri uri) {
        return cache.getUnchecked(uri);
    }

    /**
     * This is a connection manager class to manage connection to a given segmentStore node identified by PravegaNodeUri. 
     * It maintains a pool of available connections and creates a maximum number of concurrent connections. 
     * Users can request for connection from this class and it will opportunistically use existing connections or create new 
     * connections to a given segment store server and return the connection.
     * It ensures that there are only a limited number of concurrent connections created. If more users request for connection
     * it would add them to wait queue and as connections become available (when existing connections are returned),  
     * this class opportunistically tries to reuse the returned connection to fulfil waiting requests. If there are no 
     * waiting requests, this class tries to maintain an available connection pool of predetermined size. If more number of 
     * connections than max available size is returned to it, the classes closes those connections to free up resources.  
     *
     * It is important to note that this is not a connection pooling class. 
     * It simply reuses already created connections to send additional commands over it. It doesnt multiplex commands on the 
     * same connection concurrently.  
     * As users finish their processing, they should return the connection back to this class. 
     *
     * The connectionManager can be shutdown as well. However, the shutdown trigger does not prevent from callers to attempt 
     * to create new connections and new connections will be served. Shutdown ensures that 
     */
    static class SegmentStoreConnectionPool {
        private static final int MAX_CONCURRENT_CONNECTIONS = 500;
        private static final int MAX_AVAILABLE_CONNECTIONS = 100;
        private final PravegaNodeUri uri;
        private final Object lock = new Object();
        @GuardedBy("lock")
        private final ArrayDeque<ConnectionObject> availableConnections;
        @GuardedBy("lock")
        private boolean isRunning;
        @GuardedBy("lock")
        private final ArrayDeque<WaitingRequest> waitQueue;
        @GuardedBy("lock")
        private int connectionCount;
        private final ConnectionFactory clientCF;
        private final int maxConcurrentConnections;
        private final int maxAvailableConnections;

        private final ConnectionListener connectionListener;

        SegmentStoreConnectionPool(PravegaNodeUri pravegaNodeUri, ConnectionFactory clientCF) {
            this(pravegaNodeUri, clientCF, MAX_CONCURRENT_CONNECTIONS, MAX_AVAILABLE_CONNECTIONS, null);
        }

        @VisibleForTesting
        SegmentStoreConnectionPool(PravegaNodeUri pravegaNodeUri, ConnectionFactory clientCF,
                                   int maxConcurrentConnections, int maxAvailableConnections, ConnectionListener listener) {
            this.uri = pravegaNodeUri;
            this.clientCF = clientCF;
            this.availableConnections = new ArrayDeque<>();
            this.isRunning = true;
            this.waitQueue = new ArrayDeque<>();
            this.connectionCount = 0;
            this.maxConcurrentConnections = maxConcurrentConnections;
            this.maxAvailableConnections = maxAvailableConnections;
            this.connectionListener = listener;
        }

        /**
         * Method to get a connection object with the supplied replyprocessor to use.
         * This method attempts to find an existing available connection.
         * If not founf, it opportunistically submits a request to try to create a new connection.
         * It submits a new waiting request for whenever a connection becomes available. A connection could become available
         * because someone returned an existing connection or a new connection was created.
         *
         * @param processor reply processor to use
         * @return A completableFuture which when completed will have the connection object that the caller can use to
         * communicate with segment store and receive response on the supplied reply processor.
         */
        CompletableFuture<ConnectionObject> getConnection(ReplyProcessor processor) {
            CompletableFuture<ConnectionObject> connectionFuture;
            boolean tryCreateNewConnection = false;
            synchronized (lock) {
                ConnectionObject obj = availableConnections.poll();
                if (obj != null) {
                    log.debug("Returning existing connection for {}", uri);
                    // return the object from the queue
                    obj.reusableReplyProcessor.initialize(processor);
                    connectionFuture = CompletableFuture.completedFuture(obj);
                } else {
                    CompletableFuture<ConnectionObject> future = new CompletableFuture<>();
                    WaitingRequest request = new WaitingRequest(future, processor);
                    waitQueue.add(request);
                    connectionFuture = request.getFuture();
                    tryCreateNewConnection = true;
                }
            }
            if (tryCreateNewConnection) {
                tryCreateNewConnection();
            }
            return connectionFuture;
        }

        /**
         * Users use this method to return the connection back to the Connection Manager for reuse.
         * This method first checks the state of the connection and if the connection is disconnected, it is closed.
         * For valid connections, it checks if there is a waiting request for a connection and fulfils that first.
         * If there are no waiting requests, it checks if the available queue has the capacity to keep this connection.
         * If not, it destroys this connection.
         *
         * @param connectionObject connection to return to the pool.
         */
        void returnConnection(ConnectionObject connectionObject) {
            connectionObject.reusableReplyProcessor.uninitialize();
            if (connectionObject.state.get().equals(ConnectionObject.ConnectionState.DISCONNECTED)) {
                handleDisconnected(connectionObject);
            } else {
                boolean toClose = false;
                synchronized (lock) {
                    WaitingRequest waiting = waitQueue.poll();
                    if (waiting != null) {
                        connectionObject.reusableReplyProcessor.initialize(waiting.getReplyProcessor());
                        waiting.getFuture().complete(connectionObject);
                    } else {
                        if (!isRunning) {
                            // The connection will be closed if returned anytime after the shutdown has been initiated.
                            log.debug("ConnectionManager is shutdown");
                            connectionCount--;
                            toClose = true;
                        } else {
                            // as connections are returned to us, we put them in queue to be reused
                            if (availableConnections.size() < maxAvailableConnections) {
                                // if returned connection increases our available connection count, do not include it
                                log.debug("Returned connection object is included in the available list");
                                availableConnections.offer(connectionObject);
                            } else {
                                log.debug("Returned connection object is discarded as available list is full");
                                connectionCount--;
                                toClose = true;
                            }
                        }
                    }
                }

                if (toClose) {
                    if (connectionListener != null) {
                        connectionListener.notify(ConnectionListener.ConnectionEvent.ConnectionClosed);
                    }
                    connectionObject.connection.close();
                }
            }
        }

        /**
         * This method will try to create a new connection only if BOTH conditions are met
         * 1. connectionCount is less than MAX_CONCURRENT_CONNECTIONS.
         * 2. it has taken a waiting request from the waitQueue.
         */
        private void tryCreateNewConnection() {
            WaitingRequest waiting = null;
            synchronized (lock) {
                if (connectionCount < maxConcurrentConnections) {
                    waiting = waitQueue.poll();
                    if (waiting != null) {
                        connectionCount++;
                    }
                }
            }

            if (waiting != null) {
                log.debug("Creating new connection for {}", uri);
                ReusableReplyProcessor rp = new ReusableReplyProcessor();
                rp.initialize(waiting.getReplyProcessor());
                Futures.completeAfter(
                        () -> clientCF.establishConnection(uri, rp)
                                      .thenApply(connection -> new ConnectionObject(connection, rp))
                                      .whenComplete((r, e) -> {
                                          if (connectionListener != null) {
                                              connectionListener.notify(ConnectionListener.ConnectionEvent.NewConnection);
                                          }
                                      }), waiting.getFuture());
            }
        }

        private void handleDisconnected(ConnectionObject connectionObject) {
            connectionObject.connection.close();
            if (connectionListener != null) {
                connectionListener.notify(ConnectionListener.ConnectionEvent.ConnectionClosed);
            }
            boolean tryCreateNewConnection;
            synchronized (lock) {
                connectionCount--;
                tryCreateNewConnection = !waitQueue.isEmpty();
            }

            if (tryCreateNewConnection) {
                tryCreateNewConnection();
            }
        }

        // region getters for testing
        @VisibleForTesting
        int connectionCount() {
            synchronized (lock) {
                return connectionCount;
            }
        }

        @VisibleForTesting
        int availableCount() {
            synchronized (lock) {
                return availableConnections.size();
            }
        }

        @VisibleForTesting
        int waitingCount() {
            synchronized (lock) {
                return waitQueue.size();
            }
        }
        // endregion

        /**
         * Shutdown the connection manager where all returned connections are closed and not put back into the
         * available queue of connections.
         * It is important to note that even after shutdown is initiated, if `getConnection` is invoked, it will return a connection.
         */
        void shutdown() {
            log.debug("ConnectionManager shutdown initiated");

            // as connections are returned we need to shut them down
            ConnectionObject connection;
            synchronized (lock) {
                isRunning = false;
                connection = availableConnections.poll();
            }
            while (connection != null) {
                returnConnection(connection);
                synchronized (lock) {
                    connection = availableConnections.poll();
                }
            }
        }
    }
    
    static class ConnectionObject {
        private final ClientConnection connection;
        private final ReusableReplyProcessor reusableReplyProcessor;
        private final AtomicReference<ConnectionState> state;

        ConnectionObject(ClientConnection connection, ReusableReplyProcessor processor) {
            this.connection = connection;
            this.reusableReplyProcessor = processor;
            state = new AtomicReference<>(ConnectionState.CONNECTED);
        }

        ConnectionState getState() {
            return state.get();
        }

        @VisibleForTesting
        void failConnection() {
            state.set(ConnectionState.DISCONNECTED);    
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

    @VisibleForTesting
    static interface ConnectionListener {
        enum ConnectionEvent {
            NewConnection,
            ConnectionClosed
        }
        
        void notify(ConnectionEvent event);
    }
    
    @Data
    private static class WaitingRequest {
        private final CompletableFuture<ConnectionObject> future;
        private final ReplyProcessor replyProcessor;
    }
    /**
     *  A reusable reply processor class which can be initialized and uninitialized with new ReplyProcessor. 
     *  This same replyProcessor can be reused with the same connection for handling different replies from servers for
     *  different calls.
     */
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
