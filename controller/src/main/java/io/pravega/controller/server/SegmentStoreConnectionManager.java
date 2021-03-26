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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import io.pravega.client.connection.impl.ClientConnection;
import io.pravega.client.connection.impl.ConnectionFactory;
import io.pravega.common.Exceptions;
import io.pravega.common.util.ResourcePool;
import io.pravega.controller.util.Config;
import io.pravega.shared.protocol.netty.ConnectionFailedException;
import io.pravega.shared.protocol.netty.PravegaNodeUri;
import io.pravega.shared.protocol.netty.ReplyProcessor;
import io.pravega.shared.protocol.netty.WireCommand;
import io.pravega.shared.protocol.netty.WireCommands;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import javax.annotation.ParametersAreNonnullByDefault;
import lombok.extern.slf4j.Slf4j;

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
@Slf4j
class SegmentStoreConnectionManager implements AutoCloseable {
    private static final int MAX_CONCURRENT_CONNECTIONS = 500;
    private static final int MAX_IDLE_CONNECTIONS = 100;
    // cache of connection manager for segment store nodes.
    // Pravega Connection Manager maintains a pool of connection for a segment store and returns a connection from
    // the pool on the need basis.
    private final LoadingCache<PravegaNodeUri, SegmentStoreConnectionPool> cache;

    SegmentStoreConnectionManager(final ConnectionFactory clientCF) {
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


    CompletableFuture<ConnectionWrapper> getConnection(PravegaNodeUri uri, ReplyProcessor replyProcessor) {
        return cache.getUnchecked(uri).getConnection(replyProcessor);
    }

    @Override
    public void close() {
        cache.invalidateAll();
        cache.cleanUp();
    }

    /**
     * This is a connection manager class to manage connection to a given segmentStore node identified by PravegaNodeUri.
     * It uses {@link ResourcePool} to create pool of available connections and specify a maximum number of concurrent connections.
     * Users can request for connection from this class and it will opportunistically use existing connections or create new
     * connections to a given segment store server and return the connection.
     * It ensures that there are only a limited number of concurrent connections created. If more users request for connection
     * it would add them to wait queue and as connections become available (when existing connections are returned),
     * this class opportunistically tries to reuse the returned connection to fulfil waiting requests. If there are no
     * waiting requests, this class tries to maintain an available connection pool of predetermined size. If more number of
     * connections than max available size is returned to it, the classes closes those connections to free up resources.
     *
     * It is important to note that the intent is not to multiplex multiple requests over a single connection concurrently.
     * It simply reuses already created connections to send additional commands over it.
     * As users finish their processing, they should return the connection back to this class.
     *
     * The connectionManager can be shutdown as well. However, the shutdown trigger does not prevent callers to attempt
     * to create new connections and new connections will be served. Shutdown ensures that it drains all available connections
     * and as connections are returned, they are not reused.
     */
    static class SegmentStoreConnectionPool extends ResourcePool<ConnectionObject> {
        @VisibleForTesting
        SegmentStoreConnectionPool(PravegaNodeUri pravegaNodeUri, ConnectionFactory clientCF) {
            this(pravegaNodeUri, clientCF, MAX_CONCURRENT_CONNECTIONS, MAX_IDLE_CONNECTIONS);
        }

        @VisibleForTesting
        SegmentStoreConnectionPool(PravegaNodeUri pravegaNodeUri, ConnectionFactory clientCF, int maxConcurrent, int maxIdle) {
            super(() -> {
                ReusableReplyProcessor rp = new ReusableReplyProcessor();
                return clientCF.establishConnection(pravegaNodeUri, rp)
                              .thenApply(connection -> new ConnectionObject(connection, rp));
            }, connectionObj -> connectionObj.connection.close(), maxConcurrent, maxIdle);
        }

        CompletableFuture<ConnectionWrapper> getConnection(ReplyProcessor replyProcessor) {
            return getResource()
                    .thenApply(closeableResource -> {
                        ConnectionObject connectionObject = closeableResource.getResource();
                        connectionObject.reusableReplyProcessor.initialize(replyProcessor);
                        return new ConnectionWrapper(closeableResource);
                    });
        }
    }

    static class ConnectionWrapper implements AutoCloseable {
        private final ResourcePool.CloseableResource<ConnectionObject> resource;
        private AtomicBoolean isClosed;
        private ConnectionWrapper(ResourcePool.CloseableResource<ConnectionObject> resource) {
            this.resource = resource;
            this.isClosed = new AtomicBoolean(false);
        }

        void failConnection() {
            resource.getResource().failConnection();
        }

        <T> void sendAsync(WireCommand request, CompletableFuture<T> resultFuture) {
            resource.getResource().sendAsync(request, resultFuture);
        }

        // region for testing
        @VisibleForTesting
        ConnectionObject.ConnectionState getState() {
            return resource.getResource().state.get();
        }

        @VisibleForTesting
        ClientConnection getConnection() {
            return resource.getResource().connection;
        }

        @VisibleForTesting
        ReplyProcessor getReplyProcessor() {
            return resource.getResource().reusableReplyProcessor.replyProcessor.get();
        }
        // endregion

        @Override
        public void close() {
            if (isClosed.compareAndSet(false, true)) {
                ConnectionObject connectionObject = resource.getResource();
                connectionObject.reusableReplyProcessor.uninitialize();
                if (!connectionObject.state.get().equals(ConnectionObject.ConnectionState.CONNECTED)) {
                    resource.invalidate();
                }
                this.resource.close();
            }
        }
    }

    private static class ConnectionObject {
        private final ClientConnection connection;
        private final ReusableReplyProcessor reusableReplyProcessor;
        private final AtomicReference<ConnectionState> state;

        ConnectionObject(ClientConnection connection, ReusableReplyProcessor processor) {
            this.connection = connection;
            this.reusableReplyProcessor = processor;
            state = new AtomicReference<>(ConnectionState.CONNECTED);
        }

        private void failConnection() {
            state.set(ConnectionState.DISCONNECTED);
        }

        private <T> void sendAsync(WireCommand request, CompletableFuture<T> resultFuture) {
            try {
                connection.send(request);
            } catch (ConnectionFailedException cfe) {
                Throwable cause = Exceptions.unwrap(cfe);
                resultFuture.completeExceptionally(new WireCommandFailedException(cause, request.getType(),
                                                                                  WireCommandFailedException.Reason.ConnectionFailed));
                state.set(ConnectionState.DISCONNECTED);
            }
        }

        private enum ConnectionState {
            CONNECTED,
            DISCONNECTED
        }
    }

    /**
     *  A reusable reply processor class which can be initialized and uninitialized with new ReplyProcessor.
     *  This same replyProcessor can be reused with the same connection for handling different replies from servers for
     *  different calls.
     */
    @VisibleForTesting
    static class ReusableReplyProcessor implements ReplyProcessor {
        private final AtomicReference<ReplyProcessor> replyProcessor = new AtomicReference<>();

        // initialize the reusable reply processor class with a new reply processor
        void initialize(ReplyProcessor replyProcessor) {
            this.replyProcessor.set(replyProcessor);
        }

        // unset reply processor
        void uninitialize() {
            replyProcessor.set(null);
        }

        private <T> void execute(BiConsumer<ReplyProcessor, T> toInvoke, T arg) {
            ReplyProcessor rp = replyProcessor.get();
            if (rp != null) {
                toInvoke.accept(rp, arg);
            }
        }

        private void execute(Consumer<ReplyProcessor> toInvoke) {
            ReplyProcessor rp = replyProcessor.get();
            if (rp != null) {
                toInvoke.accept(rp);
            }
        }

        @Override
        public void hello(WireCommands.Hello hello) {
            execute(ReplyProcessor::hello, hello);
        }

        @Override
        public void wrongHost(WireCommands.WrongHost wrongHost) {
            execute(ReplyProcessor::wrongHost, wrongHost);
        }

        @Override
        public void segmentAlreadyExists(WireCommands.SegmentAlreadyExists segmentAlreadyExists) {
            execute(ReplyProcessor::segmentAlreadyExists, segmentAlreadyExists);
        }

        @Override
        public void segmentIsSealed(WireCommands.SegmentIsSealed segmentIsSealed) {
            execute(ReplyProcessor::segmentIsSealed, segmentIsSealed);
        }

        @Override
        public void segmentIsTruncated(WireCommands.SegmentIsTruncated segmentIsTruncated) {
            execute(ReplyProcessor::segmentIsTruncated, segmentIsTruncated);
        }

        @Override
        public void noSuchSegment(WireCommands.NoSuchSegment noSuchSegment) {
            execute(ReplyProcessor::noSuchSegment, noSuchSegment);
        }

        @Override
        public void tableSegmentNotEmpty(WireCommands.TableSegmentNotEmpty tableSegmentNotEmpty) {
            execute(ReplyProcessor::tableSegmentNotEmpty, tableSegmentNotEmpty);
        }

        @Override
        public void invalidEventNumber(WireCommands.InvalidEventNumber invalidEventNumber) {
            execute(ReplyProcessor::invalidEventNumber, invalidEventNumber);
        }

        @Override
        public void appendSetup(WireCommands.AppendSetup appendSetup) {
            execute(ReplyProcessor::appendSetup, appendSetup);
        }

        @Override
        public void dataAppended(WireCommands.DataAppended dataAppended) {
            execute(ReplyProcessor::dataAppended, dataAppended);
        }

        @Override
        public void conditionalCheckFailed(WireCommands.ConditionalCheckFailed dataNotAppended) {
            execute(ReplyProcessor::conditionalCheckFailed, dataNotAppended);
        }

        @Override
        public void segmentRead(WireCommands.SegmentRead segmentRead) {
            execute(ReplyProcessor::segmentRead, segmentRead);
        }

        @Override
        public void segmentAttributeUpdated(WireCommands.SegmentAttributeUpdated segmentAttributeUpdated) {
            execute(ReplyProcessor::segmentAttributeUpdated, segmentAttributeUpdated);
        }

        @Override
        public void segmentAttribute(WireCommands.SegmentAttribute segmentAttribute) {
            execute(ReplyProcessor::segmentAttribute, segmentAttribute);
        }

        @Override
        public void streamSegmentInfo(WireCommands.StreamSegmentInfo streamInfo) {
            execute(ReplyProcessor::streamSegmentInfo, streamInfo);
        }

        @Override
        public void segmentCreated(WireCommands.SegmentCreated segmentCreated) {
            execute(ReplyProcessor::segmentCreated, segmentCreated);
        }

        @Override
        public void segmentsMerged(WireCommands.SegmentsMerged segmentsMerged) {
            execute(ReplyProcessor::segmentsMerged, segmentsMerged);
        }

        @Override
        public void segmentSealed(WireCommands.SegmentSealed segmentSealed) {
            execute(ReplyProcessor::segmentSealed, segmentSealed);
        }

        @Override
        public void segmentTruncated(WireCommands.SegmentTruncated segmentTruncated) {
            execute(ReplyProcessor::segmentTruncated, segmentTruncated);
        }

        @Override
        public void segmentDeleted(WireCommands.SegmentDeleted segmentDeleted) {
            execute(ReplyProcessor::segmentDeleted, segmentDeleted);
        }

        @Override
        public void operationUnsupported(WireCommands.OperationUnsupported operationUnsupported) {
            execute(ReplyProcessor::operationUnsupported, operationUnsupported);
        }

        @Override
        public void keepAlive(WireCommands.KeepAlive keepAlive) {
            execute(ReplyProcessor::keepAlive, keepAlive);
        }

        @Override
        public void connectionDropped() {
            execute(ReplyProcessor::connectionDropped);
        }

        @Override
        public void segmentPolicyUpdated(WireCommands.SegmentPolicyUpdated segmentPolicyUpdated) {
            execute(ReplyProcessor::segmentPolicyUpdated, segmentPolicyUpdated);
        }

        @Override
        public void processingFailure(Exception error) {
            execute(ReplyProcessor::processingFailure, error);
        }

        @Override
        public void authTokenCheckFailed(WireCommands.AuthTokenCheckFailed authTokenCheckFailed) {
            execute(ReplyProcessor::authTokenCheckFailed, authTokenCheckFailed);
        }

        @Override
        public void tableEntriesUpdated(WireCommands.TableEntriesUpdated tableEntriesUpdated) {
            execute(ReplyProcessor::tableEntriesUpdated, tableEntriesUpdated);
        }

        @Override
        public void tableKeysRemoved(WireCommands.TableKeysRemoved tableKeysRemoved) {
            execute(ReplyProcessor::tableKeysRemoved, tableKeysRemoved);
        }

        @Override
        public void tableRead(WireCommands.TableRead tableRead) {
            execute(ReplyProcessor::tableRead, tableRead);
        }

        @Override
        public void tableKeyDoesNotExist(WireCommands.TableKeyDoesNotExist tableKeyDoesNotExist) {
            execute(ReplyProcessor::tableKeyDoesNotExist, tableKeyDoesNotExist);
        }

        @Override
        public void tableKeyBadVersion(WireCommands.TableKeyBadVersion tableKeyBadVersion) {
            execute(ReplyProcessor::tableKeyBadVersion, tableKeyBadVersion);
        }

        @Override
        public void tableKeysRead(WireCommands.TableKeysRead tableKeysRead) {
            execute(ReplyProcessor::tableKeysRead, tableKeysRead);
        }

        @Override
        public void tableEntriesRead(WireCommands.TableEntriesRead tableEntriesRead) {
            execute(ReplyProcessor::tableEntriesRead, tableEntriesRead);
        }

        @Override
        public void tableEntriesDeltaRead(WireCommands.TableEntriesDeltaRead tableEntriesDeltaRead) {
            execute(ReplyProcessor::tableEntriesDeltaRead, tableEntriesDeltaRead);
        }

        @Override
        public void errorMessage(WireCommands.ErrorMessage errorMessage) {
            execute(ReplyProcessor::errorMessage, errorMessage);
        }
    }
}
