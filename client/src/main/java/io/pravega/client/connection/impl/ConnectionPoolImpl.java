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

import static io.pravega.shared.metrics.MetricNotifier.NO_OP_METRIC_NOTIFIER;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import javax.annotation.concurrent.GuardedBy;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

import io.pravega.client.ClientConfig;
import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.Futures;
import io.pravega.shared.metrics.ClientMetricUpdater;
import io.pravega.shared.metrics.MetricListener;
import io.pravega.shared.metrics.MetricNotifier;
import io.pravega.shared.protocol.netty.ConnectionFailedException;
import io.pravega.shared.protocol.netty.PravegaNodeUri;
import io.pravega.shared.protocol.netty.ReplyProcessor;
import lombok.Data;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ConnectionPoolImpl implements ConnectionPool {

    /**
     * This class represents a Connection that is established with a Segment Store instance and its
     * attributes. (e.g: FlowCount, WriterCount)
     */
    @Data
    private class Connection implements Comparable<Connection>, AutoCloseable {
        private final PravegaNodeUri uri;
        /**
         * A future that completes when the connection is first established.
         */
        private final CompletableFuture<FlowHandler> flowHandler;

        int getFlowCount() {
            return Futures.isSuccessful(flowHandler) ? flowHandler.join().getOpenFlowCount() : 0;
        }

        boolean isConnected() {
            if (!Futures.isSuccessful(flowHandler)) {
                return false;
            }
            return !flowHandler.join().isClosed();
        }

        @Override
        public int compareTo(Connection o) {
            int v1 = Futures.isSuccessful(this.getFlowHandler()) ? this.getFlowCount() : Integer.MAX_VALUE;
            int v2 = Futures.isSuccessful(o.getFlowHandler()) ? o.getFlowCount() : Integer.MAX_VALUE;
            return Integer.compare(v1, v2);
        }
        
        @Override
        public void close() {
            if (Futures.isSuccessful(flowHandler)) {
                flowHandler.join().close();
            }
        }
    }

    private final Object lock = new Object();
    @VisibleForTesting
    @Getter
    private final ClientConfig clientConfig;
    private final MetricNotifier metricNotifier;
    private final AtomicBoolean closed = new AtomicBoolean(false);
    @GuardedBy("lock")
    private final Map<PravegaNodeUri, List<Connection>> connectionMap = new HashMap<>();
    private final ConnectionFactory connectionFactory;

    public ConnectionPoolImpl(ClientConfig clientConfig, ConnectionFactory connectionFactory) {
        this.clientConfig = clientConfig;
        this.connectionFactory = connectionFactory;
        MetricListener metricListener = clientConfig.getMetricListener();
        this.metricNotifier = metricListener == null ? NO_OP_METRIC_NOTIFIER : new ClientMetricUpdater(metricListener);
    }

    @Override
    public CompletableFuture<ClientConnection> getClientConnection(Flow flow, PravegaNodeUri location, ReplyProcessor rp) {
        Exceptions.checkNotClosed(closed.get(), this);
        Preconditions.checkNotNull(flow, "Flow");
        Preconditions.checkNotNull(location, "Location");
        Preconditions.checkNotNull(rp, "ReplyProcessor");
        synchronized (lock) {
            Exceptions.checkNotClosed(closed.get(), this);
            final List<Connection> connectionList = connectionMap.getOrDefault(location, new ArrayList<>());

            // remove connections for which the underlying network connection is disconnected.
            List<Connection> prunedConnectionList = connectionList.stream().filter(connection -> {
                // Filter out Connection objects which have been completed exceptionally or have been disconnected.
                return !connection.getFlowHandler().isDone() || connection.isConnected();
            }).collect(Collectors.toList());
            log.debug("List of connections to {} that can be used: {}", location, prunedConnectionList);

            // Choose the connection with the least number of flows.
            Optional<Connection> suggestedConnection = prunedConnectionList.stream().min(Comparator.naturalOrder());

            final Connection connection;
            if (suggestedConnection.isPresent() && (prunedConnectionList.size() >= clientConfig.getMaxConnectionsPerSegmentStore() || isUnused(suggestedConnection.get()))) {
                log.trace("Reusing connection: {}", suggestedConnection.get());
                connection = suggestedConnection.get();
            } else {
                // create a new connection.
                log.trace("Creating a new connection to {}", location);
                CompletableFuture<FlowHandler> establishedFuture = establishConnection(location);
                connection = new Connection(location, establishedFuture);
                prunedConnectionList.add(connection);
            }
            connectionMap.put(location, prunedConnectionList);
            return connection.getFlowHandler().thenApply(flowHandler -> flowHandler.createFlow(flow, rp));
        }
    }

    @Override
    public CompletableFuture<ClientConnection> getClientConnection(PravegaNodeUri location, ReplyProcessor rp) {
        Preconditions.checkNotNull(location, "Location");
        Preconditions.checkNotNull(rp, "ReplyProcessor");
        Exceptions.checkNotClosed(closed.get(), this);

        // create a new connection.
        CompletableFuture<FlowHandler> handler = establishConnection(location);
        Connection connection = new Connection(location, handler);
        return connection.getFlowHandler().thenApply(h -> h.createConnectionWithFlowDisabled(rp));
    }

    @Override
    public void getClientConnection(Flow flow, PravegaNodeUri location, ReplyProcessor rp, CompletableFuture<ClientConnection> connection) {
        getClientConnection(flow, location, rp).thenApply(connection::complete).exceptionally( e -> connection.completeExceptionally(new ConnectionFailedException(e)));
    }

    private static boolean isUnused(Connection connection) {
        return Futures.isSuccessful(connection.getFlowHandler()) && connection.getFlowCount() == 0;
    }

    /**
     * Used only for testing.
     */
    @VisibleForTesting
    public void pruneUnusedConnections() {
        synchronized (lock) {
            for (List<Connection> connections : connectionMap.values()) {
                for (Iterator<Connection> iterator = connections.iterator(); iterator.hasNext(); ) {
                    Connection connection = iterator.next();
                    if (isUnused(connection)) {
                        connection.getFlowHandler().join().close();
                        iterator.remove();
                    }
                }
            }
        }
    }

    @VisibleForTesting
    public List<Connection> getActiveChannels() {
        synchronized (lock) {
            ArrayList<Connection> result = new ArrayList<Connection>();
            for (List<Connection> connection : this.connectionMap.values()) {
                result.addAll(connection);
            }
            return result;
        }
    }

    /**
     * Establish a new connection to the Pravega Node.
     * @param location The Pravega Node Uri
     * @return A future, which completes once the connection has been established, returning a FlowHandler that can be used to create
     * flows on the connection.
     */
    private CompletableFuture<FlowHandler> establishConnection(PravegaNodeUri location) {
        return FlowHandler.openConnection(location, metricNotifier, connectionFactory);
    }

    @Override
    public void close() {
        log.info("Shutting down connection pool");
        if (closed.compareAndSet(false, true)) {
            metricNotifier.close();
            connectionFactory.close();
            synchronized (lock) {
                for (List<Connection> connections : connectionMap.values()) {
                    for (Connection connection : connections) {
                        connection.close();
                    }
                }
                connectionMap.clear();
            }
        }
    }

    @Override
    public ScheduledExecutorService getInternalExecutor() {
        return connectionFactory.getInternalExecutor();
    }
}
