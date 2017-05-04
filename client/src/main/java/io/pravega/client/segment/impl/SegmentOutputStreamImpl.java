/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
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

import com.google.common.annotations.VisibleForTesting;
import io.netty.buffer.Unpooled;
import io.pravega.client.netty.impl.ClientConnection;
import io.pravega.client.netty.impl.ConnectionFactory;
import io.pravega.client.stream.impl.Controller;
import io.pravega.client.stream.impl.PendingEvent;
import io.pravega.common.Exceptions;
import io.pravega.shared.protocol.netty.Append;
import io.pravega.shared.protocol.netty.ConnectionFailedException;
import io.pravega.shared.protocol.netty.FailingReplyProcessor;
import io.pravega.shared.protocol.netty.PravegaNodeUri;
import io.pravega.shared.protocol.netty.WireCommands.AppendSetup;
import io.pravega.shared.protocol.netty.WireCommands.ConditionalCheckFailed;
import io.pravega.shared.protocol.netty.WireCommands.DataAppended;
import io.pravega.shared.protocol.netty.WireCommands.KeepAlive;
import io.pravega.shared.protocol.netty.WireCommands.NoSuchSegment;
import io.pravega.shared.protocol.netty.WireCommands.SegmentIsSealed;
import io.pravega.shared.protocol.netty.WireCommands.SetupAppend;
import io.pravega.shared.protocol.netty.WireCommands.WrongHost;
import io.pravega.common.util.Retry;
import io.pravega.common.util.Retry.RetryWithBackoff;
import io.pravega.common.util.ReusableLatch;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import javax.annotation.concurrent.GuardedBy;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Synchronized;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

import static com.google.common.base.Preconditions.checkState;
import static io.pravega.common.concurrent.FutureHelpers.getAndHandleExceptions;

/**
 * Tracks inflight events, and manages reconnects automatically.
 * 
 * @see SegmentOutputStream
 */
@RequiredArgsConstructor
@Slf4j
@ToString(of = {"segmentName", "connectionId", "state"})
class SegmentOutputStreamImpl implements SegmentOutputStream {

    private static final RetryWithBackoff RETRY_SCHEDULE = Retry.withExpBackoff(1, 10, 5);
    @Getter
    private final String segmentName;
    private final Controller controller;
    private final ConnectionFactory connectionFactory;
    private final Supplier<Long> requestIdGenerator = new AtomicLong(0)::incrementAndGet;
    private final UUID connectionId;
    private final State state = new State();
    private final ResponseProcessor responseProcessor = new ResponseProcessor();

    /**
     * Internal object that tracks the state of the connection.
     * All mutations of data occur inside of this class. All operations are protected by the lock object.
     * No calls to external classes occur. No network calls occur via any methods in this object.
     */
    @ToString(of = {"closed", "exception", "eventNumber"})
    private static final class State {
        private final Object lock = new Object();
        @GuardedBy("lock")
        private boolean closed = false;
        @GuardedBy("lock")
        private ClientConnection connection;
        @GuardedBy("lock")
        private Exception exception = null;
        @GuardedBy("lock")
        private final ConcurrentSkipListMap<Long, PendingEvent> inflight = new ConcurrentSkipListMap<>();
        @GuardedBy("lock")
        private long eventNumber = 0;
        private final ReusableLatch connectionSetup = new ReusableLatch();
        @GuardedBy("lock")
        private CompletableFuture<Void> emptyInflightFuture = null;

        /**
         * Returns a future that will complete successfully once all the inflight events are acked
         * and will fail if that is not possible for some reason. IE: because the connection
         * dropped, or the segment was sealed.
         */
        private CompletableFuture<Void> getEmptyInflightFuture() {
            synchronized (lock) {
                if (emptyInflightFuture == null) {
                    emptyInflightFuture = new CompletableFuture<Void>();
                    if (inflight.isEmpty()) {
                        emptyInflightFuture.complete(null);
                    }
                }
                return emptyInflightFuture;
            }
        }

        private boolean isInflightEmpty() {
            synchronized (lock) {
                return inflight.isEmpty();
            }
        }

        private void connectionSetupComplete() {
            connectionSetup.release();
        }

        /**
         * @return The current connection (May be null if not connected)
         */
        private ClientConnection getConnection() {
            synchronized (lock) {
                return connection;
            }
        }

        /**
         * @param newConnection The new connection that has been established that should used going forward.
         */
        private void newConnection(ClientConnection newConnection) {
            synchronized (lock) {
                connectionSetup.reset();
                exception = null;
                connection = newConnection;
            }
        }

        /**
         * @param e Error that has occurred that needs to be handled by tearing down the connection.
         */
        private void failConnection(Exception e) {
            ClientConnection oldConnection;
            synchronized (lock) {
                if (exception == null) {
                    exception = e;
                }
                oldConnection = connection;
                connection = null;
                if (!closed) {
                    log.warn("Connection failed due to: {}", e.getMessage());
                }
                if (emptyInflightFuture != null) {
                    emptyInflightFuture.completeExceptionally(e);
                }
            }
            connectionSetupComplete();
            if (oldConnection != null) {
                oldConnection.close();
            }
        }

        /**
         * Block until a connection has been established and AppendSetup has come back from the server.
         */
        private ClientConnection waitForConnection() throws ConnectionFailedException, SegmentSealedException {
            try {
                Exceptions.handleInterrupted(() -> connectionSetup.await());
                synchronized (lock) {
                    if (exception != null) {
                        throw exception;
                    }
                    return connection;
                }
            } catch (ConnectionFailedException | IllegalArgumentException | SegmentSealedException e) {
                throw e;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        /**
         * Add event to the infight
         * @return The EventNumber for the event.
         */
        private long addToInflight(PendingEvent event) {
            synchronized (lock) {
                eventNumber++;
                inflight.put(eventNumber, event);
                if (emptyInflightFuture != null && emptyInflightFuture.isDone()) {
                    emptyInflightFuture = null;
                }
                return eventNumber;
            }
        }
        
        private PendingEvent removeSingleInflight(long inflightEventNumber) {
            synchronized (lock) {
                PendingEvent result = inflight.remove(inflightEventNumber);
                if (emptyInflightFuture != null && inflight.isEmpty()) {
                    emptyInflightFuture.complete(null);
                }
                return result;
            }
        }
        
        /**
         * Remove all events with event numbers below the provided level from inflight and return them.
         */
        private List<PendingEvent> removeInflightBelow(long ackLevel) {
            synchronized (lock) {
                ConcurrentNavigableMap<Long, PendingEvent> acked = inflight.headMap(ackLevel, true);
                List<PendingEvent> result = new ArrayList<>(acked.values());
                acked.clear();
                if (emptyInflightFuture != null && inflight.isEmpty()) {
                    emptyInflightFuture.complete(null);
                }
                return result;
            }
        }

        private List<Map.Entry<Long, PendingEvent>> getAllInflight() {
            synchronized (lock) {
                return new ArrayList<>(inflight.entrySet());
            }
        }
        
        private List<PendingEvent> getAllInflightEvents() {
            synchronized (lock) {
                return new ArrayList<>(inflight.values());
            }
        }

        private boolean isClosed() {
            synchronized (lock) {
                return closed;
            }
        }

        private void setClosed(boolean closed) {
            synchronized (lock) {
                this.closed = closed;
            }
        }
    }

    private final class ResponseProcessor extends FailingReplyProcessor {
        @Override
        public void connectionDropped() {
            state.failConnection(new ConnectionFailedException()); 
        }
        
        @Override
        public void wrongHost(WrongHost wrongHost) {
            state.failConnection(new ConnectionFailedException()); // TODO: Probably something else.
        }

        @Override
        public void segmentIsSealed(SegmentIsSealed segmentIsSealed) {
            state.failConnection(new SegmentSealedException());
        }

        @Override
        public void noSuchSegment(NoSuchSegment noSuchSegment) {
            state.failConnection(new IllegalArgumentException(noSuchSegment.toString()));
        }
        
        @Override
        public void dataAppended(DataAppended dataAppended) {
            long ackLevel = dataAppended.getEventNumber();
            ackUpTo(ackLevel);
        }
        
        @Override
        public void conditionalCheckFailed(ConditionalCheckFailed dataNotAppended) {
            long eventNumber = dataNotAppended.getEventNumber();
            conditionalFail(eventNumber);
        }

        @Override
        public void appendSetup(AppendSetup appendSetup) {
            long ackLevel = appendSetup.getLastEventNumber();
            ackUpTo(ackLevel);
            List<Append> toRetransmit = state.getAllInflight()
                                             .stream()
                                             .map(entry -> new Append(segmentName, connectionId, entry.getKey(),
                                                                      Unpooled.wrappedBuffer(entry.getValue()
                                                                                                  .getData()),
                                                                      entry.getValue().getExpectedOffset()))
                                             .collect(Collectors.toList());
            if (toRetransmit == null || toRetransmit.isEmpty()) {
                state.connectionSetupComplete();
            } else {
                state.getConnection().sendAsync(toRetransmit, e -> {
                    if (e == null) {
                        state.connectionSetupComplete();
                    } else {
                        state.failConnection(e);
                    }
                });
            }
        }

        private void ackUpTo(long ackLevel) {
            for (PendingEvent toAck : state.removeInflightBelow(ackLevel)) {
                if (toAck != null) {
                    toAck.getAckFuture().complete(true);
                }
            }
        }
        
        private void conditionalFail(long eventNumber) {
            PendingEvent toAck = state.removeSingleInflight(eventNumber);
            if (toAck != null) {
                toAck.getAckFuture().complete(false);
            }
        }

        @Override
        public void processingFailure(Exception error) {
            state.failConnection(error);
        }
    }
    
    /**
     * @see SegmentOutputStream#write(java.nio.ByteBuffer,
     *      java.util.concurrent.CompletableFuture)
     */
    @Override
    @Synchronized
    public void write(PendingEvent event) throws SegmentSealedException {
        ClientConnection connection = getConnection();
        long eventNumber = state.addToInflight(event);
        try {
            connection.send(new Append(segmentName, connectionId, eventNumber, Unpooled.wrappedBuffer(event.getData()),
                                       event.getExpectedOffset()));
        } catch (ConnectionFailedException e) {
            log.warn("Connection failed due to: ", e);
            getConnection(); // As the messages is inflight, this will perform the retransmition.
        }
    }
    
    /**
     * Blocking call to establish a connection and wait for it to be setup. (Retries built in)
     */
    @Synchronized
    ClientConnection getConnection() throws SegmentSealedException {
        checkState(!state.isClosed(), "LogOutputStream was already closed");
        return RETRY_SCHEDULE.retryingOn(ConnectionFailedException.class).throwingOn(SegmentSealedException.class).run(() -> {
            setupConnection();
            return state.waitForConnection();
        });
    }

    @Synchronized
    @VisibleForTesting
    void setupConnection() throws ConnectionFailedException {
        if (state.getConnection() == null) {
            CompletableFuture<ClientConnection> newConnection = controller.getEndpointForSegment(segmentName)
                .thenCompose((PravegaNodeUri uri) -> {
                    return connectionFactory.establishConnection(uri, responseProcessor);
                });
            ClientConnection connection = getAndHandleExceptions(newConnection, ConnectionFailedException::new);
            state.newConnection(connection);
            SetupAppend cmd = new SetupAppend(requestIdGenerator.get(), connectionId, segmentName);
            connection.send(cmd);
        }
    }

    /**
     * @see SegmentOutputStream#close()
     */
    @Override
    @Synchronized
    public void close() throws SegmentSealedException {
        if (state.isClosed()) {
            return;
        }
        flush();
        state.setClosed(true);
        ClientConnection connection = state.getConnection();
        if (connection != null) {
            connection.close();
        }
    }

    /**
     * @see SegmentOutputStream#flush()
     */
    @Override
    @Synchronized
    public void flush() throws SegmentSealedException {
        if (!state.isInflightEmpty()) {
            RETRY_SCHEDULE.retryingOn(ConnectionFailedException.class)
                          .throwingOn(SegmentSealedException.class)
                          .run(() -> {
                              ClientConnection connection = getConnection();
                              connection.send(new KeepAlive());
                              state.getEmptyInflightFuture().get();
                              return null;
                          });
        }
    }

    @Override
    public Collection<PendingEvent> getUnackedEvents() {
        return Collections.unmodifiableCollection(state.getAllInflightEvents());
    }

}
