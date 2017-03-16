/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.stream.impl.segment;

import com.emc.pravega.common.Exceptions;
import com.emc.pravega.common.netty.Append;
import com.emc.pravega.common.netty.ConnectionFailedException;
import com.emc.pravega.common.netty.FailingReplyProcessor;
import com.emc.pravega.common.netty.PravegaNodeUri;
import com.emc.pravega.common.netty.WireCommands.AppendSetup;
import com.emc.pravega.common.netty.WireCommands.ConditionalCheckFailed;
import com.emc.pravega.common.netty.WireCommands.DataAppended;
import com.emc.pravega.common.netty.WireCommands.KeepAlive;
import com.emc.pravega.common.netty.WireCommands.NoSuchSegment;
import com.emc.pravega.common.netty.WireCommands.SegmentIsSealed;
import com.emc.pravega.common.netty.WireCommands.SetupAppend;
import com.emc.pravega.common.netty.WireCommands.WrongHost;
import com.emc.pravega.common.util.Retry;
import com.emc.pravega.common.util.Retry.RetryWithBackoff;
import com.emc.pravega.common.util.ReusableLatch;
import com.emc.pravega.stream.impl.Controller;
import com.emc.pravega.stream.impl.PendingEvent;
import com.emc.pravega.stream.impl.netty.ClientConnection;
import com.emc.pravega.stream.impl.netty.ConnectionFactory;
import com.google.common.annotations.VisibleForTesting;
import io.netty.buffer.Unpooled;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import javax.annotation.concurrent.GuardedBy;
import lombok.RequiredArgsConstructor;
import lombok.Synchronized;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

import static com.emc.pravega.common.Exceptions.handleInterrupted;
import static com.emc.pravega.common.concurrent.FutureHelpers.getAndHandleExceptions;
import static com.google.common.base.Preconditions.checkState;

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
    private final String segmentName;
    private final Controller controller;
    private final ConnectionFactory connectionFactory;
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
        private final ReusableLatch inflightEmpty = new ReusableLatch(true);

        /**
         * Blocks until there are no more messages inflight. (No locking required)
         */
        private void waitForEmptyInflight() {
            handleInterrupted(() -> inflightEmpty.await());
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
            log.warn("Connection failed due to", e);
            ClientConnection oldConnection;
            synchronized (lock) {
                if (exception == null) {
                    exception = e;
                }
                oldConnection = connection;
                connection = null;
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
                inflightEmpty.reset();
                inflight.put(eventNumber, event);
                return eventNumber;
            }
        }
        
        private PendingEvent removeSingleInflight(long inflightEventNumber) {
            synchronized (lock) {
                return inflight.remove(inflightEventNumber);
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
                if (inflight.isEmpty()) {
                    inflightEmpty.release();
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
            try {
                retransmitInflight();
                state.connectionSetupComplete();
            } catch (ConnectionFailedException e) {
                state.failConnection(e);
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

        private void retransmitInflight() throws ConnectionFailedException {
            for (Entry<Long, PendingEvent> entry : state.getAllInflight()) {
                state.connection.send(new Append(segmentName,
                                                 connectionId,
                                                 entry.getKey(),
                                                 Unpooled.wrappedBuffer(entry.getValue().getData()),
                                                 entry.getValue().getExpectedOffset()));
            }
        }
    }
    
    /**
     * @see com.emc.pravega.stream.impl.segment.SegmentOutputStream#write(java.nio.ByteBuffer,
     *      java.util.concurrent.CompletableFuture)
     */
    @Override
    @Synchronized
    public void write(PendingEvent event) throws SegmentSealedException {
        ClientConnection connection = getConnection();
        long eventNumber = state.addToInflight(event);
        try {
            connection.send(new Append(segmentName,
                                       connectionId,
                                       eventNumber,
                                       Unpooled.wrappedBuffer(event.getData()),
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
            SetupAppend cmd = new SetupAppend(connectionId, segmentName);
            connection.send(cmd);
        }
    }

    /**
     * @see com.emc.pravega.stream.impl.segment.SegmentOutputStream#close()
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
     * @see com.emc.pravega.stream.impl.segment.SegmentOutputStream#flush()
     */
    @Override
    @Synchronized
    public void flush() throws SegmentSealedException {
        try {
            ClientConnection connection = getConnection();
            connection.send(new KeepAlive());
            state.waitForEmptyInflight();
        } catch (ConnectionFailedException e) {
            state.failConnection(e);
        }
    }

    @Override
    public Collection<PendingEvent> getUnackedEvents() {
        return Collections.unmodifiableCollection(state.getAllInflightEvents());
    }

}
