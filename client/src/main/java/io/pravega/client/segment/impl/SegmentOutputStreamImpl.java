/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.segment.impl;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import io.netty.buffer.Unpooled;
import io.pravega.client.netty.impl.ClientConnection;
import io.pravega.client.netty.impl.ConnectionFactory;
import io.pravega.client.stream.impl.Controller;
import io.pravega.client.stream.impl.PendingEvent;
import io.pravega.common.ExceptionHelpers;
import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.FutureHelpers;
import io.pravega.common.util.Retry;
import io.pravega.common.util.Retry.RetryWithBackoff;
import io.pravega.common.util.ReusableFutureLatch;
import io.pravega.common.util.ReusableLatch;
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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import javax.annotation.concurrent.GuardedBy;
import lombok.Getter;
import lombok.Lombok;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;


/**
 * Tracks inflight events, and manages reconnects automatically.
 * 
 * @see SegmentOutputStream
 */
@RequiredArgsConstructor
@Slf4j
@ToString(of = {"segmentName", "writerId", "state"})
class SegmentOutputStreamImpl implements SegmentOutputStream {

    @Getter
    private final String segmentName;
    private final Controller controller;
    private final ConnectionFactory connectionFactory;
    private final Supplier<Long> requestIdGenerator = new AtomicLong(0)::incrementAndGet;
    private final UUID writerId;
    private final Consumer<Segment> callBackForSealed;
    private final State state = new State();
    private final ResponseProcessor responseProcessor = new ResponseProcessor();
    private final RetryWithBackoff retrySchedule;
    private final Object connectionEstablishmentLock = new Object();
    private final Object writeOrderLock = new Object();
    
    /**
     * Internal object that tracks the state of the connection.
     * All mutations of data occur inside of this class. All operations are protected by the lock object.
     * No calls to external classes occur. No network calls occur via any methods in this object.
     */
    @ToString(of = {"closed", "exception", "eventNumber"})
    private final class State {
        private final Object lock = new Object();
        @GuardedBy("lock")
        private boolean closed = false;
        @GuardedBy("lock")
        private ClientConnection connection;
        @GuardedBy("lock")
        private CompletableFuture<Void> connectionSetupCompleted;
        @GuardedBy("lock")
        private Throwable exception = null;
        @GuardedBy("lock")
        private final ConcurrentSkipListMap<Long, PendingEvent> inflight = new ConcurrentSkipListMap<>();
        @GuardedBy("lock")
        private long eventNumber = 0;
        private final ReusableFutureLatch<ClientConnection> setupConnection = new ReusableFutureLatch<>();
        private final ReusableLatch waitingInflight = new ReusableLatch(true);
        private final AtomicBoolean sealEncountered = new AtomicBoolean();

        /**
         * Block until all events are acked by the server.
         */
        private void waitForInflight() {
           Exceptions.handleInterrupted(() -> waitingInflight.await());
        }

        private boolean isAlreadySealed() {
            synchronized (lock) {
                return connection == null && exception != null && exception instanceof SegmentSealedException;
            }
        }

        private boolean isInflightEmpty() {
            synchronized (lock) {
                return inflight.isEmpty();
            }
        }

        private void connectionSetupComplete() {
            CompletableFuture<Void> toComplete;
            synchronized (lock) {
                toComplete = connectionSetupCompleted;
            }
            if (toComplete != null) {
                toComplete.complete(null);
                setupConnection.release(connection);
            }
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
         * @return Returns a future that will complete when setup is finished or fail if it cannot be.
         */
        private CompletableFuture<Void> newConnection(ClientConnection newConnection) {
            CompletableFuture<Void> result = new CompletableFuture<Void>();
            synchronized (lock) {
                connectionSetupCompleted = result;
                connection = newConnection;
                exception = null;
            }
            return result;
        }

        /**
         * @param throwable Error that has occurred that needs to be handled by tearing down the connection.
         */
        private void failConnection(Throwable throwable) {
            ClientConnection oldConnection = null;
            CompletableFuture<Void> oldConnectionSetupCompleted = null;
            boolean failSetupConnection = false;
            synchronized (lock) {
                if (connection != null ) {
                    if (connectionSetupCompleted.isDone()) {
                        failSetupConnection = true;
                    } else {
                        oldConnectionSetupCompleted = connectionSetupCompleted;
                    }
                    oldConnection = connection;
                }
                if (exception == null) {
                    exception = throwable;
                }
                connection = null;
                connectionSetupCompleted = null;
                if (closed || throwable instanceof SegmentSealedException) {
                    waitingInflight.release();
                } 
                if (!closed) {
                    log.warn("Connection for segment {} failed due to: {}", segmentName, throwable.getMessage());
                }
            }
            if (failSetupConnection) {
                setupConnection.releaseExceptionallyAndReset(throwable);                
            }
            if (oldConnectionSetupCompleted != null) {
                oldConnectionSetupCompleted.completeExceptionally(throwable);
            }
            if (oldConnection != null) {
                oldConnection.close();
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
                waitingInflight.reset();
                return eventNumber;
            }
        }
        
        private PendingEvent removeSingleInflight(long inflightEventNumber) {
            synchronized (lock) {
                PendingEvent result = inflight.remove(inflightEventNumber);
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
                return result;
            }
        }

        private void releaseIfEmptyInflight() {
            synchronized (lock) {
                if (inflight.isEmpty()) {
                    waitingInflight.release();
                }
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
            failConnection(new ConnectionFailedException());
        }
        
        @Override
        public void wrongHost(WrongHost wrongHost) {
            failConnection(new ConnectionFailedException());
        }

        /**
         * Invariants for segment sealed:
         *   - SegmentSealed callback and write will not run concurrently.
         *   - During the execution of the call back no new writes will be executed.
         * Once the segment Sealed callback is executed successfully
         *  - there will be no new writes to this segment.
         *  - any write to this segment will throw a SegmentSealedException.
         *  - any thread waiting on state.getEmptyInflightFuture() will get SegmentSealedException.
         * @param segmentIsSealed SegmentIsSealed WireCommand.
         */
        @Override
        public void segmentIsSealed(SegmentIsSealed segmentIsSealed) {
            log.info("Received SegmentSealed {}", segmentIsSealed);
            if (state.sealEncountered.compareAndSet(false, true)) {
                Retry.indefinitelyWithExpBackoff(retrySchedule.getInitialMillis(), retrySchedule.getMultiplier(),
                                                 retrySchedule.getMaxDelay(),
                                                 t -> log.error(writerId + " to invoke sealed callback: ", t))
                     .runInExecutor(() -> {
                         log.debug("Invoking SealedSegment call back for {}", segmentIsSealed);
                         callBackForSealed.accept(Segment.fromScopedName(getSegmentName()));
                     }, connectionFactory.getInternalExecutor());
            }
        }

        @Override
        public void noSuchSegment(NoSuchSegment noSuchSegment) {
            String segment = noSuchSegment.getSegment();
            checkArgument(segmentName.equals(segment), "Wrong segment name %s, %s", segmentName, segment);
            log.info("Segment being written to {} no longer exists failing all writes", segment);
            state.setClosed(true);
            ClientConnection connection = state.getConnection();
            if (connection != null) {
                connection.close();
            }
            NoSuchSegmentException exception = new NoSuchSegmentException(segment);
            for (PendingEvent toAck : state.removeInflightBelow(Long.MAX_VALUE)) {
                if (toAck != null) {
                    toAck.getAckFuture().completeExceptionally(exception);
                }
            }
            state.releaseIfEmptyInflight();
        }
        
        @Override
        public void dataAppended(DataAppended dataAppended) {
            log.trace("Received ack: {}", dataAppended);
            long ackLevel = dataAppended.getEventNumber();
            ackUpTo(ackLevel);
        }
        
        @Override
        public void conditionalCheckFailed(ConditionalCheckFailed dataNotAppended) {
            log.debug("Received ConditionalCheckFailed: {}", dataNotAppended);
            long eventNumber = dataNotAppended.getEventNumber();
            conditionalFail(eventNumber);
        }

        @Override
        public void appendSetup(AppendSetup appendSetup) {
            log.info("Received AppendSetup {}", appendSetup);
            long ackLevel = appendSetup.getLastEventNumber();
            ackUpTo(ackLevel);
            List<Append> toRetransmit = state.getAllInflight()
                                             .stream()
                                             .map(entry -> new Append(segmentName, writerId, entry.getKey(),
                                                                      Unpooled.wrappedBuffer(entry.getValue()
                                                                                                  .getData()),
                                                                      entry.getValue().getExpectedOffset()))
                                             .collect(Collectors.toList());
            if (toRetransmit == null || toRetransmit.isEmpty()) {
                log.info("Connection setup complete for writer {}", writerId);
                state.connectionSetupComplete();
            } else {
                state.getConnection().sendAsync(toRetransmit, e -> {
                    if (e == null) {
                        state.connectionSetupComplete();
                    } else {
                        failConnection(e);
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
            state.releaseIfEmptyInflight();
        }
        
        private void conditionalFail(long eventNumber) {
            PendingEvent toAck = state.removeSingleInflight(eventNumber);
            if (toAck != null) {
                toAck.getAckFuture().complete(false);
            }
            state.releaseIfEmptyInflight();
        }

        @Override
        public void processingFailure(Exception error) {
            failConnection(error);
        }
    }

    /**
     * @see SegmentOutputStream#write(PendingEvent)
     *
     */
    @Override
    public void write(PendingEvent event) {
        checkState(!state.isAlreadySealed(), "Segment: {} is already sealed", segmentName);
        synchronized (writeOrderLock) {
            ClientConnection connection;
            try {
                // if connection is null getConnection() establishes a connection and retransmits all events in inflight
                // list.
                connection = FutureHelpers.getThrowingException(getConnection());
            } catch (SegmentSealedException e) {
                // Add the event to inflight and indicate to the caller that the segment is sealed.
                state.addToInflight(event);
                return;
            }
            long eventNumber = state.addToInflight(event);
            try {
                Append append = new Append(segmentName, writerId, eventNumber, Unpooled.wrappedBuffer(event.getData()),
                        event.getExpectedOffset());
                log.trace("Sending append request: {}", append);
                connection.send(append);
            } catch (ConnectionFailedException e) {
                log.warn("Connection " + writerId + " failed due to: ", e);
                reconnect(); // As the message is inflight, this will perform the retransmission.
            }
        }
    }
    
    /**
     * Establish a connection and wait for it to be setup. (Retries built in)
     */
    CompletableFuture<ClientConnection> getConnection() throws SegmentSealedException {
        checkState(!state.isClosed(), "LogOutputStream was already closed");
        if (state.isAlreadySealed()) {
            throw new SegmentSealedException(this.segmentName);
        }
        if (state.getConnection() == null) {
            reconnect();
        }
        CompletableFuture<ClientConnection> future =  new CompletableFuture<>();
        state.setupConnection.register(future);
        return future;
    }
    
    /**
     * @see SegmentOutputStream#close()
     */
    @Override
    public void close() throws SegmentSealedException {
        if (state.isClosed()) {
            return;
        }
        log.debug("Closing writer: {}", writerId);
        // Wait until all the inflight events are written
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
    public void flush() throws SegmentSealedException {
        if (!state.isInflightEmpty()) {
            log.debug("Flushing writer: {}", writerId);
            try {
                ClientConnection connection = FutureHelpers.getThrowingException(getConnection());
                connection.send(new KeepAlive());
            } catch (Exception e) {
                failConnection(e);
            }
            state.waitForInflight();
            Exceptions.checkNotClosed(state.isClosed(), this);
            if (state.isAlreadySealed()) {
                throw new SegmentSealedException(segmentName + " sealed");
            }
        }
    }
    
    private void failConnection(Throwable e) {
        state.failConnection(ExceptionHelpers.getRealException(e));
        reconnect();
    }
    
    @VisibleForTesting
    void reconnect() {
        if (state.isClosed()) {
            return;
        }
        log.debug("(Re)connect invoked, Segment: {}, writerID: {}", segmentName, writerId);
        state.setupConnection.registerAndRunReleaser(() -> {
            Retry.indefinitelyWithExpBackoff(retrySchedule.getInitialMillis(), retrySchedule.getMultiplier(),
                                             retrySchedule.getMaxDelay(),
                                             t -> log.warn(writerId + " Failed to connect: ", t))
                 .runAsync(() -> {
                     log.debug("Running reconnect for segment:{} writerID: {}", segmentName, writerId);
                     if (state.isClosed() || state.isAlreadySealed()) {
                         return CompletableFuture.completedFuture(null);
                     }
                     Preconditions.checkState(state.getConnection() == null);
                     log.info("Fetching endpoint for segment {}, writerID: {}", segmentName, writerId);
                     return controller.getEndpointForSegment(segmentName).thenComposeAsync((PravegaNodeUri uri) -> {
                         log.info("Establishing connection to {} for {}, writerID: {}", uri, segmentName, writerId);
                         return connectionFactory.establishConnection(uri, responseProcessor);
                     }, connectionFactory.getInternalExecutor()).thenComposeAsync(connection -> {
                         CompletableFuture<Void> connectionSetupFuture = state.newConnection(connection);
                         SetupAppend cmd = new SetupAppend(requestIdGenerator.get(), writerId, segmentName);
                         try {
                             connection.send(cmd);
                         } catch (ConnectionFailedException e1) {
                             throw Lombok.sneakyThrow(e1);
                         }
                         return connectionSetupFuture.exceptionally(t -> {
                             if (ExceptionHelpers.getRealException(t) instanceof SegmentSealedException) {
                                 log.info("Ending reconnect attempts to {} because segment is sealed", segmentName);
                                 return null;
                             }
                             throw Lombok.sneakyThrow(t);
                         });
                     }, connectionFactory.getInternalExecutor());
                 }, connectionFactory.getInternalExecutor());
        }, new CompletableFuture<ClientConnection>());
    }



    /**
     * This function is invoked by SegmentSealedCallback, i.e., when SegmentSealedCallback or getUnackedEventsOnSeal()
     * is invoked there are no writes happening to the Segment.
     * @see SegmentOutputStream#getUnackedEventsOnSeal()
     *
     */
    @Override
    public List<PendingEvent> getUnackedEventsOnSeal() {
        // close connection and update the exception to SegmentSealed, this ensures future writes receive a
        // SegmentSealedException.
        log.trace("GetUnackedEventsOnSeal called on {}", writerId);
        synchronized (writeOrderLock) {   
            state.failConnection(new SegmentSealedException(this.segmentName));
            return Collections.unmodifiableList(state.getAllInflightEvents());
        }
    }
}
