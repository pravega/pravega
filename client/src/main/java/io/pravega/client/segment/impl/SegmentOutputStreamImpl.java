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
package io.pravega.client.segment.impl;

import static com.google.common.base.Preconditions.checkState;

import java.time.Duration;
import java.util.AbstractMap;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import javax.annotation.concurrent.GuardedBy;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

import io.pravega.auth.InvalidTokenException;
import io.pravega.auth.TokenExpiredException;
import io.pravega.client.ClientConfig;
import io.pravega.client.connection.impl.ClientConnection;
import io.pravega.client.connection.impl.ConnectionPool;
import io.pravega.client.connection.impl.Flow;
import io.pravega.client.security.auth.DelegationTokenProvider;
import io.pravega.client.control.impl.Controller;
import io.pravega.client.stream.impl.PendingEvent;
import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.util.RetriesExhaustedException;
import io.pravega.common.util.Retry;
import io.pravega.common.util.Retry.RetryWithBackoff;
import io.pravega.common.util.ReusableFutureLatch;
import io.pravega.common.util.ReusableLatch;
import io.pravega.shared.NameUtils;
import io.pravega.shared.protocol.netty.Append;
import io.pravega.shared.protocol.netty.ConnectionFailedException;
import io.pravega.shared.protocol.netty.FailingReplyProcessor;
import io.pravega.shared.protocol.netty.PravegaNodeUri;
import io.pravega.shared.protocol.netty.WireCommand;
import io.pravega.shared.protocol.netty.WireCommands;
import io.pravega.shared.protocol.netty.WireCommands.AppendSetup;
import io.pravega.shared.protocol.netty.WireCommands.DataAppended;
import io.pravega.shared.protocol.netty.WireCommands.KeepAlive;
import io.pravega.shared.protocol.netty.WireCommands.NoSuchSegment;
import io.pravega.shared.protocol.netty.WireCommands.SegmentIsSealed;
import io.pravega.shared.protocol.netty.WireCommands.SetupAppend;
import io.pravega.shared.protocol.netty.WireCommands.WrongHost;
import lombok.Getter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;


/**
 * Tracks inflight events, and manages reconnects automatically.
 * 
 * @see SegmentOutputStream
 */
@Slf4j
@ToString(of = {"segmentName", "writerId", "state"})
class SegmentOutputStreamImpl implements SegmentOutputStream {

    @Getter
    private final String segmentName;
    @VisibleForTesting
    @Getter
    private final boolean useConnectionPooling;
    private final Controller controller;
    private final ConnectionPool connectionPool;
    private final ClientConfig clientConfig;

    private final UUID writerId;
    private final Consumer<Segment> resendToSuccessorsCallback;
    private final State state = new State();
    private final ResponseProcessor responseProcessor = new ResponseProcessor();
    private final RetryWithBackoff retrySchedule;
    private final Object writeOrderLock = new Object();
    private final DelegationTokenProvider tokenProvider;
    @VisibleForTesting
    @Getter
    private final long requestId = Flow.create().asLong();

    @VisibleForTesting
    SegmentOutputStreamImpl(String segmentName, boolean useConnectionPooling, Controller controller,
                            ConnectionPool connectionPool, UUID writerId, Consumer<Segment> resendToSuccessorsCallback,
                            RetryWithBackoff retrySchedule, DelegationTokenProvider tokenProvider) {
        this(segmentName, useConnectionPooling, controller, connectionPool, writerId, resendToSuccessorsCallback,
             retrySchedule, tokenProvider, ClientConfig.builder().build());
    }

    SegmentOutputStreamImpl(String segmentName, boolean useConnectionPooling, Controller controller,
                            ConnectionPool connectionPool, UUID writerId, Consumer<Segment> resendToSuccessorsCallback,
                            RetryWithBackoff retrySchedule, DelegationTokenProvider tokenProvider,
                            ClientConfig clientConfig) {
        this.segmentName = segmentName;
        this.useConnectionPooling = useConnectionPooling;
        this.controller = controller;
        this.connectionPool = connectionPool;
        this.writerId = writerId;
        this.resendToSuccessorsCallback = resendToSuccessorsCallback;
        this.retrySchedule = retrySchedule;
        this.tokenProvider = tokenProvider;
        this.clientConfig = clientConfig;
    }

    /**
     * Internal object that tracks the state of the connection.
     * All mutations of data occur inside of this class. All operations are protected by the lock object.
     * No calls to external classes occur. No network calls occur via any methods in this object.
     * Note: In a failure scenario SegmentOutputStreamImpl.State#failConnection can be invoked before
     * SegmentOutputStreamImpl.State#newConnection is invoked as we do not want connection setup and teardown to occur
     * within the scope of the lock.
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
        private final ArrayDeque<Entry<Long, PendingEvent>> inflight = new ArrayDeque<>();
        @GuardedBy("lock")
        private long eventNumber = 0;
        @GuardedBy("lock")
        private long segmentLength = -1;
        private final ReusableFutureLatch<ClientConnection> setupConnection = new ReusableFutureLatch<>();
        private final ReusableLatch waitingInflight = new ReusableLatch(true);
        private final AtomicBoolean needSuccessors = new AtomicBoolean();

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

        private int getNumInflight() {
            synchronized (lock) {
                return inflight.size();
            }
        }

        private long getLastSegmentLength() {
            synchronized (lock) {
                return segmentLength;
            }
        }

        private void noteSegmentLength(long newLength) {
            synchronized (lock) {
                segmentLength = Math.max(segmentLength, newLength);
            }
        }

        private void connectionSetupComplete(ClientConnection connection) {
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
            CompletableFuture<Void> result = Futures.futureWithTimeout(Duration.ofMillis(clientConfig.getConnectTimeoutMilliSec()),
                    "Establishing connection to server",
                    connectionPool.getInternalExecutor());
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
                log.info("Handling exception {} for connection {} on writer {}. SetupCompleted: {}, Closed: {}",
                         throwable, connection, writerId, connectionSetupCompleted == null ? null : connectionSetupCompleted.isDone(), closed);
                if (exception == null || throwable instanceof RetriesExhaustedException) {
                    exception = throwable;
                }
                connection = null;
                connectionSetupCompleted = null;
                if (closed || throwable instanceof SegmentSealedException || throwable instanceof RetriesExhaustedException) {
                    waitingInflight.release();
                }
                if (!closed) {
                    String message = throwable.getMessage() == null ? throwable.getClass().toString() : throwable.getMessage();
                    log.warn("Connection for segment {} on writer {} failed due to: {}", segmentName, writerId, message);
                }
            }
            if (throwable instanceof SegmentSealedException || throwable instanceof NoSuchSegmentException
                    || throwable instanceof InvalidTokenException || throwable instanceof RetriesExhaustedException) {
                setupConnection.releaseExceptionally(throwable);
            } else if (failSetupConnection) {
                setupConnection.releaseExceptionallyAndReset(throwable);
            }
            if (oldConnection != null) {
                oldConnection.close();
            }
            if (oldConnectionSetupCompleted != null) {
                oldConnectionSetupCompleted.completeExceptionally(throwable);
            }
        }

        /**
         * Add event to the infight
         * @return The EventNumber for the event.
         */
        private long addToInflight(PendingEvent event) {
            synchronized (lock) {
                eventNumber += event.getEventCount();
                log.trace("Adding event {} to inflight on writer {}", eventNumber, writerId);
                inflight.addLast(new SimpleImmutableEntry<>(eventNumber, event));
                if (!needSuccessors.get()) {
                    waitingInflight.reset();
                }
                return eventNumber;
            }
        }

        /**
         * Remove all events with event numbers below the provided level from inflight and return them.
         */
        private List<PendingEvent> removeInflightBelow(long ackLevel) {
            synchronized (lock) {
                List<PendingEvent> result = new ArrayList<>();
                Entry<Long, PendingEvent> entry = inflight.peekFirst();
                while (entry != null && entry.getKey() <= ackLevel) {
                    inflight.pollFirst();
                    result.add(entry.getValue());
                    entry = inflight.peekFirst();
                }
                releaseIfEmptyInflight(); // release waitingInflight under the same re-entrant lock.
                return result;
            }
        }

        private Long getLowestInflight() {
            synchronized (lock) {
                Entry<Long, PendingEvent> entry = inflight.peekFirst();
                return entry == null ? null : entry.getKey();
            }
        }

        private void releaseIfEmptyInflight() {
            synchronized (lock) {
                if (inflight.isEmpty()) {
                    log.trace("Inflight empty for writer {}", writerId);
                    waitingInflight.release();
                }
            }
        }

        private List<Map.Entry<Long, PendingEvent>> getAllInflight() {
            synchronized (lock) {
                return new ArrayList<>(inflight);
            }
        }

        private List<PendingEvent> getAllInflightEvents() {
            synchronized (lock) {
                return inflight.stream().map(entry -> entry.getValue()).collect(Collectors.toList());
            }
        }

        private List<PendingEvent> getAllInflightEventsAndClear() {
            synchronized (lock) {
                List<PendingEvent> inflightEvents = getAllInflightEvents();
                inflight.clear();
                return inflightEvents;
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

        private Throwable getException() {
            synchronized (lock) {
                return exception;
            }
        }
    }

    private final class ResponseProcessor extends FailingReplyProcessor {
        @Override
        public void connectionDropped() {
            failConnection(new ConnectionFailedException("Connection dropped for writer " + writerId));
        }

        @Override
        public void wrongHost(WrongHost wrongHost) {
            log.info("Received wrongHost {}", wrongHost);
            ClientConnection connection = state.getConnection();
            if (connection != null) {
                controller.updateStaleValueInCache(wrongHost.getSegment(), connection.getLocation());
            }
            failConnection(new ConnectionFailedException(wrongHost.toString()));
        }

        /**
         * Invariants for segment sealed:
         *   - SegmentSealed callback and write will not run concurrently.
         *   - During the execution of the call back no new writes will be executed.
         * Once the segment Sealed callback is executed successfully
         *  - there will be no new writes to this segment.
         *  - any write to this segment will throw a SegmentSealedException.
         *
         * @param segmentIsSealed SegmentIsSealed WireCommand.
         */
        @Override
        public void segmentIsSealed(SegmentIsSealed segmentIsSealed) {
            log.info("Received SegmentSealed {} on writer {}", segmentIsSealed, writerId);
            invokeResendCallBack(segmentIsSealed);
        }

        @Override
        public void noSuchSegment(NoSuchSegment noSuchSegment) {
            log.info("Received noSuchSegment for writer {}", writerId);
            final String segment = noSuchSegment.getSegment();
            if (NameUtils.isTransactionSegment(segment)) {
                log.info("Transaction Segment: {} no longer exists since the txn is aborted. {}", noSuchSegment.getSegment(),
                        noSuchSegment.getServerStackTrace());
                //close the connection and update the exception to SegmentSealed.
                state.failConnection(new SegmentSealedException(segment));
            } else {
                state.failConnection(new NoSuchSegmentException(segment));
                log.info("Segment being written to {} by writer {} no longer exists due to Stream Truncation, resending to the newer segment. {}",
                        noSuchSegment.getSegment(), writerId, noSuchSegment.getServerStackTrace());
                invokeResendCallBack(noSuchSegment);
            }
        }

        @Override
        public void errorMessage(WireCommands.ErrorMessage errorMessage) {
            log.info("Received an errorMessage containing an unhandled {} on segment {}",
                    errorMessage.getErrorCode().getExceptionType().getSimpleName(),
                    errorMessage.getSegment());
            state.failConnection(errorMessage.getThrowableException());
        }

        @Override
        public void dataAppended(DataAppended dataAppended) {
            log.trace("Received dataAppended ack: {}", dataAppended);
            long ackLevel = dataAppended.getEventNumber();
            long previousAckLevel = dataAppended.getPreviousEventNumber();
            try {
                checkAckLevels(ackLevel, previousAckLevel);
                state.noteSegmentLength(dataAppended.getCurrentSegmentWriteOffset());
                ackUpTo(ackLevel);
            } catch (Exception e) {
                failConnection(e);
            }
        }

        @Override
        public void appendSetup(AppendSetup appendSetup) {
            log.info("Received appendSetup {}", appendSetup);
            long ackLevel = appendSetup.getLastEventNumber();
            ackUpTo(ackLevel);
            List<Append> toRetransmit = state.getAllInflight()
                                             .stream()
                                             .map(entry -> new Append(segmentName, writerId, entry.getKey(),
                                                                      entry.getValue().getEventCount(),
                                                                      entry.getValue().getData(),
                                                                      null,
                                                                      requestId
                                                                      ))
                                             .collect(Collectors.toList());
            ClientConnection connection = state.getConnection();
            if (connection == null) {
                log.warn("Connection setup could not be completed because connection is already failed for writer {}", writerId);
                return;
            }
            if (toRetransmit.isEmpty() || state.needSuccessors.get()) {
                log.info("Connection setup complete for writer {}", writerId);
                state.connectionSetupComplete(connection);
            } else {
                connection.sendAsync(toRetransmit, e -> {
                    if (e == null) {
                        state.connectionSetupComplete(connection);
                    } else {
                        failConnection(e);
                    }
                });
            }
        }

        private void invokeResendCallBack(WireCommand wireCommand) {
            if (state.needSuccessors.compareAndSet(false, true)) {
                Retry.indefinitelyWithExpBackoff(retrySchedule.getInitialMillis(), retrySchedule.getMultiplier(),
                        retrySchedule.getMaxDelay(),
                        t -> log.error(writerId + " to invoke resendToSuccessors callback: ", t))
                     .runInExecutor(() -> {
                         log.debug("Invoking resendToSuccessors call back for {} on writer {}", wireCommand, writerId);
                         resendToSuccessorsCallback.accept(Segment.fromScopedName(getSegmentName()));
                     }, connectionPool.getInternalExecutor())
                     .thenRun(() -> {
                         log.trace("Release inflight latch for writer {}", writerId);
                         state.waitingInflight.release();
                     });
            }
        }

        private void ackUpTo(long ackLevel) {
            final List<PendingEvent> pendingEvents = state.removeInflightBelow(ackLevel);
            // Complete the futures and release buffer in a different thread.
            connectionPool.getInternalExecutor().execute(() -> {
                for (PendingEvent toAck : pendingEvents) {
                    if (toAck.getAckFuture() != null) {
                        toAck.getAckFuture().complete(null);
                    }
                    toAck.getData().release();
                }
            });
        }

        private void checkAckLevels(long ackLevel, long previousAckLevel) {
            checkState(previousAckLevel < ackLevel, "Bad ack from server - previousAckLevel = %s, ackLevel = %s",
                       previousAckLevel, ackLevel);
            // we only care that the lowest in flight level is higher than previous ack level.
            // it may be higher by more than 1 (eg: in the case of a prior failed conditional appends).
            // this is because client never decrements eventNumber.
            Long lowest = state.getLowestInflight();
            checkState(lowest > previousAckLevel, "Missed ack from server - previousAckLevel = %s, ackLevel = %s, inFlightLevel = %s",
                       previousAckLevel, ackLevel, lowest);
        }

        @Override
        public void processingFailure(Exception error) {
            failConnection(error);
        }

        @Override
        public void authTokenCheckFailed(WireCommands.AuthTokenCheckFailed authTokenCheckFailed) {
            if (authTokenCheckFailed.isTokenExpired()) {
                failConnection(new TokenExpiredException(authTokenCheckFailed.getServerStackTrace()));
            } else {
                failConnection(new InvalidTokenException(authTokenCheckFailed.toString()));
            }
        }
    }

    /**
     * @see SegmentOutputStream#write(PendingEvent)
     *
     */
    @Override
    public void write(PendingEvent event) {
        //State is set to sealed during a Transaction abort and the segment writer should not throw an {@link IllegalStateException} in such a case.
        checkState(!state.isAlreadySealed() || NameUtils.isTransactionSegment(segmentName), "Segment: %s is already sealed", segmentName);
        synchronized (writeOrderLock) {
            ClientConnection connection;
            try {
                // if connection is null getConnection() establishes a connection and retransmits all events in inflight
                // list.
                connection = Futures.getThrowingException(getConnection());
            } catch (SegmentSealedException | NoSuchSegmentException e) {
                // Add the event to inflight, this will be resent to the successor during the execution of resendToSuccessorsCallback
                state.addToInflight(event);
                return;
            } catch (RetriesExhaustedException e) {
                event.getAckFuture().completeExceptionally(e);
                log.error("Failed to write event to Pravega due connectivity error ", e);
                return;
            }
            long eventNumber = state.addToInflight(event);
            try {
                Append append = new Append(segmentName, writerId, eventNumber, event.getEventCount(), event.getData(), null, requestId);
                log.trace("Sending append request: {}", append);
                connection.send(append);
            } catch (ConnectionFailedException e) {
                log.warn("Failed writing event through writer " + writerId + " due to: ", e);
                failConnection(e); // As the message is inflight, this will perform the retransmission.
                // Note that failConnection is called here instead of reconnect because it avoids the risk that
                // some other code path could have re-established the connection before the event was added to inflight.
            }
        }
    }

    /**
     * Establish a connection and wait for it to be setup. (Retries built in)
     */
    CompletableFuture<ClientConnection> getConnection() throws SegmentSealedException {
        if (state.isClosed()) {
            throw new IllegalStateException("SegmentOutputStream is already closed", state.getException());
        }
        if (state.needSuccessors.get()) {
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
        int numInflight = state.getNumInflight();
        log.debug("Flushing writer: {} with {} inflight events", writerId, numInflight);
        if (numInflight != 0) {
            try {
                ClientConnection connection = Futures.getThrowingException(getConnection());
                connection.send(new KeepAlive());
            } catch (SegmentSealedException | NoSuchSegmentException e) {
                if (NameUtils.isTransactionSegment(segmentName)) {
                    log.warn("Exception observed during a flush on a transaction segment, this indicates that the transaction is " +
                                     "committed/aborted. Details: {}", e.getMessage());
                    failConnection(e);
                } else {
                    log.info("Exception observed while obtaining connection during flush. Details: {} ", e.getMessage());
                }
            } catch (Exception e) {
                failConnection(e);
                if (e instanceof RetriesExhaustedException) {
                    log.error("Flush on segment {} by writer {} failed after all retries", segmentName, writerId);
                    //throw an exception to the external world that the flush failed due to RetriesExhaustedException
                    throw Exceptions.sneakyThrow(e);
                }
            }
            state.waitForInflight();
            Exceptions.checkNotClosed(state.isClosed(), this);
            /* SegmentSealedException is thrown if either of the below conditions are true
                 - resendToSuccessorsCallback has been invoked.
                 - the segment corresponds to an aborted Transaction.
             */
            if (state.needSuccessors.get() || (NameUtils.isTransactionSegment(segmentName) && state.isAlreadySealed())) {
                throw new SegmentSealedException(segmentName + " sealed for writer " + writerId);
            }

        } else if (state.exception instanceof RetriesExhaustedException) {
            // All attempts to connect with SSS have failed.
            // The number of retry attempts is based on EventWriterConfig
            log.error("Flush on segment {} by writer {} failed after all retries", segmentName, writerId);
            throw Exceptions.sneakyThrow(state.exception);
        }
    }

    /**
     * @see SegmentOutputStream#flush()
     */
    @Override
    public void flushAsync() {}

    private void failConnection(Throwable e) {
        if (e instanceof TokenExpiredException) {
            this.tokenProvider.signalTokenExpired();
        }
        log.warn("Failing connection for writer {} with exception {}", writerId, e.toString());
        state.failConnection(Exceptions.unwrap(e));
        reconnect();
    }

    @VisibleForTesting
    void reconnect() {
        if (state.isClosed()) {
            return;
        }
        log.debug("(Re)connect invoked, Segment: {}, writerID: {}", segmentName, writerId);
        state.setupConnection.registerAndRunReleaser(() -> {
            retrySchedule.withInitialDelayForfirstRetry(true).retryWhen(t -> t instanceof Exception) // retry on all exceptions.
              .runAsync(() -> {
                  log.debug("Running reconnect for segment {} writer {}", segmentName, writerId);

                  if (state.isClosed() || state.needSuccessors.get()) {
                      // stop reconnect when writer is closed or resend inflight to successors has been triggered.
                      return CompletableFuture.completedFuture(null);
                  }
                  Preconditions.checkState(state.getConnection() == null);
                  log.info("Fetching endpoint for segment {}, writer {}", segmentName, writerId);

                  return controller.getEndpointForSegment(segmentName)
                      // Establish and return a connection to segment store
                      .thenComposeAsync((PravegaNodeUri uri) -> {
                          log.info("Establishing connection to {} for {}, writerID: {}", uri, segmentName, writerId);
                          return establishConnection(uri);
                      }, connectionPool.getInternalExecutor())
                      .thenCombineAsync(tokenProvider.retrieveToken(),
                                        AbstractMap.SimpleEntry<ClientConnection, String>::new,
                                        connectionPool.getInternalExecutor())
                      .thenComposeAsync(pair -> {
                          ClientConnection connection = pair.getKey();
                          String token = pair.getValue();

                          CompletableFuture<Void> connectionSetupFuture = state.newConnection(connection);
                          SetupAppend cmd = new SetupAppend(requestId, writerId, segmentName, token);
                          try {
                              connection.send(cmd);
                          } catch (ConnectionFailedException e1) {
                              // This needs to be invoked here because call to failConnection from netty may occur before state.newConnection above.
                              state.failConnection(e1);
                              throw Exceptions.sneakyThrow(e1);
                          }
                          // A timeout is added to the future before the call, and it triggers a TimeoutException.
                          // A late timeout if fine it will just cause a spurious connection close.
                          // A late success may be a problem because it causes retransmits of the wrong messages.
                          // In theory the server should guard against this, but that's not ideal to depend on for client correctness.
                          // Instead, the local future and connection is used and connectionSetupComplete takes a connection object.
                          return connectionSetupFuture.exceptionally(t1 -> {
                              Throwable exception = Exceptions.unwrap(t1);
                              if (exception instanceof InvalidTokenException) {
                                  log.info("Ending reconnect attempts on writer {} to {} because token verification failed due to invalid token",
                                          writerId, segmentName);
                                  return null;
                              }
                              if (exception instanceof TimeoutException) {
                                  log.info("Writer writer {} on Segemnt {} timed out contacting the server", writerId, segmentName);
                                  connection.close();
                              }
                              if (exception instanceof SegmentSealedException) {
                                  log.info("Ending reconnect attempts on writer {} to {} because segment is sealed", writerId, segmentName);
                                  return null;
                              }
                              if (exception instanceof NoSuchSegmentException) {
                                  log.info("Ending reconnect attempts on writer {} to {} because segment is truncated", writerId, segmentName);
                                  return null;
                              }
                              throw Exceptions.sneakyThrow(t1);
                          });

                      }, connectionPool.getInternalExecutor());
              }, connectionPool.getInternalExecutor()).exceptionally(t -> {
                 log.error("Error while attempting to establish connection for writer {}", writerId, t);
                 failAndRemoveUnackedEvents(t);
                 return null;
             });

        }, new CompletableFuture<ClientConnection>());
    }

    private CompletableFuture<ClientConnection> establishConnection(PravegaNodeUri uri) {
        if (useConnectionPooling) {
            return connectionPool.getClientConnection(Flow.from(requestId), uri, responseProcessor);
        } else {
            return connectionPool.getClientConnection(uri, responseProcessor);
        }
    }

    private void failAndRemoveUnackedEvents(Throwable t) {
        state.getAllInflightEventsAndClear().parallelStream().forEach(event -> event.getAckFuture().completeExceptionally(t));
        state.failConnection(t);
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
        log.debug("GetUnackedEventsOnSeal called on {}", writerId);
        synchronized (writeOrderLock) {   
            state.failConnection(new SegmentSealedException(this.segmentName));
            return Collections.unmodifiableList(state.getAllInflightEvents());
        }
    }

    @Override
    public long getLastObservedWriteOffset() {
        return state.getLastSegmentLength();
    }
}