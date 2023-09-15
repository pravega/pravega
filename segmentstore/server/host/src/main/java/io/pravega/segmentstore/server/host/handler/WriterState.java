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
package io.pravega.segmentstore.server.host.handler;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import io.pravega.segmentstore.contracts.BadOffsetException;
import io.pravega.shared.protocol.netty.WireCommands;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

/**
 * Maintains state for a Writer in the {@link AppendProcessor}.
 * This class is tightly-coupled with the {@link AppendProcessor} behavior and should not be used for any other purpose.
 */
@ThreadSafe
class WriterState {
    //region Members
    /**
     * An Event Number that is used for {@link #getLowestFailedEventNumber} to indicate there is no failure yet.
     */
    static final long NO_FAILED_EVENT_NUMBER = Long.MAX_VALUE;
    /**
     * A mutex that can be used to ensure Acks to the Client are sent in order.
     */
    @Getter
    private final Object ackLock = new Object();
    /**
     * The Event Number of the last Append that was sent to the Store (these events may not yet be complete yet).
     */
    @GuardedBy("this")
    private long lastStoredEventNumber;
    /**
     * The Event Number of the last Event that was Acked to the Client.
     */
    @GuardedBy("this")
    private long lastAckedEventNumber;
    /**
     * Number of Appends that have been sent to the Store but not yet acked back from the Store.
     */
    @GuardedBy("this")
    private int inFlightCount;
    /**
     * The Event Number of the earliest Append that was failed.
     */
    @GuardedBy("this")
    private long smallestFailedEventNumber;
    /**
     * The {@link ErrorContext}s that have been recorded for this instance.
     */
    @GuardedBy("this")
    private ArrayList<ErrorContext> errorContexts;

    /**
     * Expected size of events for index segment append.Value 0 indicates the segment is not a Stream Segment.
     */
    private final long indexEventSize;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the {@link WriterState} class.
     *
     * @param initialEventNumber The current Event Number on the Segment associated with this writer.
     */
    @VisibleForTesting
    WriterState(long initialEventNumber) {
        this.inFlightCount = 0;
        this.smallestFailedEventNumber = NO_FAILED_EVENT_NUMBER; // Nothing failed yet.
        this.lastStoredEventNumber = initialEventNumber;
        this.lastAckedEventNumber = initialEventNumber;
        this.indexEventSize = 0L;
    }

    /**
     * Creates a new instance of the {@link WriterState} class.
     *
     * @param initialEventNumber The current Event Number on the Segment associated with this writer.
     * @param indexEventSize Event size allowed for index appends in case of stream segments.
     *                  Value 0 indicates the segment is not a Stream Segment.
     */
    WriterState(long initialEventNumber, long indexEventSize) {
        this.inFlightCount = 0;
        this.smallestFailedEventNumber = NO_FAILED_EVENT_NUMBER; // Nothing failed yet.
        this.lastStoredEventNumber = initialEventNumber;
        this.lastAckedEventNumber = initialEventNumber;
        this.indexEventSize = indexEventSize;
    }
    //endregion

    //region Operations

    /**
     * Invoked when a new append is initiated.
     *
     * @param eventNumber The Append's Event Number.
     * @return The previously attempted Event Number.
     * @throws IllegalStateException If eventNumber is less than the one for the previous attempt.
     */
    synchronized long beginAppend(long eventNumber) {
        long previousEventNumber = this.lastStoredEventNumber;
        Preconditions.checkState(eventNumber >= previousEventNumber, "Event was already appended.");
        this.lastStoredEventNumber = eventNumber;
        this.inFlightCount++;
        return previousEventNumber;
    }

    /**
     * Invoked when a conditional append has failed due to {@link BadOffsetException}. If no more appends are in the
     * pipeline, then the Last Stored Event Number is reverted to the Last (Successfully) Acked Event Number.
     *
     * @param eventNumber The Event Number of the Append that failed.
     */
    synchronized void conditionalAppendFailed(long eventNumber) {
        this.inFlightCount--;
        if (this.inFlightCount == 0) {
            this.lastStoredEventNumber = this.lastAckedEventNumber;
        }

        // Update any existing ErrorContexts that an append has completed.
        updateErrorContexts(eventNumber);
    }

    /**
     * Invoked when an append failed for any reason other than a conditional failure (i.e. BadOffsetException).
     *
     * @param eventNumber         The Event Number of the Append that failed.
     * @param delayedErrorHandler A {@link Runnable} that will be made available via {@link #fetchEligibleDelayedErrorHandler()}
     *                            when all appends with Event Numbers less than the current {@link #lastStoredEventNumber}
     *                            are completed (successfully or not).
     */
    synchronized void appendFailed(long eventNumber, @NonNull Runnable delayedErrorHandler) {
        this.inFlightCount--;
        this.smallestFailedEventNumber = Math.min(eventNumber, this.smallestFailedEventNumber);
        if (this.errorContexts == null) {
            this.errorContexts = new ArrayList<>();
        }

        // Update any existing ErrorContexts that an append has completed.
        updateErrorContexts(eventNumber);

        // Record a new ErrorContext.
        this.errorContexts.add(new ErrorContext(eventNumber, this.lastStoredEventNumber, this.inFlightCount, delayedErrorHandler));

        // Ensure all error contexts are sorted by their failed Event Number. That will ensure that they will be executed
        // in the correct order.
        this.errorContexts.sort(Comparator.comparingLong(ErrorContext::getFailedEventNumber));
    }

    /**
     * Invoked when an append has been successfully stored and is about to be ack-ed to the Client.
     *
     * This method is designed to be invoked immediately before sending a {@link WireCommands.DataAppended} ack to the client,
     * however both its invocation and the ack must be sent atomically as the Client expects acks to arrive in order.
     *
     * When composing a {@link WireCommands.DataAppended} ack, the value passed to eventNumber should be passed as
     * {@link WireCommands.DataAppended#getEventNumber()} and the return value from this method should be passed as
     * {@link WireCommands.DataAppended#getPreviousEventNumber()}.
     *
     * @param eventNumber The Append's Event Number. This should correspond to the last successful append in the Store
     *                    and will be sent in the {@link WireCommands.DataAppended} ack back to the Client. This value will be
     *                    remembered and returned upon the next invocation of this method. If this value is less
     *                    than that of a previous invocation of this method (due to out-of-order acks from the Store),
     *                    it will have no effect as it has already been ack-ed as part of a previous call.
     * @return The last successful Append's Event Number (prior to this one). This is the value of eventNumber for
     * the previous invocation of this method.
     */
    synchronized long appendSuccessful(long eventNumber) {
        this.inFlightCount--;
        long previousLastAcked = this.lastAckedEventNumber;
        this.lastAckedEventNumber = Math.max(previousLastAcked, eventNumber);
        updateErrorContexts(eventNumber); // Update any existing ErrorContexts that an append has completed.
        return previousLastAcked;
    }

    /**
     * Gets the Event Number of the earliest Event failure recorded via {@link #appendFailed}.
     *
     * @return The earliest failed Event Number, or {@link #NO_FAILED_EVENT_NUMBER} if {@link #appendFailed} has not been invoked.
     */
    synchronized long getLowestFailedEventNumber() {
        return this.smallestFailedEventNumber;
    }

    /**
     * Gets a {@link DelayedErrorHandler} based on the following rules:
     * - If no call has been made to {@link #appendFailed}, this will return null.
     * - If {@link #appendFailed} has been invoked at least once, this will return a {@link DelayedErrorHandler} with
     * the following contents:
     * -- {@link DelayedErrorHandler#getHandlersToExecute()} will contain those handlers that were passed to {@link #appendFailed}
     * which are eligible to execute (all appends with Event Numbers less than their trigger have been completed).
     * -- {@link DelayedErrorHandler#getHandlersRemaining()} will indicate the number of handlers that are not yet
     * eligible for execution but which may be in a future call.
     *
     * This method is not idempotent. Any handlers for which {@link DelayedErrorHandler#getHandlersToExecute()} contains
     * something will be unregistered and will not be served upon a subsequent request.
     *
     * @return A {@link DelayedErrorHandler} or null.
     */
    synchronized DelayedErrorHandler fetchEligibleDelayedErrorHandler() {
        if (this.errorContexts == null) {
            return null;
        }

        ArrayList<Runnable> toRun = new ArrayList<>();
        Iterator<ErrorContext> i = this.errorContexts.iterator();
        while (i.hasNext()) {
            ErrorContext c = i.next();
            Runnable r = c.getDelayedErrorHandlerIfEligible();
            if (r != null) {
                toRun.add(r);
                i.remove();
            }
        }

        return new DelayedErrorHandler(toRun, this.errorContexts.size());
    }

    /**
     * Updates any {@link ErrorContext}s with the fact that an append with the given EventNumber has completed.
     *
     * @param eventNumber The EventNumber of the append that completed.
     */
    @GuardedBy("this")
    private void updateErrorContexts(long eventNumber) {
        if (this.errorContexts != null) {
            this.errorContexts.forEach(c -> c.appendComplete(eventNumber));
        }
    }

    @Override
    public synchronized String toString() {
        return String.format("Stored=%s, Acked=%s, InFlight=%s", this.lastStoredEventNumber, this.lastAckedEventNumber, this.inFlightCount);
    }

    //endregion

    //region DelayedErrorHandler

    /**
     * Contains information about an Error Handler that should be executed.
     */
    @RequiredArgsConstructor(access = AccessLevel.PRIVATE)
    @Getter
    static class DelayedErrorHandler {
        /**
         * A list of {@link Runnable}s that should be executed. It is OK if this list is empty (it means there is nothing
         * eligible to execute now, but there are outstanding handlers registered).
         */
        private final List<Runnable> handlersToExecute;
        /**
         * The number of handlers ({@link Runnable}s) that are still outstanding (but not yet eligible to execute).
         */
        private final int handlersRemaining;
    }

    //endregion

    //region ErrorContext

    /**
     * Keeps track of the triggering context for a single error handler.
     */
    private static class ErrorContext {
        /**
         * The Event Number of the Append that failed.
         */
        @Getter
        private final long failedEventNumber;
        /**
         * The Event Number of the last Append that was sent to the Store.
         */
        private final long lastStoredEventNumber;
        /**
         * The number of Events with Event Number less than {@link #lastStoredEventNumber} that are still in flight.
         */
        private int remainingInFlightCount;
        /**
         * A {@link Runnable} that should be invoked when {@link #remainingInFlightCount} reaches 0.
         */
        private final Runnable delayedErrorHandler;

        /**
         * Creates a new instance of the {@link ErrorContext} class.
         *
         * @param failedEventNumber      The Event Number of the Append that failed.
         * @param lastStoredEventNumber  The Event Number of the last Append that was sent to the Store.
         * @param remainingInFlightCount The current number of in-flight appends with Event Number less than or equal to
         *                               {@link #lastStoredEventNumber}.
         * @param delayedErrorHandler    A {@link Runnable} that should be invoked.
         */
        ErrorContext(long failedEventNumber, long lastStoredEventNumber, int remainingInFlightCount, Runnable delayedErrorHandler) {
            this.failedEventNumber = failedEventNumber;
            this.lastStoredEventNumber = lastStoredEventNumber;
            this.remainingInFlightCount = remainingInFlightCount;
            this.delayedErrorHandler = delayedErrorHandler;
        }

        /**
         * Records the fact that an append has been completed (successfully or not).
         *
         * @param eventNumber The Event Number of the completed append. If this is less than {@link #lastStoredEventNumber},
         *                    then {@link #remainingInFlightCount} will be decremented.
         */
        void appendComplete(long eventNumber) {
            if (eventNumber < this.lastStoredEventNumber) {
                this.remainingInFlightCount--;
                assert this.remainingInFlightCount >= 0; // sanity check, only for unit tests.
            }
        }

        /**
         * Gets the Delayed Error Handler.
         *
         * @return the {@link Runnable} passed into this instance's constructor, but only if all appends with Event Numbers
         * less than {@link #lastStoredEventNumber} have been completed. Otherwise returns null.
         */
        Runnable getDelayedErrorHandlerIfEligible() {
            if (this.remainingInFlightCount == 0) {
                return this.delayedErrorHandler;
            }

            return null;
        }
    }

    /**
     * Gets the expected Event size for index segment appends.
     *
     * @return The expected event size for index segment appends.
     */
    synchronized long getEventSizeForAppend() {
        return this.indexEventSize;
    }
    //endregion
}
