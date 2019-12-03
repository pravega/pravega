/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.server.host.handler;

import com.google.common.base.Preconditions;
import io.pravega.segmentstore.contracts.BadOffsetException;
import io.pravega.shared.protocol.netty.WireCommands;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

@ThreadSafe
class WriterState {
    @Getter
    private final Object ackLock = new Object();
    @GuardedBy("this")
    private long lastStoredEventNumber;
    @GuardedBy("this")
    private long lastAckedEventNumber;
    @GuardedBy("this")
    private int inFlightCount;
    @GuardedBy("this")
    private ArrayList<ErrorContext> errorContexts;

    WriterState(long initialEventNumber) {
        this.inFlightCount = 0;
        this.lastStoredEventNumber = initialEventNumber;
        this.lastAckedEventNumber = initialEventNumber;
    }

    /**
     * Invoked when a new append is initiated.
     *
     * @param eventNumber The Append's Event Number.
     * @return The previously attempted Event Number.
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
     * @param eventNumber
     */
    synchronized void conditionalAppendFailed(long eventNumber) {
        this.inFlightCount--;
        if (this.inFlightCount == 0) {
            this.lastStoredEventNumber = this.lastAckedEventNumber;
        }

        updateErrorContexts(eventNumber);
    }

    /**
     * TODO: javadoc if this works well.
     *
     * @param eventNumber
     * @param delayedErrorHandler
     */
    synchronized void appendFailed(long eventNumber, Runnable delayedErrorHandler) {
        this.inFlightCount--;
        if (this.errorContexts == null) {
            this.errorContexts = new ArrayList<>();
        }

        updateErrorContexts(eventNumber);
        this.errorContexts.add(new ErrorContext(this.lastStoredEventNumber, this.inFlightCount, delayedErrorHandler));
    }

    /**
     * Invoked when an append has been successfully stored and is about to be ack-ed to the Client.
     * <p>
     * This method is designed to be invoked immediately before sending a {@link WireCommands.DataAppended} ack to the client,
     * however both its invocation and the ack must be sent atomically as the Client expects acks to arrive in order.
     * <p>
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
        updateErrorContexts(eventNumber);
        return previousLastAcked;
    }

    synchronized DelayedErrorHandler getDelayedErrorHandlerIfEligible() {
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

    @RequiredArgsConstructor(access = AccessLevel.PRIVATE)
    @Getter
    static class DelayedErrorHandler {
        private final List<Runnable> toRun;
        private final int handlersRemaining;
    }

    private static class ErrorContext {
        private final long erroredEventNumber;
        private int remainingInFlightCount;
        private final Runnable delayedErrorHandler;

        ErrorContext(long erroredEventNumber, int remainingInFlightCount, Runnable delayedErrorHandler) {
            this.erroredEventNumber = erroredEventNumber;
            this.remainingInFlightCount = remainingInFlightCount;
            this.delayedErrorHandler = delayedErrorHandler;
        }

        void appendComplete(long eventNumber) {
            if (eventNumber < this.erroredEventNumber) {
                this.remainingInFlightCount--;
                assert this.remainingInFlightCount >= 0;
            }
        }

        Runnable getDelayedErrorHandlerIfEligible() {
            if (this.remainingInFlightCount == 0) {
                return this.delayedErrorHandler;
            }

            return null;
        }
    }
}
