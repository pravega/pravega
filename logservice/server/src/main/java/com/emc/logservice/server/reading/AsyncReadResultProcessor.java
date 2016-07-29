/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.emc.logservice.server.reading;

import com.emc.logservice.common.CallbackHelpers;
import com.emc.logservice.common.FutureHelpers;
import com.emc.logservice.contracts.ReadResult;
import com.emc.logservice.contracts.ReadResultEntry;
import com.emc.logservice.contracts.ReadResultEntryContents;
import com.emc.logservice.server.ServiceShutdownListener;
import com.google.common.util.concurrent.AbstractIdleService;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * An Asynchronous processor for ReadResult objects. Attaches to a ReadResult and executes a callback using an Executor
 * for each ReadResultEntry returned by the result. This class is suitable for handling long-poll reads, as it does not
 * hog any threads while waiting for such future reads to become available. It only uses (a thread from) the Executor
 * when the data for a read becomes available, at which point it executes the handler on such a thread.
 * <p/>
 * The AsyncReadResultProcessor stops when any of the following conditions occur
 * <ul>
 * <li> The ReadResult reaches the end (hasNext() == false)
 * <li> The ReadResult is closed
 * <li> An error was encountered while fetching such an entry.
 * <li> The user-supplied entry handler returns false.
 * </ul>
 */
public class AsyncReadResultProcessor extends AbstractIdleService implements AutoCloseable {
    //region Members

    private final ReadResult readResult;
    private final UUID processorId;
    private final Function<AsyncReadResultEntry, Boolean> readEntryHandler;
    private final Consumer<AsyncReadResultFailure> failureHandler;
    private final Executor executor;
    private ReadResultEntry currentEntry;
    private boolean closed;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the AsyncReadResultProcessor class.
     *
     * @param readResult       The ReadResult to attach to. When the ReadResult is closed, the AsyncReadResultProcessor
     *                         is closed as well. When the AsyncReadResultProcessor is closed, the ReadResult is closed too.
     * @param processorId      A unique identifier for this AsyncReadResultProcessor. Every callback invocation will
     *                         contain this identifier.
     * @param readEntryHandler A callback that will be invoked with every ReadResultEntry that is returned by the ReadResult.
     * @param failureCallback  A callback that will be invoked when a ReadResultEntry failed to process. After this callback
     *                         is invoked, the AsyncReadResultProcessor will auto-close itself and become unusable.
     * @param executor         An Executor to use for asynchronous callbacks.
     */
    public AsyncReadResultProcessor(ReadResult readResult, UUID processorId, Function<AsyncReadResultEntry, Boolean> readEntryHandler, Consumer<AsyncReadResultFailure> failureCallback, Executor executor) {
        this.readResult = readResult;
        this.processorId = processorId;
        this.readEntryHandler = readEntryHandler;
        this.failureHandler = failureCallback;
        this.executor = executor;
    }

    //endregion

    //region AutoCloseable Implementation

    @Override
    public void close() {
        if (!this.closed) {
            if (state() == State.RUNNING) {
                stopAsync();
                ServiceShutdownListener.awaitShutdown(this, false);
            }

            this.closed = true;
        }
    }

    //endregion

    //region AbstractIdleService Implementation

    @Override
    protected void startUp() throws Exception {
        this.executor.execute(this::fetchNextEntry);
    }

    @Override
    protected void shutDown() throws Exception {
        if (this.currentEntry != null) {
            this.currentEntry.getContent().cancel(true);
            this.currentEntry = null;
        }

        this.readResult.close();
    }

    //endregion

    //region Processing

    private void fetchNextEntry() {
        if (this.closed) {
            // Nothing else to do.
            return;
        }

        if (this.currentEntry != null) {
            fail(new AssertionError("fetchNextEntry: previous entry retrieval is still in progress."));
            return;
        }

        // Get the next item. Both next() and getContent() may throw exceptions at us, so we must make sure we handle
        // them appropriately.
        try {
            // We don't really rely on hasNext; we just use the fact that next() returns null if there is nothing else to read.
            this.currentEntry = this.readResult.next();
            if (this.currentEntry == null || this.currentEntry.isEndOfStreamSegment()) {
                close();
                return;
            }

            // Retrieve the contents.
            CompletableFuture<ReadResultEntryContents> entryContentsFuture = this.currentEntry.getContent();

            // Attach the appropriate handlers.
            FutureHelpers.exceptionListener(entryContentsFuture, this::fail);
            entryContentsFuture.thenRunAsync(this::handleEntryFetched, this.executor);
        } catch (Exception | AssertionError ex) {
            // Any processing exception must be dealt with. Otherwise we are stuck with a Processor that keeps running forever.
            fail(ex);
        }
    }

    private void handleEntryFetched() {
        if (this.currentEntry == null) {
            fail(new AssertionError("handleEntryFetched: currentEntry is null"));
            return;
        }

        boolean shouldContinue;
        try {
            if (this.currentEntry.getContent().isCompletedExceptionally()) {
                fail(new AssertionError("handleEntryFetched: About to have processed a ReadResultEntry that was not properly fetched."));
                return;
            }

            // Process the current entry.
            shouldContinue = this.readEntryHandler.apply(new AsyncReadResultEntry(this.currentEntry, this.processorId));

            // At this point, the entry is processed, so we should clear the pointer to it.
            this.currentEntry = null;
        } catch (Exception | AssertionError ex) {
            // Any processing exception must be dealt with. Otherwise we are stuck with a Processor that keeps running forever.
            fail(ex);
            return;
        }

        if (shouldContinue) {
            // If the callback indicated we should continue, do so.
            fetchNextEntry();
        } else {
            // Otherwise close the processor (and the underlying result).
            close();
        }
    }

    private void fail(Throwable exception) {
        CallbackHelpers.invokeSafely(this.failureHandler, new AsyncReadResultFailure(exception, this.currentEntry), null);
        close();
    }

    //endregion

    //region AsyncReadResultEntry

    /**
     * Callback argument when a ReadResultEntry is successfully retrieved.
     */
    public static class AsyncReadResultEntry {
        private final ReadResultEntry entry;
        private final UUID processorId;

        private AsyncReadResultEntry(ReadResultEntry entry, UUID processorId) {
            this.entry = entry;
            this.processorId = processorId;
        }

        /**
         * Gets a pointer to the ReadResultEntry that was retrieved.
         *
         * @return
         */
        public ReadResultEntry getEntry() {
            return this.entry;
        }

        /**
         * Gets a value representing the AsyncReadResultProcessor Id that invoked the callback.
         *
         * @return
         */
        public UUID getProcessorId() {
            return this.processorId;
        }

        @Override
        public String toString() {
            return String.format("%s: %s", this.processorId, this.getEntry());
        }
    }

    //endregion

    //region AsyncReadResultFailure

    /**
     * Callback argument when a ReadResultEntry fails to fetch.
     */
    public static class AsyncReadResultFailure {
        private final Throwable cause;
        private final ReadResultEntry currentEntry;

        private AsyncReadResultFailure(Throwable cause, ReadResultEntry currentEntry) {
            this.cause = cause;
            this.currentEntry = currentEntry;
        }

        /**
         * Gets a pointer to the causing Exception for the failure.
         *
         * @return
         */
        public Throwable getCause() {
            return this.cause;
        }

        /**
         * Gets the current ReadResultEntry that failed to fetch. May be null.
         *
         * @return
         */
        public ReadResultEntry getCurrentEntry() {
            return this.currentEntry;
        }

        @Override
        public String toString() {
            return String.format("Entry = %s, Cause = %s", this.currentEntry, this.cause);
        }
    }

    //endregion
}

