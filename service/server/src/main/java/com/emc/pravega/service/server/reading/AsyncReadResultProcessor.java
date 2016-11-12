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

package com.emc.pravega.service.server.reading;

import com.emc.pravega.common.concurrent.FutureHelpers;
import com.emc.pravega.service.contracts.ReadResult;
import com.emc.pravega.service.contracts.ReadResultEntry;
import com.emc.pravega.service.contracts.ReadResultEntryContents;
import com.emc.pravega.service.contracts.ReadResultEntryType;
import com.emc.pravega.service.server.ExceptionHelpers;
import com.emc.pravega.service.server.ServiceShutdownListener;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.AbstractIdleService;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

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
 * <li> The user-supplied AsyncReadResultEntryHandler returns false from a call to shouldRequestContents.
 * <li> The user-supplied AsyncReadResultEntryHandler returns false from a call to processEntry.
 * </ul>
 */
public class AsyncReadResultProcessor extends AbstractIdleService implements AutoCloseable {
    //region Members

    private final ReadResult readResult;
    private final AsyncReadResultEntryHandler entryHandler;
    private final Executor executor;
    private ReadResultEntry currentEntry;
    private boolean closed;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the AsyncReadResultProcessor class.
     *
     * @param readResult   The ReadResult to attach to. When the ReadResult is closed, the AsyncReadResultProcessor
     *                     is closed as well. When the AsyncReadResultProcessor is closed, the ReadResult is closed too.
     * @param entryHandler A handler for every ReadResultEntry that is extracted out of the ReadResult.
     * @param executor     An Executor to use for asynchronous callbacks.
     */
    public AsyncReadResultProcessor(ReadResult readResult, AsyncReadResultEntryHandler entryHandler, Executor
            executor) {
        Preconditions.checkNotNull(readResult, "readResult");
        Preconditions.checkNotNull(entryHandler, "entryHandler");
        Preconditions.checkNotNull(executor, "executor");

        this.readResult = readResult;
        this.entryHandler = entryHandler;
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
            // We don't really rely on hasNext; we just use the fact that next() returns null if there is nothing
            // else to read.
            this.currentEntry = this.readResult.next();
            if (this.currentEntry == null || this.currentEntry.getType() == ReadResultEntryType.EndOfStreamSegment) {
                close();
                return;
            }

            // Retrieve the contents.
            CompletableFuture<ReadResultEntryContents> entryContentsFuture = this.currentEntry.getContent();
            if (!entryContentsFuture.isDone()) {
                // We have received a ReadResultEntry that does not have data readily available.
                if (this.entryHandler.shouldRequestContents(this.currentEntry.getType(), this.currentEntry
                        .getStreamSegmentOffset())) {
                    // We were instructed to request the content.
                    this.currentEntry.requestContent(this.entryHandler.getRequestContentTimeout());
                } else {
                    // Not requesting content means we do not want to proceed with the result anymore.
                    close();
                    return;
                }
            }

            // Attach the appropriate handlers.
            FutureHelpers.exceptionListener(entryContentsFuture, this::fail);
            entryContentsFuture.thenRunAsync(this::handleEntryFetched, this.executor);
        } catch (Exception | AssertionError ex) {
            // Any processing exception must be dealt with. Otherwise we are stuck with a Processor that keeps
            // running forever.
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
                fail(new AssertionError("handleEntryFetched: About to have processed a ReadResultEntry that was not " +
                        "properly fetched."));
                return;
            }

            // Process the current entry.
            shouldContinue = this.entryHandler.processEntry(this.currentEntry);
            this.currentEntry = null;
        } catch (Exception | AssertionError ex) {
            // Any processing exception must be dealt with. Otherwise we are stuck with a Processor that keeps
            // running forever.
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
        try {
            this.entryHandler.processError(this.currentEntry, exception);
        } catch (Throwable ex) {
            if (ExceptionHelpers.mustRethrow(ex)) {
                throw ex;
            }
        }

        close();
    }

    //endregion
}

