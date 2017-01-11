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
import com.google.common.base.Preconditions;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;

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
 * <li> The user-supplied AsyncReadResultHandler returns false from a call to shouldRequestContents.
 * <li> The user-supplied AsyncReadResultHandler returns false from a call to processEntry.
 * </ul>
 */
public class AsyncReadResultProcessor implements AutoCloseable {
    //region Members

    private final ReadResult readResult;
    private final AsyncReadResultHandler entryHandler;
    private final AtomicBoolean closed;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the AsyncReadResultProcessor class.
     *
     * @param readResult   The ReadResult to attach to. When the ReadResult is closed, the AsyncReadResultProcessor
     *                     is closed as well. When the AsyncReadResultProcessor is closed, the ReadResult is closed too.
     * @param entryHandler A handler for every ReadResultEntry that is extracted out of the ReadResult.
     */
    private AsyncReadResultProcessor(ReadResult readResult, AsyncReadResultHandler entryHandler) {
        Preconditions.checkNotNull(readResult, "readResult");
        Preconditions.checkNotNull(entryHandler, "entryHandler");

        this.readResult = readResult;
        this.entryHandler = entryHandler;
        this.closed = new AtomicBoolean();
    }

    /**
     * Processes the given ReadResult using the given AsyncReadResultHandler.
     *
     * @param readResult   The ReadResult to process.
     * @param entryHandler An AsyncReadResultHandler to be used for callbacks.
     * @param executor     An Executor to run asynchronous tasks on.
     * @return An instance of the AsyncReadResultProcessor that is processing the result. This can be closed at any time
     * there is no longer a need to process the result (Note that this will also close the underlying ReadResult). The
     * returned instance will auto-close when the ReadResult is processed in its entirety or when an exception is encountered.
     */
    public static AsyncReadResultProcessor process(ReadResult readResult, AsyncReadResultHandler entryHandler, Executor executor) {
        Preconditions.checkNotNull(executor, "executor");
        AsyncReadResultProcessor processor = new AsyncReadResultProcessor(readResult, entryHandler);
        processor.processResult(executor);
        return processor;
    }

    //endregion

    //region AutoCloseable Implementation

    @Override
    public void close() {
        // Try to close without setting any exception.
        close(null);
    }

    private void close(Throwable failureCause) {
        if (!this.closed.getAndSet(true)) {
            this.readResult.close();
            if (failureCause == null) {
                // We are closing normally with no processing exception.
                this.entryHandler.processResultComplete();
            } else {
                // An exception was encountered; this must be reported to the entry handler.
                this.entryHandler.processError(ExceptionHelpers.getRealException(failureCause));
            }
        }
    }

    //endregion

    //region Processing

    private void processResult(Executor executor) {
        // Process the result, one entry at a time, until one of the stopping conditions occurs.
        AtomicBoolean shouldContinue = new AtomicBoolean(true);
        FutureHelpers
                .loop(
                        () -> !this.closed.get() && shouldContinue.get(),
                        () -> {
                            CompletableFuture<ReadResultEntry> resultEntryFuture = fetchNextEntry();
                            shouldContinue.set(resultEntryFuture != null);
                            return resultEntryFuture != null ? resultEntryFuture : CompletableFuture.completedFuture(null);
                        },
                        resultEntry -> {
                            if (resultEntry != null) {
                                shouldContinue.set(this.entryHandler.processEntry(resultEntry));
                            }
                        },
                        executor)
                .whenComplete((r, ex) -> close(ex)); // Make sure always close the result processor when done (with our without failures).
    }

    private CompletableFuture<ReadResultEntry> fetchNextEntry() {
        // Get the next item. We don't really rely on hasNext; we just use the fact that next() returns null
        // if there is nothing else to read.
        ReadResultEntry currentEntry = this.readResult.next();
        if (currentEntry != null && currentEntry.getType() != ReadResultEntryType.EndOfStreamSegment) {
            // We have something to retrieve.
            CompletableFuture<ReadResultEntryContents> entryContentsFuture = currentEntry.getContent();
            if (entryContentsFuture.isDone()) {
                // Result is readily available.
                return CompletableFuture.completedFuture(currentEntry);
            } else if (this.entryHandler.shouldRequestContents(currentEntry.getType(), currentEntry.getStreamSegmentOffset())) {
                // ReadResultEntry that does not have data readily available and we were instructed to request the content.
                currentEntry.requestContent(this.entryHandler.getRequestContentTimeout());
                CompletableFuture<ReadResultEntry> resultEntryFuture = new CompletableFuture<>();
                entryContentsFuture.whenComplete((r, e) -> FutureHelpers.complete(resultEntryFuture, currentEntry, e));
                return resultEntryFuture;
            }
        }

        return null;
    }

    //endregion
}

