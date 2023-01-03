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
package io.pravega.segmentstore.server.reading;

import com.google.common.base.Preconditions;
import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.util.BufferView;
import io.pravega.segmentstore.contracts.ReadResult;
import io.pravega.segmentstore.contracts.ReadResultEntry;
import io.pravega.segmentstore.contracts.ReadResultEntryType;
import java.time.Duration;
import java.util.List;
import java.util.Vector;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

/**
 * An Asynchronous processor for ReadResult objects. Attaches to a ReadResult and executes a callback using an Executor
 * for each ReadResultEntry returned by the result. This class is suitable for handling long-poll reads, as it does not
 * hog any threads while waiting for such future reads to become available. It only uses (a thread from) the Executor
 * when the data for a read becomes available, at which point it executes the handler on such a thread.
 *
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
        this.readResult.setMaxReadAtOnce(this.entryHandler.getMaxReadAtOnce());
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

    /**
     * Processes the given {@link ReadResult} and returns the contents as an {@link BufferView}.
     *
     * @param readResult            The {@link ReadResult} to process.
     * @param executor              An Executor to run asynchronous tasks on.
     * @param requestContentTimeout Timeout for each call to {@link ReadResultEntry#requestContent(Duration)}, for those
     *                              {@link ReadResultEntry} instances that are not already cached in memory.
     * @return A CompletableFuture that, when completed, will contain an {@link BufferView} with the requested data.
     */
    public static CompletableFuture<BufferView> processAll(ReadResult readResult, Executor executor, Duration requestContentTimeout) {
        ProcessAllHandler handler = new ProcessAllHandler(requestContentTimeout);
        process(readResult, handler, executor);
        return handler.result;
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
                this.entryHandler.processError(Exceptions.unwrap(failureCause));
            }
        }
    }

    //endregion

    //region Processing

    private void processResult(Executor executor) {
        // Process the result, one entry at a time, until one of the stopping conditions occurs.
        AtomicBoolean shouldContinue = new AtomicBoolean(true);
        Futures
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
                                this.readResult.setMaxReadAtOnce(this.entryHandler.getMaxReadAtOnce());
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
            CompletableFuture<BufferView> entryContentsFuture = currentEntry.getContent();
            if (entryContentsFuture.isDone()) {
                // Result is readily available.
                return CompletableFuture.completedFuture(currentEntry);
            } else if (this.entryHandler.shouldRequestContents(currentEntry.getType(), currentEntry.getStreamSegmentOffset())) {
                // ReadResultEntry that does not have data readily available and we were instructed to request the content.
                currentEntry.requestContent(this.entryHandler.getRequestContentTimeout());
                return entryContentsFuture.thenApply(v -> currentEntry);
            }
        }

        return null;
    }

    //endregion

    @RequiredArgsConstructor
    private static class ProcessAllHandler implements AsyncReadResultHandler {
        @Getter
        private final Duration requestContentTimeout;
        private final List<BufferView> parts = new Vector<>();
        private final CompletableFuture<BufferView> result = new CompletableFuture<>();

        @Override
        public boolean shouldRequestContents(ReadResultEntryType entryType, long streamSegmentOffset) {
            return true;
        }

        @Override
        public boolean processEntry(ReadResultEntry entry) {
            this.parts.add(entry.getContent().join());
            return true;
        }

        @Override
        public void processError(Throwable cause) {
            this.result.completeExceptionally(cause);
        }

        @Override
        public void processResultComplete() {
            this.result.complete(BufferView.wrap(this.parts));
        }
    }
}

