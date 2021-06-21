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
package io.pravega.test.integration.selftest.adapters;

import com.google.common.base.Preconditions;
import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.CancellationToken;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.util.ByteArraySegment;
import io.pravega.segmentstore.contracts.ReadResult;
import io.pravega.segmentstore.contracts.ReadResultEntry;
import io.pravega.segmentstore.contracts.ReadResultEntryType;
import io.pravega.segmentstore.contracts.SegmentProperties;
import io.pravega.segmentstore.contracts.StreamSegmentNotExistsException;
import io.pravega.segmentstore.contracts.StreamSegmentSealedException;
import io.pravega.segmentstore.contracts.StreamSegmentStore;
import io.pravega.segmentstore.server.reading.AsyncReadResultHandler;
import io.pravega.segmentstore.server.reading.AsyncReadResultProcessor;
import io.pravega.segmentstore.storage.ReadOnlyStorage;
import io.pravega.segmentstore.storage.Storage;
import io.pravega.test.integration.selftest.Event;
import io.pravega.test.integration.selftest.TestConfig;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.time.Duration;
import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.val;

/**
 * StoreReader that can read directly from a StreamSegmentStore.
 */
@ThreadSafe
class SegmentStoreReader implements StoreReader {
    //region Members

    // We don't want to immediately truncate the Segment in Storage as that may interfere with the normal functioning of
    // the StorageWriter and cause unintended errors. We are giving it a grace buffer and only truncate it at this delta
    // below the currently read segment offset.
    private static final int TRUNCATE_BUFFER_BYTES = 32 * 1024 * 1024;
    private final TestConfig testConfig;
    private final StreamSegmentStore store;
    private final ReadOnlyStorage storage;
    private final ScheduledExecutorService executor;

    //endregion

    //region Constructor

    /**
     * Creates a new instance oif the SegmentStoreReader class.
     *
     * @param testConfig The Test Configuration to use.
     * @param store      A reference to the StreamSegmentStore to use.
     * @param storage    A reference to the Storage to use for StorageDirect reads.
     * @param executor   An Executor to use for async operations.
     */
    SegmentStoreReader(TestConfig testConfig, StreamSegmentStore store, ReadOnlyStorage storage, ScheduledExecutorService executor) {
        this.testConfig = Preconditions.checkNotNull(testConfig, "testConfig");
        this.store = Preconditions.checkNotNull(store, "store");
        this.storage = Preconditions.checkNotNull(storage, "storage");
        this.executor = Preconditions.checkNotNull(executor, "executor");
    }

    //endregion

    //region StoreReader Implementation

    @Override
    public CompletableFuture<Void> readAll(String segmentName, Consumer<ReadItem> eventHandler, CancellationToken cancellationToken) {
        Exceptions.checkNotNullOrEmpty(segmentName, "segmentName");
        Preconditions.checkNotNull(eventHandler, "eventHandler");
        if (cancellationToken == null) {
            cancellationToken = CancellationToken.NONE;
        }

        return new SegmentReader(segmentName, eventHandler, cancellationToken).run();
    }

    @Override
    public CompletableFuture<ReadItem> readExact(String segmentName, Object address) {
        Exceptions.checkNotNullOrEmpty(segmentName, "segmentName");
        Preconditions.checkArgument(address instanceof Address, "Unexpected address type.");
        Address a = (Address) address;
        return this.store
                .read(segmentName, a.offset, a.length, this.testConfig.getTimeout())
                .thenApplyAsync(readResult -> {
                    byte[] data = new byte[a.length];
                    readResult.readRemaining(data, this.testConfig.getTimeout());
                    return new SegmentStoreReadItem(new Event(new ByteArraySegment(data), 0), address);
                }, this.executor);
    }

    @Override
    public CompletableFuture<Void> readAllStorage(String segmentName, Consumer<Event> eventHandler, CancellationToken cancellationToken) {
        Exceptions.checkNotNullOrEmpty(segmentName, "segmentName");
        Preconditions.checkNotNull(eventHandler, "eventHandler");
        if (cancellationToken == null) {
            cancellationToken = CancellationToken.NONE;
        }

        return new StorageReader(segmentName, eventHandler, cancellationToken).run();
    }

    //endregion

    //region StorageReader

    /**
     * Reads all data from Storage for a given Segment.
     */
    @RequiredArgsConstructor
    private class StorageReader {
        private final Duration waitDuration = Duration.ofMillis(500);
        private final String segmentName;
        private final Consumer<Event> eventHandler;
        private final CancellationToken cancellationToken;
        @GuardedBy("readBuffer")
        private final TruncateableArray readBuffer = new TruncateableArray();
        @GuardedBy("readBuffer")
        private long readBufferOffset;

        /**
         * Runs in a loop as long as the CancellationToken is not cancelled. Checks, on a periodic basis, if the Segment's
         * length changed in Storage. If so, it reads the outstanding data and interprets it as an ordered sequence of Events,
         * which are then passed on via the given event handler.
         */
        CompletableFuture<Void> run() {
            return Futures.loop(
                    () -> !this.cancellationToken.isCancellationRequested(),
                    () -> SegmentStoreReader.this.storage
                            .getStreamSegmentInfo(segmentName, SegmentStoreReader.this.testConfig.getTimeout())
                            .thenComposeAsync(this::performRead, SegmentStoreReader.this.executor),
                    SegmentStoreReader.this.executor);
        }

        /**
         * Executes a read from Storage using the current, given state of the Segment.
         */
        private CompletableFuture<Void> performRead(SegmentProperties segmentInfo) {
            // Calculate the last offset we read up to.
            long lastReadOffset;
            synchronized (this.readBuffer) {
                lastReadOffset = this.readBufferOffset + this.readBuffer.getLength();
            }

            long diff = segmentInfo.getLength() - lastReadOffset;
            if (diff <= 0) {
                if (segmentInfo.isSealed()) {
                    // Segment has been sealed; no point in looping anymore.
                    return Futures.failedFuture(new StreamSegmentSealedException(this.segmentName));
                } else {
                    // No change in the segment.
                    return Futures.delayedFuture(this.waitDuration, SegmentStoreReader.this.executor);
                }
            } else {
                byte[] buffer = new byte[(int) Math.min(Integer.MAX_VALUE, diff)];
                return SegmentStoreReader.this.storage
                        .openRead(segmentName)
                        .thenCompose(handle -> SegmentStoreReader.this.storage.read(
                                handle, lastReadOffset, buffer, 0, buffer.length, SegmentStoreReader.this.testConfig.getTimeout()))
                        .thenComposeAsync(bytesRead -> {
                            processRead(buffer, bytesRead);
                            return truncateIfPossible(segmentInfo.getLength());
                        }, SegmentStoreReader.this.executor);
            }
        }

        private void processRead(byte[] buffer, int bytesRead) {
            val events = new ArrayList<Event>();
            synchronized (this.readBuffer) {
                this.readBuffer.append(new ByteArrayInputStream(buffer), bytesRead);

                // Drain the read buffer (as much as we can) by extracting Events out of it.
                while (this.readBuffer.getLength() > 0) {
                    try {
                        val e = new Event(this.readBuffer, 0);
                        events.add(e);
                        this.readBuffer.truncate(e.getTotalLength());
                        this.readBufferOffset += e.getTotalLength();
                    } catch (IndexOutOfBoundsException ex) {
                        break;
                    }
                }
            }

            events.forEach(this.eventHandler);
        }

        private CompletableFuture<Void> truncateIfPossible(long offset) {
            if (SegmentStoreReader.this.storage instanceof Storage) {
                Storage s = (Storage) SegmentStoreReader.this.storage;
                long truncateOffset = offset - TRUNCATE_BUFFER_BYTES;
                if (s.supportsTruncation() && truncateOffset > 0) {
                    return s.openWrite(this.segmentName)
                            .thenCompose(handle -> s.truncate(handle, truncateOffset, SegmentStoreReader.this.testConfig.getTimeout()));
                }
            }

            return CompletableFuture.completedFuture(null);
        }
    }

    //endregion

    //region SegmentReader

    /**
     * Reads the entire Segment from the SegmentStore.
     */
    @RequiredArgsConstructor
    private class SegmentReader {
        private final String segmentName;
        private final Consumer<ReadItem> eventHandler;
        private final CancellationToken cancellationToken;
        @GuardedBy("readBuffer")
        private final TruncateableArray readBuffer = new TruncateableArray();
        @GuardedBy("readBuffer")
        private long startOffset = 0;
        @GuardedBy("readBuffer")
        private boolean isClosed = false;

        /**
         * Runs in a loop as long as the CancellationToken is not cancelled. Asynchronously invokes the given callback
         * whenever there is new data available, which is interpreted as Events.
         */
        CompletableFuture<Void> run() {
            return Futures.loop(
                    this::canRun,
                    () -> SegmentStoreReader.this.store
                            .read(segmentName, getReadOffset(), Integer.MAX_VALUE, SegmentStoreReader.this.testConfig.getTimeout())
                            .thenComposeAsync(this::processReadResult, SegmentStoreReader.this.executor)
                            .thenCompose(v -> SegmentStoreReader.this.store
                                    .getStreamSegmentInfo(segmentName, SegmentStoreReader.this.testConfig.getTimeout()))
                            .handle(this::readCompleteCallback),
                    SegmentStoreReader.this.executor);
        }

        private boolean canRun() {
            synchronized (this.readBuffer) {
                return !this.cancellationToken.isCancellationRequested() && !this.isClosed;
            }
        }

        private CompletableFuture<Void> processReadResult(ReadResult readResult) {
            CompletableFuture<Void> result = new CompletableFuture<>();
            AsyncReadResultProcessor.process(readResult,
                    new ReadResultHandler(this::processRead, this.cancellationToken, result),
                    SegmentStoreReader.this.executor);
            return result;
        }

        private void processRead(InputStream data, long segmentOffset, int length) {
            val events = new ArrayList<SegmentStoreReadItem>();
            synchronized (this.readBuffer) {
                // Add data to read buffer.
                long expectedOffset = getReadOffset();
                Preconditions.checkArgument(segmentOffset == expectedOffset, "Out-of-order read for Segment '%s'. " +
                        "Expected offset %s, got %s.", this.segmentName, expectedOffset, segmentOffset);
                this.readBuffer.append(data, length);

                // Drain the read buffer (as much as we can) by extracting Events out of it.
                while (this.readBuffer.getLength() > 0) {
                    try {
                        val e = new Event(this.readBuffer, 0);
                        events.add(new SegmentStoreReadItem(e, new Address(this.startOffset, e.getTotalLength())));
                        this.readBuffer.truncate(e.getTotalLength());
                        this.startOffset += e.getTotalLength();
                    } catch (IndexOutOfBoundsException ex) {
                        break;
                    }
                }
            }

            events.forEach(this.eventHandler);
        }

        private long getReadOffset() {
            synchronized (this.readBuffer) {
                return this.startOffset + this.readBuffer.getLength();
            }
        }

        @SneakyThrows
        private Void readCompleteCallback(SegmentProperties r, Throwable ex) {
            if (ex != null) {
                ex = Exceptions.unwrap(ex);
                if (ex instanceof StreamSegmentNotExistsException) {
                    // Cannot continue anymore (segment has been deleted).
                    synchronized (this.readBuffer) {
                        this.isClosed = true;
                    }
                } else {
                    // Unexpected exception.
                    throw ex;
                }
            } else if (r.isSealed() && getReadOffset() >= r.getLength()) {
                // Cannot continue anymore (segment has been sealed and we reached its end).
                synchronized (this.readBuffer) {
                    this.isClosed = true;
                }
            }

            return null;
        }
    }

    //endregion

    //region ReadResultHandler

    /**
     * Handler for the AsyncReadResultProcessor that processes the Segment read.
     */
    @RequiredArgsConstructor
    private static class ReadResultHandler implements AsyncReadResultHandler {
        private final ReadCallback callback;
        private final CancellationToken cancellationToken;
        private final CompletableFuture<Void> completion;
        private final AtomicLong readLength = new AtomicLong();

        @Override
        public boolean shouldRequestContents(ReadResultEntryType entryType, long streamSegmentOffset) {
            return true;
        }

        @Override
        public boolean processEntry(ReadResultEntry entry) {
            if (!entry.getContent().isDone()) {
                // Make sure we only request content if it's not already available.
                entry.requestContent(getRequestContentTimeout());
            }

            val contents = entry.getContent().join();
            this.readLength.addAndGet(contents.getLength());
            this.callback.accept(contents.getReader(), entry.getStreamSegmentOffset(), contents.getLength());
            return !this.cancellationToken.isCancellationRequested();
        }

        @Override
        public void processError(Throwable cause) {
            cause = Exceptions.unwrap(cause);
            if (cause instanceof StreamSegmentSealedException) {
                processResultComplete();
            } else {
                this.completion.completeExceptionally(cause);
            }
        }

        @Override
        public void processResultComplete() {
            this.completion.complete(null);
        }

        @Override
        public Duration getRequestContentTimeout() {
            return Duration.ofSeconds(10);
        }
    }

    //endregion

    //region Other Nested Classes

    @Data
    private static class SegmentStoreReadItem implements ReadItem {
        private final Event event;
        private final Object address;

        @Override
        public String toString() {
            return String.format("Event = [%s], Address = [%s]", this.event, this.address);
        }
    }

    @RequiredArgsConstructor
    private static class Address {
        private final long offset;
        private final int length;

        @Override
        public String toString() {
            return String.format("%d,%d", this.offset, this.length);
        }
    }

    @FunctionalInterface
    private interface ReadCallback {
        void accept(InputStream data, long segmentOffset, int length);
    }

    //endregion
}
