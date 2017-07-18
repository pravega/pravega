package io.pravega.test.integration.selftest;

import com.google.common.base.Preconditions;
import io.pravega.common.ExceptionHelpers;
import io.pravega.common.concurrent.CancellationToken;
import io.pravega.common.concurrent.FutureHelpers;
import io.pravega.common.util.ByteArraySegment;
import io.pravega.segmentstore.contracts.ReadResult;
import io.pravega.segmentstore.contracts.ReadResultEntry;
import io.pravega.segmentstore.contracts.ReadResultEntryType;
import io.pravega.segmentstore.contracts.SegmentProperties;
import io.pravega.segmentstore.contracts.StreamSegmentNotExistsException;
import io.pravega.segmentstore.contracts.StreamSegmentStore;
import io.pravega.segmentstore.server.reading.AsyncReadResultHandler;
import io.pravega.segmentstore.server.reading.AsyncReadResultProcessor;
import io.pravega.segmentstore.storage.ReadOnlyStorage;
import java.io.InputStream;
import java.time.Duration;
import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
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

    private static final Duration READ_TIMEOUT = Duration.ofSeconds(10);
    private final StreamSegmentStore store;
    private final ReadOnlyStorage storage;
    private final Executor executor;

    SegmentStoreReader(StreamSegmentStore store, ReadOnlyStorage storage, Executor executor) {
        this.store = Preconditions.checkNotNull(store, "store");
        this.storage = Preconditions.checkNotNull(storage, "storage");
        this.executor = Preconditions.checkNotNull(executor, "executor");
    }

    //region AutoCloseable Implementation

    @Override
    public void close() {

    }

    //endregion

    //region StoreReader Implementation

    @Override
    public CompletableFuture<Void> readAll(String segmentName, Consumer<ReadItem> eventHandler, CancellationToken cancellationToken) {
        return new SegmentReader(segmentName, eventHandler, cancellationToken, this.store, this.executor).run();
    }

    @Override
    public CompletableFuture<ReadItem> readExact(String segmentName, Object address) {
        Preconditions.checkArgument(address instanceof Address, "Unexpected address type.");
        Address a = (Address) address;
        AtomicReference<ReadItem> result = new AtomicReference<>();
        return this.store
                .read(segmentName, a.offset, a.length, READ_TIMEOUT)
                .thenComposeAsync(readResult -> {
                    SegmentReader reader = new SegmentReader(segmentName, result::set, null, this.store, this.executor);
                    reader.setLimits(a.getOffset(), a.getOffset() + a.getLength());
                    return reader.run();
                }, this.executor)
                .thenApply(v -> result.get());
    }

    @Override
    public CompletableFuture<ReadItem> readStorage(String segmentName, Object address) {
        Preconditions.checkArgument(address instanceof Address, "Unexpected address type.");
        Address a = (Address) address;
        byte[] buffer = new byte[a.getLength()];
        return this.storage
                .openRead(segmentName)
                .thenCompose(handle -> this.storage.read(handle, a.offset, buffer, 0, a.length, READ_TIMEOUT))
                .thenApply(bytesRead -> {
                    assert bytesRead == a.getLength() : "Unexpected number of bytes read.";
                    Event e = new Event(new ByteArraySegment(buffer), 0);
                    return new SegmentStoreReadItem(e, address);
                });
    }

    //endregion

    //region SegmentReader

    @RequiredArgsConstructor
    private static class SegmentReader {
        private final String segmentName;
        private final Consumer<ReadItem> eventHandler;
        private final CancellationToken cancellationToken;
        private final StreamSegmentStore store;
        private final Executor executor;
        @GuardedBy("readBuffer")
        private final TruncateableArray readBuffer = new TruncateableArray();
        @GuardedBy("readBuffer")
        private long startOffset = 0;
        @GuardedBy("readBuffer")
        private long endOffset = Long.MAX_VALUE;

        void setLimits(long startOffset, long endOffset) {
            Preconditions.checkArgument(startOffset <= endOffset, "startOffset must be smaller than endOffset");
            synchronized (this.readBuffer) {
                this.startOffset = startOffset;
                this.endOffset = endOffset;
            }
        }

        CompletableFuture<Void> run() {
            return FutureHelpers.loop(
                    this::canRun,
                    () -> this.store
                            .read(segmentName, getReadOffset(), getReadLength(), READ_TIMEOUT)
                            .thenComposeAsync(this::processReadResult, this.executor)
                            .thenCompose(v -> this.store.getStreamSegmentInfo(segmentName, false, READ_TIMEOUT))
                            .handle(this::readCompleteCallback),
                    this.executor);
        }

        private CompletableFuture<Void> processReadResult(ReadResult readResult) {
            CompletableFuture<Void> result = new CompletableFuture<>();
            AsyncReadResultProcessor.process(readResult, new ReadResultHandler(this::processRead, this.cancellationToken, result), this.executor);
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
                        events.add(new SegmentStoreReadItem(e, this.startOffset));
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

        private int getReadLength() {
            synchronized (this.readBuffer) {
                return (int) Math.min(Integer.MAX_VALUE, this.endOffset - getReadOffset());
            }
        }

        private boolean canRun() {
            if (this.cancellationToken.isCancellationRequested()) {
                return false;
            }

            synchronized (this.readBuffer) {
                return getReadOffset() < this.endOffset;
            }
        }

        @SneakyThrows
        private Void readCompleteCallback(SegmentProperties r, Throwable ex) {
            if (ex != null) {
                ex = ExceptionHelpers.getRealException(ex);
                if (ex instanceof StreamSegmentNotExistsException) {
                    // Cannot continue anymore (segment has been deleted).
                    this.cancellationToken.requestCancellation();
                } else {
                    // Unexpected exception.
                    throw ex;
                }
            } else if (r.isSealed() && getReadOffset() >= r.getLength()) {
                // Cannot continue anymore (segment has been sealed and we reached its end).
                this.cancellationToken.requestCancellation();
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
                entry.requestContent(READ_TIMEOUT);
            }

            val contents = entry.getContent().join();
            this.readLength.addAndGet(contents.getLength());
            this.callback.accept(contents.getData(), entry.getStreamSegmentOffset(), contents.getLength());
            return !this.cancellationToken.isCancellationRequested();
        }

        @Override
        public void processError(Throwable cause) {
            cause = ExceptionHelpers.getRealException(cause);
            this.completion.completeExceptionally(cause);
        }

        @Override
        public void processResultComplete() {
            this.completion.complete(null);
        }

        @Override
        public Duration getRequestContentTimeout() {
            return READ_TIMEOUT;
        }
    }

    //endregion

    @Data
    private static class SegmentStoreReadItem implements ReadItem {
        private final Event event;
        private final Object address;
    }

    @Data
    private static class Address {
        private final long offset;
        private final int length;

        @Override
        public String toString() {
            return String.format("Offset = %d, Length = %d", this.offset, this.length);
        }
    }

    @FunctionalInterface
    private interface ReadCallback {
        void accept(InputStream data, long segmentOffset, int length);
    }
}
