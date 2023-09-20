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
package io.pravega.segmentstore.storage.mocks;

import com.google.common.base.Preconditions;
import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.util.CloseableIterator;
import io.pravega.common.util.CompositeArrayView;
import io.pravega.segmentstore.storage.DataLogDisabledException;
import io.pravega.segmentstore.storage.DataLogInitializationException;
import io.pravega.segmentstore.storage.DataLogWriterNotPrimaryException;
import io.pravega.segmentstore.storage.DurableDataLog;
import io.pravega.segmentstore.storage.DurableDataLogException;
import io.pravega.segmentstore.storage.LogAddress;
import io.pravega.segmentstore.storage.QueueStats;
import io.pravega.segmentstore.storage.ReadOnlyLogMetadata;
import io.pravega.segmentstore.storage.ThrottleSourceListener;
import io.pravega.segmentstore.storage.WriteSettings;
import io.pravega.segmentstore.storage.WriteTooLongException;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.time.Duration;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

/**
 * In-Memory Mock for DurableDataLog. Contents is destroyed when object is garbage collected.
 */
@ThreadSafe
class InMemoryDurableDataLog implements DurableDataLog {
    static final Supplier<Duration> DEFAULT_APPEND_DELAY_PROVIDER = () -> Duration.ZERO; // No delay.
    private final EntryCollection entries;
    private final String clientId;
    private final ScheduledExecutorService executorService;
    private final Supplier<Duration> appendDelayProvider;
    @GuardedBy("entries")
    private long offset;
    @GuardedBy("entries")
    private long epoch;
    private boolean closed;
    private boolean initialized;

    InMemoryDurableDataLog(EntryCollection entries, ScheduledExecutorService executorService) {
        this(entries, DEFAULT_APPEND_DELAY_PROVIDER, executorService);
    }

    InMemoryDurableDataLog(EntryCollection entries, Supplier<Duration> appendDelayProvider, ScheduledExecutorService executorService) {
        this.entries = Preconditions.checkNotNull(entries, "entries");
        this.appendDelayProvider = Preconditions.checkNotNull(appendDelayProvider, "appendDelayProvider");
        this.executorService = Preconditions.checkNotNull(executorService, "executorService");
        this.offset = Long.MIN_VALUE;
        this.epoch = Long.MIN_VALUE;
        this.clientId = UUID.randomUUID().toString();
    }

    //region DurableDataLog Implementation

    @Override
    public void close() {
        if (!this.closed) {
            try {
                this.entries.releaseLock(this.clientId);
            } catch (DataLogWriterNotPrimaryException ex) {
                // Nothing. Just let it go.
            }

            this.closed = true;
        }
    }

    @Override
    public void initialize(Duration timeout) throws DataLogInitializationException {
        long newEpoch = this.entries.acquireLock(this.clientId);
        synchronized (this.entries) {
            this.epoch = newEpoch;
            Entry last = this.entries.getLast();
            if (last == null) {
                this.offset = 0;
            } else {
                this.offset = last.sequenceNumber + last.data.length;
            }
        }

        this.initialized = true;
    }

    @Override
    public void enable() {
        Exceptions.checkNotClosed(this.closed, this);
        Preconditions.checkState(!this.initialized, "InMemoryDurableDataLog is initialized; cannot enable.");
        synchronized (this.entries) {
            this.entries.enable();
        }
    }

    @Override
    public void disable() throws DurableDataLogException {
        ensurePreconditions();
        synchronized (this.entries) {
            this.entries.disable(this.clientId);
        }

        close();
    }

    @Override
    public WriteSettings getWriteSettings() {
        return new WriteSettings(this.entries.getMaxAppendSize(), Duration.ofMinutes(1), Integer.MAX_VALUE);
    }

    @Override
    public ReadOnlyLogMetadata loadMetadata() throws DataLogInitializationException {
        return null;
    }

    @Override
    public long getEpoch() {
        ensurePreconditions();
        synchronized (this.entries) {
            return this.epoch;
        }
    }

    @Override
    public void overrideEpoch(long epoch) throws DurableDataLogException {
        synchronized (this.entries) {
            this.epoch = epoch;
        }
    }

    @Override
    public QueueStats getQueueStatistics() {
        // InMemory DurableDataLog has almost infinite bandwidth, so no need to complicate ourselves with this.
        return QueueStats.DEFAULT;
    }

    @Override
    public void registerQueueStateChangeListener(ThrottleSourceListener listener) {
        // No-op (because getQueueStatistics() doesn't return anything interesting).
    }

    @Override
    public CompletableFuture<LogAddress> append(CompositeArrayView data, Duration timeout) {
        ensurePreconditions();
        if (data.getLength() > getWriteSettings().getMaxWriteLength()) {
            return Futures.failedFuture(new WriteTooLongException(data.getLength(), getWriteSettings().getMaxWriteLength()));
        }

        CompletableFuture<LogAddress> result;
        try {
            Entry entry = new Entry(data);
            synchronized (this.entries) {
                entry.sequenceNumber = this.offset;
                this.entries.add(entry, clientId);

                // Only update internals after a successful add.
                this.offset += entry.data.length;
            }
            result = CompletableFuture.completedFuture(new InMemoryLogAddress(entry.sequenceNumber));
        } catch (Throwable ex) {
            return Futures.failedFuture(ex);
        }

        Duration delay = this.appendDelayProvider.get();
        if (delay.compareTo(Duration.ZERO) <= 0) {
            // No delay, execute right away.
            return result;
        } else {
            // Schedule the append after the given delay.
            return result.thenComposeAsync(
                    logAddress -> Futures.delayedFuture(delay, this.executorService)
                                         .thenApply(ignored -> logAddress),
                    this.executorService);
        }
    }

    @Override
    public CompletableFuture<Void> truncate(LogAddress upToAddress, Duration timeout) {
        ensurePreconditions();
        return CompletableFuture.runAsync(() -> {
            try {
                synchronized (this.entries) {
                    this.entries.truncate(upToAddress.getSequence(), this.clientId);
                }
            } catch (DataLogWriterNotPrimaryException ex) {
                throw new CompletionException(ex);
            }
        }, this.executorService);
    }

    @Override
    public CloseableIterator<ReadItem, DurableDataLogException> getReader() throws DurableDataLogException {
        ensurePreconditions();
        return new ReadResultIterator(this.entries.iterator());
    }

    //endregion

    private void ensurePreconditions() {
        Exceptions.checkNotClosed(this.closed, this);
        Preconditions.checkState(this.initialized, "InMemoryDurableDataLog is not initialized.");
    }

    //region ReadResultIterator

    @RequiredArgsConstructor
    private static class ReadResultIterator implements CloseableIterator<ReadItem, DurableDataLogException> {
        private final Iterator<Entry> entryIterator;

        @Override
        public ReadItem getNext() throws DurableDataLogException {
            if (this.entryIterator.hasNext()) {
                return new ReadResultItem(this.entryIterator.next());
            }

            return null;
        }

        @Override
        public void close() {

        }
    }

    //endregion

    //region ReadResultItem

    private static class ReadResultItem implements DurableDataLog.ReadItem {
        private final byte[] payload;
        @Getter
        private final LogAddress address;

        ReadResultItem(Entry entry) {
            this.payload = entry.data;
            this.address = new InMemoryLogAddress(entry.sequenceNumber);
        }

        @Override
        public InputStream getPayload() {
            return new ByteArrayInputStream(this.payload);
        }

        @Override
        public int getLength() {
            return this.payload.length;
        }

        @Override
        public String toString() {
            return String.format("Address = %s, Length = %d", this.address, this.payload.length);
        }
    }

    //endregion

    //region EntryCollection

    static class EntryCollection {
        private final LinkedList<Entry> entries;
        private final AtomicReference<String> writeLock;
        private final AtomicLong epoch;
        private final AtomicBoolean enabled;
        private final int maxAppendSize;

        EntryCollection() {
            this(1024 * 1024 - 8 * 1024);
        }

        EntryCollection(int maxAppendSize) {
            this.entries = new LinkedList<>();
            this.writeLock = new AtomicReference<>();
            this.epoch = new AtomicLong();
            this.maxAppendSize = maxAppendSize;
            this.enabled = new AtomicBoolean(true);
        }

        void enable() {
            if (!this.enabled.compareAndSet(false, true)) {
                throw new IllegalStateException("Log already enabled.");
            }
        }

        void disable(String clientId) throws DataLogWriterNotPrimaryException {
            ensureLock(clientId);
            if (!this.enabled.compareAndSet(true, false)) {
                throw new IllegalStateException("Log already disabled.");
            }
        }

        public void add(Entry entry, String clientId) throws DataLogWriterNotPrimaryException {
            ensureLock(clientId);
            ensureEnabled();
            synchronized (this.entries) {
                this.entries.add(entry);
            }
        }

        int getMaxAppendSize() {
            return this.maxAppendSize;
        }

        Entry getLast() {
            synchronized (this.entries) {
                return this.entries.peekLast();
            }
        }

        void truncate(long upToSequence, String clientId) throws DataLogWriterNotPrimaryException {
            ensureLock(clientId);
            ensureEnabled();
            synchronized (this.entries) {
                while (!this.entries.isEmpty()) {
                    Entry first = this.entries.peekFirst();
                    if (first.getSequenceNumber() <= upToSequence) {
                        this.entries.removeFirst();
                    } else {
                        break;
                    }
                }
            }
        }

        Iterator<Entry> iterator() {
            ensureEnabled();
            synchronized (this.entries) {
                return new LinkedList<>(this.entries).iterator();
            }
        }

        long acquireLock(String clientId) throws DataLogDisabledException {
            Exceptions.checkNotNullOrEmpty(clientId, "clientId");
            if (!this.enabled.get()) {
                throw new DataLogDisabledException("Log is disabled; cannot acquire lock.");
            }

            this.writeLock.set(clientId);
            return this.epoch.incrementAndGet();
        }

        void releaseLock(String clientId) throws DataLogWriterNotPrimaryException {
            Exceptions.checkNotNullOrEmpty(clientId, "clientId");
            if (!writeLock.compareAndSet(clientId, null)) {
                throw new DataLogWriterNotPrimaryException(
                        "Unable to release exclusive write lock because the current client does not own it. Current owner: "
                                + clientId);
            }
        }

        private void ensureLock(String clientId) throws DataLogWriterNotPrimaryException {
            Exceptions.checkNotNullOrEmpty(clientId, "clientId");
            String existingLockOwner = this.writeLock.get();
            if (existingLockOwner != null && !existingLockOwner.equals(clientId)) {
                throw new DataLogWriterNotPrimaryException("Unable to perform operation because the write lock is owned by a different client " + clientId);
            }
        }

        private void ensureEnabled() {
            Preconditions.checkState(this.enabled.get(), "Log not enabled.");
        }
    }

    //endregion

    //region Entry

    static class Entry {
        @Getter
        long sequenceNumber = -1;
        final byte[] data;

        Entry(CompositeArrayView inputData) {
            this.data = inputData.getCopy();
        }

        @Override
        public String toString() {
            return String.format("SequenceNumber = %d, Length = %d", sequenceNumber, data.length);
        }
    }

    //endregion

    //region InMemoryLogAddress

    static class InMemoryLogAddress extends LogAddress {
        InMemoryLogAddress(long sequence) {
            super(sequence);
        }

        @Override
        public int hashCode() {
            return Long.hashCode(getSequence());
        }

        @Override
        public boolean equals(Object other) {
            if (other instanceof InMemoryLogAddress) {
                return this.getSequence() == ((InMemoryLogAddress) other).getSequence();
            }

            return false;
        }
    }

    //endregion
}
