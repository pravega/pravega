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

package com.emc.pravega.service.storage.mocks;

import com.emc.pravega.common.Exceptions;
import com.emc.pravega.common.io.StreamHelpers;
import com.emc.pravega.common.util.CloseableIterator;
import com.emc.pravega.common.util.TruncateableList;
import com.emc.pravega.service.storage.DataLogWriterNotPrimaryException;
import com.emc.pravega.service.storage.DurableDataLog;
import com.emc.pravega.service.storage.DurableDataLogException;
import com.emc.pravega.service.storage.LogAddress;
import com.google.common.base.Preconditions;

import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.Iterator;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicReference;

/**
 * In-Memory Mock for DurableDataLog. Contents is destroyed when object is garbage collected.
 */
class InMemoryDurableDataLog implements DurableDataLog {
    private final EntryCollection entries;
    private final String clientId;
    private long offset;
    private long lastAppendSequence;
    private boolean closed;
    private boolean initialized;

    InMemoryDurableDataLog(EntryCollection entries) {
        Preconditions.checkNotNull(entries, "entries");
        this.entries = entries;
        this.offset = Long.MIN_VALUE;
        this.lastAppendSequence = Long.MIN_VALUE;
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
    public void initialize(Duration timeout) {
        try {
            this.entries.acquireLock(this.clientId);
        } catch (DataLogWriterNotPrimaryException ex) {
            throw new CompletionException(ex);
        }

        Entry last = this.entries.getLast();
        if (last == null) {
            this.offset = 0;
            this.lastAppendSequence = -1;
        } else {
            this.offset = last.sequenceNumber + last.data.length;
            this.lastAppendSequence = last.sequenceNumber;
        }

        this.initialized = true;
    }

    @Override
    public int getMaxAppendLength() {
        ensurePreconditions();
        return this.entries.getMaxAppendSize();
    }

    @Override
    public long getLastAppendSequence() {
        ensurePreconditions();
        return this.lastAppendSequence;
    }

    @Override
    public CompletableFuture<LogAddress> append(InputStream data, Duration timeout) {
        ensurePreconditions();
        return CompletableFuture.supplyAsync(() -> {
            Entry entry;
            try {
                entry = new Entry(data);
                synchronized (this.entries) {
                    entry.sequenceNumber = this.offset;
                    this.entries.add(entry, clientId);

                    // Only update internals after a successful add.
                    this.offset += entry.data.length;
                    this.lastAppendSequence = entry.sequenceNumber;
                }
            } catch (DataLogWriterNotPrimaryException | IOException ex) {
                throw new CompletionException(ex);
            }

            return new InMemoryLogAddress(entry.sequenceNumber);
        });
    }

    @Override
    public CompletableFuture<Boolean> truncate(LogAddress upToAddress, Duration timeout) {
        ensurePreconditions();
        return CompletableFuture.supplyAsync(() -> {
            synchronized (this.entries) {
                try {
                    return this.entries.truncate(upToAddress.getSequence(), this.clientId) > 0;
                } catch (DataLogWriterNotPrimaryException ex) {
                    throw new CompletionException(ex);
                }
            }
        });
    }

    @Override
    public CloseableIterator<ReadItem, DurableDataLogException> getReader(long afterSequence) throws
            DurableDataLogException {
        ensurePreconditions();
        return new ReadResultIterator(this.entries.iterator(), afterSequence);
    }

    //endregion

    private void ensurePreconditions() {
        Exceptions.checkNotClosed(this.closed, this);
        Preconditions.checkState(this.initialized, "InMemoryDurableDataLog is not initialized.");
    }

    //region ReadResultIterator

    private static class ReadResultIterator implements CloseableIterator<ReadItem, DurableDataLogException> {
        private final Iterator<Entry> entryIterator;
        private final long afterSequence;

        ReadResultIterator(Iterator<Entry> entryIterator, long afterSequence) {
            this.entryIterator = entryIterator;
            this.afterSequence = afterSequence;
        }

        @Override
        public ReadItem getNext() throws DurableDataLogException {
            while (this.entryIterator.hasNext()) {
                Entry e = this.entryIterator.next();
                if (e.sequenceNumber <= afterSequence) {
                    continue;
                }

                return new ReadResultItem(e);
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
        private final LogAddress address;

        ReadResultItem(Entry entry) {
            this.payload = new byte[entry.data.length];
            System.arraycopy(entry.data, 0, this.payload, 0, this.payload.length);
            this.address = new InMemoryLogAddress(entry.sequenceNumber);
        }

        @Override
        public byte[] getPayload() {
            return this.payload;
        }

        @Override
        public LogAddress getAddress() {
            return this.address;
        }

        @Override
        public String toString() {
            return String.format("Address = %s, Length = %d", this.address, this.payload.length);
        }
    }

    //endregion

    //region EntryCollection

    static class EntryCollection {
        private final TruncateableList<Entry> entries;
        private final AtomicReference<String> writeLock;
        private final int maxAppendSize;

        EntryCollection() {
            this(1024 * 1024 - 8 * 1024);
        }

        EntryCollection(int maxAppendSize) {
            this.entries = new TruncateableList<>();
            this.writeLock = new AtomicReference<>();
            this.maxAppendSize = maxAppendSize;
        }

        public void add(Entry entry, String clientId) throws DataLogWriterNotPrimaryException {
            ensureLock(clientId);
            this.entries.add(entry);
        }

        int getMaxAppendSize() {
            return this.maxAppendSize;
        }

        Entry getLast() {
            return this.entries.getLast();
        }

        int truncate(long upToSequence, String clientId) throws DataLogWriterNotPrimaryException {
            ensureLock(clientId);
            return this.entries.truncate(e -> e.sequenceNumber <= upToSequence);
        }

        Iterator<Entry> iterator() {
            return this.entries.read(i -> true, Integer.MAX_VALUE);
        }

        void acquireLock(String clientId) throws DataLogWriterNotPrimaryException {
            Exceptions.checkNotNullOrEmpty(clientId, "clientId");
            synchronized (this.writeLock) {
                String existingLockOwner = this.writeLock.get();
                if (existingLockOwner != null) {
                    throw new DataLogWriterNotPrimaryException("Unable to acquire exclusive write lock because is " +
                            "already owned by " + clientId);
                }

                this.writeLock.set(clientId);
            }
        }

        void forceAcquireLock(String clientId) {
            Exceptions.checkNotNullOrEmpty(clientId, "clientId");
            synchronized (this.writeLock) {
                this.writeLock.set(clientId);
            }
        }

        void releaseLock(String clientId) throws DataLogWriterNotPrimaryException {
            Exceptions.checkNotNullOrEmpty(clientId, "clientId");
            synchronized (this.writeLock) {
                String existingLockOwner = this.writeLock.get();
                if (existingLockOwner == null || !existingLockOwner.equals(clientId)) {
                    throw new DataLogWriterNotPrimaryException("Unable to release exclusive write lock because the " +
                            "current client does not own it. Current owner: " + clientId);
                }

                this.writeLock.set(null);
            }
        }

        private void ensureLock(String clientId) throws DataLogWriterNotPrimaryException {
            Exceptions.checkNotNullOrEmpty(clientId, "clientId");
            String existingLockOwner = this.writeLock.get();
            if (existingLockOwner != null && !existingLockOwner.equals(clientId)) {
                throw new DataLogWriterNotPrimaryException("Unable to perform operation because the write lock is " +
                        "owned by a different client " + clientId);
            }
        }
    }

    //endregion

    //region Entry

    static class Entry {
        long sequenceNumber;
        final byte[] data;

        Entry(InputStream inputData) throws IOException {
            this.data = new byte[inputData.available()];
            StreamHelpers.readAll(inputData, this.data, 0, this.data.length);
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
