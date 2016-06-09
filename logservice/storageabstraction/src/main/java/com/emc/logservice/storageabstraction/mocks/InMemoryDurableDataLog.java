package com.emc.logservice.storageabstraction.mocks;

import com.emc.logservice.common.*;
import com.emc.logservice.storageabstraction.DurableDataLog;

import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

/**
 * In-Memory Mock for DurableDataLog. Contents is destroyed when object is garbage collected.
 */
class InMemoryDurableDataLog implements DurableDataLog {
    private final EntryCollection entries;
    private long offset;
    private long lastAppendSequence;
    private boolean closed;
    private boolean initialized;

    public InMemoryDurableDataLog(EntryCollection entries) {
        Exceptions.throwIfNull(entries, "entries");
        this.entries = entries;
        this.offset = Long.MIN_VALUE;
        this.lastAppendSequence = Long.MIN_VALUE;
    }

    //region DurableDataLog Implementation

    @Override
    public void close() {
        this.closed = true;
    }

    @Override
    public CompletableFuture<Void> initialize(Duration timeout) {
        if (this.entries.size() == 0) {
            this.offset = 0;
            this.lastAppendSequence = Long.MIN_VALUE;
        }
        else {
            Entry last = this.entries.getLast();
            this.offset = last.offset + last.data.length;
            this.lastAppendSequence = last.offset;
        }

        this.initialized = true;
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public int getMaxAppendLength() {
        ensurePreconditions();
        return 1024 * 1024 - 8 * 1024;
    }

    @Override
    public long getLastAppendSequence() {
        ensurePreconditions();
        return this.lastAppendSequence;
    }

    @Override
    public CompletableFuture<Long> append(InputStream data, Duration timeout) {
        ensurePreconditions();
        return CompletableFuture.supplyAsync(() -> {
            Entry entry;
            try {
                entry = new Entry(data);
            }
            catch (IOException ex) {
                throw new CompletionException(ex);
            }

            synchronized (this.entries) {
                entry.offset = this.offset;
                this.offset += entry.data.length;
                this.entries.add(entry);
                this.lastAppendSequence = entry.offset;
            }

            return entry.offset;
        });
    }

    @Override
    public CompletableFuture<Void> truncate(long offset, Duration timeout) {
        ensurePreconditions();

        return CompletableFuture.runAsync(() -> {
            synchronized (this.entries) {
                while (this.entries.size() > 0 && this.entries.getFirst().offset + this.entries.getFirst().data.length <= offset) {
                    this.entries.removeFirst();
                }
            }
        });
    }

    @Override
    public AsyncIterator<ReadItem> getReader(long afterSequence) {
        ensurePreconditions();
        return new ReadResultIterator(this.entries.iterator(), afterSequence);
    }

    //endregion

    private void ensurePreconditions() {
        Exceptions.throwIfClosed(this.closed, this);
        Exceptions.throwIfIllegalState(this.initialized, "InMemoryDurableDataLog is not initialized.");
    }

    //region ReadResultIterator

    private static class ReadResultIterator implements AsyncIterator<ReadItem> {
        private final Iterator<Entry> entryIterator;
        private final long afterSequence;

        public ReadResultIterator(Iterator<Entry> entryIterator, long afterSequence) {
            this.entryIterator = entryIterator;
            this.afterSequence = afterSequence;
        }

        @Override
        public CompletableFuture<ReadItem> getNext(Duration timeout) {
            ReadItem result = null;
            while (this.entryIterator.hasNext() && result == null) {
                Entry e = this.entryIterator.next();
                if (e.offset <= afterSequence) {
                    continue;
                }

                result = new ReadResultItem(e);
            }

            return CompletableFuture.completedFuture(result);
        }

        @Override
        public void close() {

        }
    }

    //endregion

    //region ReadResultItem

    private static class ReadResultItem implements DurableDataLog.ReadItem {

        private final byte[] payload;
        private final long sequence;

        public ReadResultItem(Entry entry) {
            this.payload = new byte[entry.data.length];
            System.arraycopy(entry.data, 0, this.payload, 0, this.payload.length);
            this.sequence = entry.offset;
        }

        @Override
        public byte[] getPayload() {
            return payload;
        }

        @Override
        public long getSequence() {
            return sequence;
        }

        @Override
        public String toString() {
            return String.format("Sequence = %d, Length = %d", sequence, payload.length);
        }
    }

    //endregion

    static class EntryCollection {
        private final LinkedList<Entry> entries;

        public EntryCollection() {
            this.entries = new LinkedList<>();
        }

        public void add(Entry entry) {
            this.entries.add(entry);
        }

        public int size() {
            return this.entries.size();
        }

        public Entry getFirst() {
            return this.entries.getFirst();
        }

        public Entry getLast() {
            return this.entries.getLast();
        }

        public Entry removeFirst() {
            return this.entries.removeFirst();
        }

        public Iterator<Entry> iterator() {
            return this.entries.iterator();
        }
    }

    //region Entry

    static class Entry {
        public long offset;
        public final byte[] data;

        public Entry(InputStream inputData) throws IOException {
            this.data = new byte[inputData.available()];
            StreamHelpers.readAll(inputData, this.data, 0, this.data.length);
        }

        @Override
        public String toString() {
            return String.format("Offset = %d, Length = %d", offset, data.length);
        }
    }

    //endregion
}
