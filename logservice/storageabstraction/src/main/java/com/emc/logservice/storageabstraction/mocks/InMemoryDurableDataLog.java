package com.emc.logservice.storageabstraction.mocks;

import com.emc.logservice.common.*;
import com.emc.logservice.storageabstraction.DurableDataLog;
import com.emc.logservice.storageabstraction.DurableDataLogException;

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
public class InMemoryDurableDataLog implements DurableDataLog {
    private final LinkedList<Entry> entries;
    private long offset;
    private long lastAppendSequence;
    private final long delayMillisPerMB;
    private boolean closed;
    private boolean initialized;

    public InMemoryDurableDataLog() {
        this(0);
    }

    public InMemoryDurableDataLog(long delayMillisPerMB) {
        this.entries = new LinkedList<>();
        this.offset = 0;
        this.lastAppendSequence = -1;
        this.delayMillisPerMB = delayMillisPerMB;
    }

    @Override
    public void close() throws DurableDataLogException {
        this.closed = true;
    }

    @Override
    public CompletableFuture<Void> initialize(Duration timeout) {
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

            long delayMillis = this.delayMillisPerMB * entry.data.length / 1024 / 1024;
            if (delayMillis > 0) {
                try {
                    Thread.sleep(delayMillis);
                }
                catch (InterruptedException ex) {
                }
            }

            return entry.offset;
        });
    }

    @Override
    public CompletableFuture<Void> truncate(long offset, Duration timeout) {
        ensurePreconditions();
        CompletableFuture<Void> resultFuture = new CompletableFuture<>();

        // Run in a new thread to "simulate" asynchronous behavior.
        Thread t = new Thread(() ->
        {
            synchronized (this.entries) {
                while (this.entries.size() > 0 && this.entries.getFirst().offset + this.entries.getFirst().data.length <= offset) {
                    this.entries.removeFirst();
                }
            }

            resultFuture.complete(null);
        });

        t.start();
        return resultFuture;
    }

    @Override
    public AsyncIterator<ReadItem> getReader(long afterSequence) {
        ensurePreconditions();
        return new ReadResultIterator(this.entries.iterator(), afterSequence);
    }

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

    private void ensurePreconditions() {
        if (this.closed) {
            throw new ObjectClosedException(this);
        }

        if (!this.initialized) {
            throw new IllegalStateException("InMemoryDurableDataLog is not initialized.");
        }
    }

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

    private static class Entry {
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
}
