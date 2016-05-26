package com.emc.logservice.storageabstraction.mocks;

import com.emc.logservice.common.StreamHelpers;
import com.emc.logservice.storageabstraction.DurableDataLog;

import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.concurrent.*;

/**
 * In-Memory Mock for DurableDataLog. Contents is destroyed when object is garbage collected.
 */
public class InMemoryDurableDataLog implements DurableDataLog {
    private final LinkedList<Entry> entries;
    private long offset;
    private long lastAppendSequence;
    private final long delayMillisPerMB;

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
    public int getMaxAppendLength() {
        return 1024 * 1024 - 8 * 1024;
    }

    @Override
    public long getLastAppendSequence() {
        return this.lastAppendSequence;
    }

    @Override
    public CompletableFuture<Long> append(InputStream data, Duration timeout) {
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
    public CompletableFuture<Iterator<DurableDataLog.ReadItem>> read(long afterOffset, int maxCount, Duration timeout) {
        CompletableFuture<Iterator<DurableDataLog.ReadItem>> resultFuture = new CompletableFuture<>();

        // Run in a new thread to "simulate" asynchronous behavior.
        Thread t = new Thread(() -> {
            // TODO: should we block if we have no data?
            LinkedList<DurableDataLog.ReadItem> result = new LinkedList<>();
            synchronized (this.entries) {
                for (Entry e : this.entries) {
                    if (e.offset > afterOffset) {
                        result.add(new ReadResultItem(e));

                        if (result.size() >= maxCount) {
                            break;
                        }
                    }
                }
            }

            resultFuture.complete(result.iterator());
        });

        t.start();
        return resultFuture;
    }

    @Override
    public CompletableFuture<Void> recover(Duration timeout) {
        return CompletableFuture.completedFuture(null);
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
