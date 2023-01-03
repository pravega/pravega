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
package io.pravega.segmentstore.storage.impl.bookkeeper;

import com.google.common.base.Preconditions;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.pravega.common.Exceptions;
import io.pravega.common.util.BufferedIterator;
import io.pravega.common.util.CloseableIterator;
import io.pravega.segmentstore.storage.DataLogCorruptedException;
import io.pravega.segmentstore.storage.DurableDataLog;
import io.pravega.segmentstore.storage.DurableDataLogException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.annotation.concurrent.NotThreadSafe;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.bookkeeper.client.api.BKException;
import org.apache.bookkeeper.client.api.BookKeeper;
import org.apache.bookkeeper.client.api.LedgerEntry;
import org.apache.bookkeeper.client.api.LedgerEntries;
import org.apache.bookkeeper.client.api.ReadHandle;
import lombok.NonNull;
import lombok.SneakyThrows;
import org.apache.bookkeeper.client.api.Handle;

/**
 * Performs read from BookKeeper Logs.
 */
@Slf4j
@NotThreadSafe
class LogReader implements CloseableIterator<DurableDataLog.ReadItem, DurableDataLogException> {
    //region Members

    private final int logId;
    private final BookKeeper bookKeeper;
    private final LogMetadata metadata;
    private final AtomicBoolean closed;
    private final BookKeeperConfig config;
    private ReadLedger currentLedger;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the LogReader class.
     *
     * @param logId      The Id of the {@link BookKeeperLog} to read from. This is used for validation purposes.
     * @param metadata   The LogMetadata of the Log to read.
     * @param bookKeeper A reference to the BookKeeper client to use.
     * @param config     Configuration to use.
     */
    LogReader(int logId, LogMetadata metadata, BookKeeper bookKeeper, BookKeeperConfig config) {
        this.logId = logId;
        this.metadata = Preconditions.checkNotNull(metadata, "metadata");
        this.bookKeeper = Preconditions.checkNotNull(bookKeeper, "bookKeeper");
        this.config = Preconditions.checkNotNull(config, "config");
        this.closed = new AtomicBoolean();
    }

    //endregion

    //region AutoCloseable Implementation

    @Override
    public void close() {
        if (!this.closed.getAndSet(true)) {
            if (this.currentLedger != null) {
                this.currentLedger.close();
                this.currentLedger = null;
            }
        }
    }

    //endregion

    //region CloseableIterator Implementation

    @Override
    public DurableDataLog.ReadItem getNext() throws DurableDataLogException {
        Exceptions.checkNotClosed(this.closed.get(), this);

        if (this.currentLedger == null) {
            // First time we call this. Locate the first ledger based on the metadata truncation address. We don't know
            // how many entries are in that first ledger, so open it anyway so we can figure out.
            openNextLedger(this.metadata.getNextAddress(this.metadata.getTruncationAddress(), Long.MAX_VALUE));
        }

        while (this.currentLedger != null && (!this.currentLedger.canRead())) {
            // We have reached the end of the current ledger. Find next one, and skip over empty ledgers).
            val lastAddress = new LedgerAddress(this.currentLedger.metadata, this.currentLedger.handle.getLastAddConfirmed());
            this.currentLedger.close();
            openNextLedger(this.metadata.getNextAddress(lastAddress, this.currentLedger.handle.getLastAddConfirmed()));
        }

        // Try to read from the current reader.
        if (this.currentLedger == null || this.currentLedger.isEmpty()) {
            return null;
        }

        return wrapItem(this.currentLedger.reader.next(), this.currentLedger.metadata);
    }

    private void openNextLedger(LedgerAddress address) throws DurableDataLogException {
        if (address == null) {
            // We have reached the end.
            close();
            return;
        }

        LedgerMetadata metadata = this.metadata.getLedger(address.getLedgerId());
        assert metadata != null : "no LedgerMetadata could be found with valid LedgerAddress " + address;
        val allMetadatas = this.metadata.getLedgers();

        // Open the ledger.
        ReadHandle ledger;
        if (allMetadatas.size() == 0 || metadata == allMetadatas.get(allMetadatas.size() - 1)) {
            // This is our last ledger (the active one); we need to make sure open it without recovery since otherwise we
            // we would fence ourselves out.
            ledger = Ledgers.openRead(metadata.getLedgerId(), this.bookKeeper, this.config);
        } else {
            // Older ledger. Open with recovery to make sure any uncommitted fragments will be recovered. Since we do our
            // Log fencing based on the last Ledger, open-fencing this Ledger will not have any adverse effects.
            ledger = Ledgers.openFence(metadata.getLedgerId(), this.bookKeeper, this.config);
        }

        checkLogIdProperty(ledger);
        long lastEntryId = ledger.getLastAddConfirmed();
        if (lastEntryId < address.getEntryId()) {
            // This ledger is empty.
            Ledgers.close(ledger);
            this.currentLedger = ReadLedger.empty(metadata, ledger);
            return;
        }

        ReadLedger previousLedger;
        try {
            previousLedger = this.currentLedger;
            this.currentLedger = new ReadLedger(metadata, ledger, address.getEntryId(), lastEntryId, this.config.getBkReadBatchSize());
            if (previousLedger != null) {
                // Close previous ledger handle.
                previousLedger.close();
            }
        } catch (Exception ex) {
            Ledgers.close(ledger);
            close();
            throw new DurableDataLogException("Error while reading from BookKeeper.", ex);
        }
    }

    private void checkLogIdProperty(Handle handle) throws DataLogCorruptedException {
        int actualLogId = Ledgers.getBookKeeperLogId(handle);
        // For special log repair operations, we allow the LogReader to read from a special log id that contains admin-provided
        // changes to repair the original lod data (i.e., Ledgers.REPAIR_LOG_ID).
        if (actualLogId != Ledgers.NO_LOG_ID && actualLogId != this.logId && actualLogId != Ledgers.REPAIR_LOG_ID) {
            throw new DataLogCorruptedException(String.format("BookKeeperLog %s contains ledger %s which belongs to BookKeeperLog %s.",
                    this.logId, handle.getId(), actualLogId));
        }
    }

    //endregion

    //region ReadItem

    private static class ReadItem implements DurableDataLog.ReadItem {
        @Getter
        private final InputStream payload;
        @Getter
        private final int length;
        @Getter
        private final LedgerAddress address;

        ReadItem(long entryId, InputStream payload, int length, LedgerMetadata ledgerMetadata) {
            this.address = new LedgerAddress(ledgerMetadata, entryId);
            this.payload = payload;
            this.length = length;
        }

        @Override
        public String toString() {
            return String.format("%s, Length = %d.", this.address, this.length);
        }
    }

    private static DurableDataLog.ReadItem wrapItem(LedgerEntry entry, LedgerMetadata metadata) {
        ByteBuf content = entry.getEntryBuffer();
        return new LogReader.ReadItem(entry.getEntryId(),
               new ByteBufInputStream(content, false /*relaseOnClose*/),
               content.readableBytes(), metadata);
    }

    //endregion

    //region ReadLedger

    private static class ReadLedger {
        final LedgerMetadata metadata;
        final ReadHandle handle;
        final BufferedIterator<LedgerEntry> reader;
        final AtomicBoolean closed = new AtomicBoolean(false);
        volatile LedgerEntries currentLedgerEntries;

        public ReadLedger(LedgerMetadata metadata, ReadHandle handle, long firstEntryId, long lastEntryId,
                int batchSize) {
            this.metadata = metadata;
            this.handle = handle;
            if (lastEntryId >= firstEntryId) {
                this.reader = new BufferedIterator<>(this::readRange, firstEntryId, lastEntryId, batchSize);
            } else {
                // Empty ledger;
                this.reader = null;
            }
        }

        boolean isEmpty() {
            return this.reader == null;
        }

        private void close() {
            // Release memory held by BookKeeper internals.
            // we have to prevent a double free
            if (closed.compareAndSet(false, true)) {
                if (currentLedgerEntries != null) {
                    currentLedgerEntries.close();
                }
                // closing a ReadHandle is mostly a no-op, it is not expected
                // to really fail
                try {
                    Ledgers.close(handle);
                } catch (DurableDataLogException bkEx) {
                    log.error("Unable to close ReadHandle for Ledger {}.", handle.getId(), bkEx);
                }
            }
        }

        @SneakyThrows(BKException.class)
        private Iterator<LedgerEntry> readRange(long fromEntryId, long toEntryId) {
            if (currentLedgerEntries != null) {
                currentLedgerEntries.close();
            }
            currentLedgerEntries = Exceptions.handleInterruptedCall(() -> this.handle.read(fromEntryId, toEntryId));
            return currentLedgerEntries.iterator();
        }

        static ReadLedger empty(@NonNull LedgerMetadata metadata, @NonNull ReadHandle handle) {
            return new ReadLedger(metadata, handle, Long.MAX_VALUE, Long.MIN_VALUE, 1);
        }

        boolean canRead() {
            return this.reader != null && this.reader.hasNext();
        }
    }

    //endregion
}

