/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */

package io.pravega.service.storage.impl.bookkeeper;

import com.google.common.base.Preconditions;
import io.pravega.common.Exceptions;
import io.pravega.common.util.CloseableIterator;
import io.pravega.service.storage.DurableDataLog;
import io.pravega.service.storage.DurableDataLogException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Enumeration;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.annotation.concurrent.NotThreadSafe;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerEntry;
import org.apache.bookkeeper.client.LedgerHandle;

/**
 * Performs read from BookKeeper Logs.
 */
@Slf4j
@NotThreadSafe
class LogReader implements CloseableIterator<DurableDataLog.ReadItem, DurableDataLogException> {
    //region Members

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
     * @param metadata   The LogMetadata of the Log to read.
     * @param bookKeeper A reference to the BookKeeper client to use.
     * @param config     Configuration to use.
     */
    LogReader(LogMetadata metadata, BookKeeper bookKeeper, BookKeeperConfig config) {
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
                try {
                    Ledgers.close(this.currentLedger.handle);
                } catch (DurableDataLogException bkEx) {
                    log.error("Unable to close LedgerHandle for Ledger {}.", this.currentLedger.handle.getId(), bkEx);
                }

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
            Ledgers.close(this.currentLedger.handle);
            openNextLedger(this.metadata.getNextAddress(lastAddress, this.currentLedger.handle.getLastAddConfirmed()));
        }

        // Try to read from the current reader.
        if (this.currentLedger == null || this.currentLedger.reader == null) {
            return null;
        }

        return new LogReader.ReadItem(this.currentLedger.reader.nextElement(), this.currentLedger.metadata);
    }

    @SneakyThrows
    private void openNextLedger(LedgerAddress address) throws DurableDataLogException {
        if (address == null) {
            // We have reached the end.
            close();
            return;
        }

        LedgerMetadata metadata = this.metadata.getLedger(address.getLedgerId());
        assert metadata != null : "no LedgerMetadata could be found with valid LedgerAddress " + address;

        // Open the ledger.
        val ledger = Ledgers.openRead(metadata.getLedgerId(), this.bookKeeper, this.config);

        long lastEntryId = ledger.getLastAddConfirmed();
        if (lastEntryId < address.getEntryId()) {
            // This ledger is empty.
            Ledgers.close(ledger);
            this.currentLedger = new ReadLedger(metadata, ledger, null);
            return;
        }

        try {
            val reader = Exceptions.handleInterrupted(() -> ledger.readEntries(address.getEntryId(), lastEntryId));
            this.currentLedger = new ReadLedger(metadata, ledger, reader);
        } catch (Exception ex) {
            Ledgers.close(ledger);
            close();
            throw new DurableDataLogException("Error while reading from BookKeeper.", ex);
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

        @SneakyThrows(IOException.class)
        ReadItem(LedgerEntry entry, LedgerMetadata ledgerMetadata) {
            this.address = new LedgerAddress(ledgerMetadata, entry.getEntryId());
            this.payload = entry.getEntryInputStream();
            this.length = this.payload.available();
        }

        @Override
        public String toString() {
            return String.format("%s, Length = %d.", this.address, this.length);
        }
    }

    //endregion

    //region ReadLedger

    @RequiredArgsConstructor
    private static class ReadLedger {
        final LedgerMetadata metadata;
        final LedgerHandle handle;
        final Enumeration<LedgerEntry> reader;

        boolean canRead() {
            return this.reader != null && this.reader.hasMoreElements();
        }
    }

    //endregion
}

