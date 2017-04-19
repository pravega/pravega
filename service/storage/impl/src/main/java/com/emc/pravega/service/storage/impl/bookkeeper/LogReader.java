/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */

package com.emc.pravega.service.storage.impl.bookkeeper;

import com.emc.pravega.common.Exceptions;
import com.emc.pravega.common.util.CloseableIterator;
import com.emc.pravega.service.storage.DurableDataLog;
import com.emc.pravega.service.storage.DurableDataLogException;
import com.google.common.collect.Iterators;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.annotation.concurrent.ThreadSafe;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerEntry;
import org.apache.bookkeeper.client.LedgerHandle;

/**
 * Performs reads from the logs.
 */
@Slf4j
@ThreadSafe
class LogReader implements CloseableIterator<DurableDataLog.ReadItem, DurableDataLogException> {
    //region Members

    private final BookKeeper bookKeeper;
    private final LogMetadata metadata;
    private final AtomicBoolean closed;
    private final BookKeeperConfig config;
    private LedgerMetadata currentLedgerMetadata;
    private LedgerHandle currentLedger;
    private Iterator<LedgerEntry> currentLedgerReader;

    //endregion

    //region Constructor

    LogReader(LogMetadata metadata, BookKeeper bookKeeper, BookKeeperConfig config) {
        this.metadata = metadata;
        this.bookKeeper = bookKeeper;
        this.config = config;
        this.closed = new AtomicBoolean();
    }

    //endregion

    //region AutoCloseable Implementation

    @Override
    public void close() {
        if (!this.closed.getAndSet(true)) {
            this.currentLedgerReader = null;
            if (this.currentLedger != null) {
                try {
                    Ledgers.close(this.currentLedger);
                } catch (DurableDataLogException bkEx) {
                    log.error("Unable to close LedgerHandle for Ledger {}.", this.currentLedger.getId(), bkEx);
                }

                this.currentLedger = null;
            }

            this.currentLedgerMetadata = null;
        }
    }

    //endregion

    //region CloseableIterator Implementation

    @Override
    public DurableDataLog.ReadItem getNext() throws DurableDataLogException {
        Exceptions.checkNotClosed(this.closed.get(), this);

        if (this.currentLedgerReader == null) {
            // First time we call this. Locate the first ledger based on the metadata truncation address.
            openNextLedger(this.metadata.nextAddress(this.metadata.getTruncationAddress(), 0));
        }

        while (this.currentLedgerReader != null && !this.currentLedgerReader.hasNext()) {
            // We have reached the end of the current ledger. Find next one (the loop accounts for empty ledgers).
            val lastAddress = new LedgerAddress(this.currentLedgerMetadata.getSequence(), this.currentLedger.getId(), this.currentLedger.getLastAddConfirmed());
            Ledgers.close(this.currentLedger);
            openNextLedger(this.metadata.nextAddress(lastAddress, this.currentLedger.getLastAddConfirmed()));
        }

        // Try to read from the current reader.
        if (this.currentLedgerReader == null) {
            return null;
        }

        val nextEntry = this.currentLedgerReader.next();

        byte[] payload = nextEntry.getEntry();// TODO: this also exposes an InputStream, which may be more efficient.
        val address = new LedgerAddress(this.currentLedgerMetadata.getSequence(), this.currentLedger.getId(), nextEntry.getEntryId());
        return new LogReader.ReadItem(payload, address);
    }

    private void openNextLedger(LedgerAddress address) throws DurableDataLogException {
        if (address == null) {
            // We have reached the end.
            close();
            return;
        }
        LedgerMetadata metadata = this.metadata.getLedgerMetadata(address.getLedgerId());
        assert metadata != null : "no LedgerMetadata could be found with valid LedgerAddress " + address;

        // Open the ledger.
        this.currentLedgerMetadata = metadata;
        this.currentLedger = Ledgers.open(metadata.getLedgerId(), this.bookKeeper, this.config);

        long lastEntryId = this.currentLedger.getLastAddConfirmed();
        if (lastEntryId < address.getEntryId()) {
            // This ledger is empty.
            Ledgers.close(this.currentLedger);
            return;
        }

        try {
            this.currentLedgerReader = Exceptions.handleInterrupted(() -> Iterators.forEnumeration(this.currentLedger.readEntries(address.getEntryId(), lastEntryId)));
        } catch (Exception ex) {
            close();
            throw new DurableDataLogException("Error while reading from BookKeeper.", ex);
        }
    }

    //endregion

    //region ReadItem

    @RequiredArgsConstructor
    private static class ReadItem implements DurableDataLog.ReadItem {
        @Getter
        private final byte[] payload;
        @Getter
        private final LedgerAddress address;

        @Override
        public String toString() {
            return String.format("%s, Length = %d.", getAddress(), getPayload().length);
        }
    }

    //endregion
}

