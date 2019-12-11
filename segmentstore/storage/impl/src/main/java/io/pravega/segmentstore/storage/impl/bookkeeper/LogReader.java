/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.storage.impl.bookkeeper;

import com.google.common.base.Preconditions;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.pravega.common.Exceptions;
import io.pravega.common.util.CloseableIterator;
import io.pravega.segmentstore.storage.DurableDataLog;
import io.pravega.segmentstore.storage.DurableDataLogException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.annotation.concurrent.NotThreadSafe;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.bookkeeper.client.api.BookKeeper;
import org.apache.bookkeeper.client.api.LedgerEntry;
import org.apache.bookkeeper.client.api.LedgerEntries;
import org.apache.bookkeeper.client.api.ReadHandle;

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
                    this.currentLedger.close();
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
            this.currentLedger.close();
            openNextLedger(this.metadata.getNextAddress(lastAddress, this.currentLedger.handle.getLastAddConfirmed()));
        }

        // Try to read from the current reader.
        if (this.currentLedger == null || this.currentLedger.isEmpty()) {
            return null;
        }

        return this.currentLedger.nextItem();
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

        long lastEntryId = ledger.getLastAddConfirmed();
        if (lastEntryId < address.getEntryId()) {
            // This ledger is empty.
            Ledgers.close(ledger);
            this.currentLedger = new ReadLedger(metadata, ledger, null);
            return;
        }

        ReadLedger previousLedger;
        try {
            LedgerEntries entries = Exceptions.handleInterruptedCall(
                    () -> ledger.read(address.getEntryId(), lastEntryId));
            previousLedger = this.currentLedger;
            this.currentLedger = new ReadLedger(metadata, ledger, entries);
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

    //endregion

    //region ReadLedger

    private static class ReadLedger {
        final LedgerMetadata metadata;
        final ReadHandle handle;
        final LedgerEntries ledgerEntries;
        final Iterator<LedgerEntry> entryIterator;
        final AtomicBoolean closed = new AtomicBoolean(false);

        public ReadLedger(LedgerMetadata metadata, ReadHandle handle, LedgerEntries ledgerEntries) {
            this.metadata = metadata;
            this.handle = handle;
            this.ledgerEntries = ledgerEntries;
            if (ledgerEntries != null) {
                entryIterator = ledgerEntries.iterator();
            } else {
                entryIterator = null;
            }
        }

        boolean isEmpty() {
            return this.entryIterator == null;
        }

        boolean canRead() {
            return this.entryIterator != null && this.entryIterator.hasNext();
        }

        private DurableDataLog.ReadItem nextItem() {
            LedgerEntry entry = entryIterator.next();
            ByteBuf content = entry.getEntryBuffer();
            return new LogReader.ReadItem(entry.getEntryId(),
                    new ByteBufInputStream(content, false /*relaseOnClose*/),
                    content.readableBytes(), metadata);
        }

        private void close() throws DurableDataLogException {
            // Release memory held by BookKeeper internals.
            // we have to prevent a double free
            if (closed.compareAndSet(false, true)) {
                if (ledgerEntries != null) {
                    ledgerEntries.close();
                }
                // closing a Readonly is mostly a no-op, it is not expected
                // to really fail
                Ledgers.close(handle);
            }
        }

        /**
         * Release memory held by BookKeeper internals.
         */
        private void release() {

        }
    }

    //endregion
}

