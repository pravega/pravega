/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */

package com.emc.pravega.service.storage.impl.bookkeeper;

import com.emc.pravega.service.storage.LogAddress;
import com.google.common.base.Preconditions;
import lombok.Getter;

/**
 * LogAddress for BookKeeper. Wraps around BookKeeper-specific addressing scheme without exposing such information to the outside.
 */
class LedgerAddress extends LogAddress {
    private static final long INT_MASK = 0xFFFFFFFFL;
    @Getter
    private final long ledgerId;

    /**
     * Creates a new instance of the LedgerAddress class.
     *
     * @param ledgerSequence The sequence of the Ledger (This is different from the Entry Sequence).
     * @param ledgerId       The Id of the Ledger that this Address corresponds to.
     * @param entryId        The Entry Id inside the Ledger that this Address corresponds to.
     */
    LedgerAddress(int ledgerSequence, long ledgerId, long entryId) {
        this(calculateAppendSequence(ledgerSequence, entryId), ledgerId);
    }

    /**
     * Creates a new instance of the LedgerAddress class.
     *
     * @param addressSequence The sequence of the Address (This is different from the Ledger Sequence).
     * @param ledgerId        The Id of the Ledger that this Address corresponds to.
     */
    LedgerAddress(long addressSequence, long ledgerId) {
        super(addressSequence);
        Preconditions.checkArgument(ledgerId >= 0, "ledgerId must be a non-negative number.");
        this.ledgerId = ledgerId;
    }


    public int getLedgerSequence() {
        return (int) (getSequence() >>> 32);
    }

    public long getEntryId() {
        return getSequence() & INT_MASK;
    }

    @Override
    public String toString() {
        return String.format("%s, LedgerId = %d, EntryId = %d", super.toString(), this.ledgerId, getEntryId());
    }

    private static long calculateAppendSequence(int ledgerSequence, long entryId) {
        return ((long) ledgerSequence << 32) + (entryId & INT_MASK);
    }
}
