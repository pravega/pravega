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
    @Getter
    private final long ledgerId;
    @Getter
    private final long entryId;

    /**
     * Creates a new instance of the LedgerAddress class.
     *
     * @param sequence The sequence of the address (location).
     * @param ledgerId The Id of the Ledger that this Address corresponds to.
     * @param entryId  The Entry Id inside the Ledger that this Address corresponds to.
     */
    LedgerAddress(long sequence, long ledgerId, long entryId) {
        super(sequence);
        Preconditions.checkArgument(ledgerId >= 0, "ledgerId must be a non-negative number.");
        this.ledgerId = ledgerId;
        this.entryId = entryId;
    }

    @Override
    public String toString() {
        return String.format("%s, LedgerId = %d, EntryId = %d", super.toString(), this.ledgerId, this.entryId);
    }
}
