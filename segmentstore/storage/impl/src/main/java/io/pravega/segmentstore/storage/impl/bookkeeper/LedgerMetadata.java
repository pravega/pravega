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

import lombok.Getter;
import lombok.RequiredArgsConstructor;

/**
 * Represents metadata about a particular ledger.
 */
@RequiredArgsConstructor
@Getter
class LedgerMetadata {
    /**
     * Defines a special value for LastAddConfirmed that indicates this has not yet been set.
     */
    static final long NO_LAST_ADD_CONFIRMED = -1;

    /**
     * The BookKeeper-assigned Ledger Id.
     */
    private final long ledgerId;

    /**
     * The metadata-assigned internal sequence number of the Ledger inside the log.
     */
    private final int sequence;

    /**
     * The Entry Id of the Last Confirmed Add inside this Ledger. Only applies to closed ledgers.
     */
    private final long lastAddConfirmed;

    /**
     * Creates a new instance of the LedgerMetadata class with no defined LastAddConfirmed.
     *
     * @param ledgerId The BookKeeper-assigned Ledger Id.
     * @param sequence The metadata-assigned sequence number.
     */
    LedgerMetadata(long ledgerId, int sequence) {
        this(ledgerId, sequence, NO_LAST_ADD_CONFIRMED);
    }

    @Override
    public String toString() {
        return String.format("Id = %d, Sequence = %d, LAC = %d", this.ledgerId, this.sequence, this.lastAddConfirmed);
    }
}
