/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */

package com.emc.pravega.service.storage.impl.bookkeeper;

import lombok.Data;

/**
 * Represents metadata about a particular ledger.
 */
@Data
class LedgerMetadata {
    private final long ledgerId;
    private final int sequence;

    @Override
    public String toString() {
        return String.format("Id = %d, Sequence = %d", this.ledgerId, this.sequence);
    }
}
