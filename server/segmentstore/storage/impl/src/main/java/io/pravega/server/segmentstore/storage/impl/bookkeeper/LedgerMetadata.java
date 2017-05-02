/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 *
 */

package io.pravega.server.segmentstore.storage.impl.bookkeeper;

import lombok.Data;

/**
 * Represents metadata about a particular ledger.
 */
@Data
class LedgerMetadata {
    /**
     * The BookKeeper-assigned Ledger Id.
     */
    private final long ledgerId;

    /**
     * The metadata-assigned internal sequence number of the Ledger inside the log.
     */
    private final int sequence;

    @Override
    public String toString() {
        return String.format("Id = %d, Sequence = %d", this.ledgerId, this.sequence);
    }
}
