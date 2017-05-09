/*
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.service.storage.impl.bookkeeper;

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
