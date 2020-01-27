/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.storage.impl.bookkeeper;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

/**
 * Represents metadata about a particular ledger.
 */
@RequiredArgsConstructor
@Getter
public class LedgerMetadata {
    /**
     * The BookKeeper-assigned Ledger Id.
     */
    private final long ledgerId;

    /**
     * The metadata-assigned internal sequence number of the Ledger inside the log.
     */
    private final int sequence;

    /**
     * Gets the current status of this Ledger.
     */
    private final Status status;

    /**
     * Creates a new instance of the LedgerMetadata class with an unknown Empty Status.
     *
     * @param ledgerId The BookKeeper-assigned Ledger Id.
     * @param sequence The metadata-assigned sequence number.
     */
    LedgerMetadata(long ledgerId, int sequence) {
        this(ledgerId, sequence, Status.Unknown);
    }

    @Override
    public String toString() {
        return String.format("Id = %d, Sequence = %d, Status = %s", this.ledgerId, this.sequence, this.status);
    }

    @RequiredArgsConstructor(access = AccessLevel.PRIVATE)
    enum Status {
        Unknown((byte) 0),
        Empty((byte) 1),
        NotEmpty((byte) 2);
        @Getter
        private final byte value;

        static Status valueOf(byte b) {
            if (b == Unknown.value) {
                return Unknown;
            } else if (b == Empty.value) {
                return Empty;
            } else if (b == NotEmpty.value) {
                return NotEmpty;
            }

            throw new IllegalArgumentException("Unsupported Status " + b);
        }
    }
}
