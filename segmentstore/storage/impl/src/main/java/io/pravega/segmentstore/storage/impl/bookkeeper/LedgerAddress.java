/**
 * Copyright Pravega Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.pravega.segmentstore.storage.impl.bookkeeper;

import com.google.common.base.Preconditions;
import io.pravega.segmentstore.storage.LogAddress;
import lombok.Getter;

/**
 * LogAddress for BookKeeper. Wraps around BookKeeper-specific addressing scheme without exposing such information to the outside.
 */
class LedgerAddress extends LogAddress implements Comparable<LedgerAddress> {
    //region Members

    private static final long INT_MASK = 0xFFFFFFFFL;
    /**
     * The BookKeeper-assigned identifier for the Ledger of this address.
     */
    @Getter
    private final long ledgerId;

    //endregion

    //region Constructor

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
     * @param metadata The LedgerMetadata for the ledger.
     * @param entryId  The Entry Id inside the Ledger that this Address corresponds to.
     */
    LedgerAddress(LedgerMetadata metadata, long entryId) {
        this(calculateAppendSequence(metadata.getSequence(), entryId), metadata.getLedgerId());
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

    //endregion

    //region Properties

    /**
     * Gets a Sequence number identifying the Ledger inside the log. This is different from getSequence (which identifies
     * a particular write inside the entire log. It is also different from LedgerId, which is a BookKeeper assigned id.
     *
     * @return The result.
     */
    int getLedgerSequence() {
        return (int) (getSequence() >>> 32);
    }

    /**
     * Gets a value representing the BookKeeper-assigned Entry id of this address. This entry id is unique per ledger, but
     * is likely duplicated across ledgers (since it grows sequentially from 0 in each ledger).
     *
     * @return The result.
     */
    long getEntryId() {
        return getSequence() & INT_MASK;
    }

    @Override
    public String toString() {
        return String.format("%s, LedgerId = %d, EntryId = %d", super.toString(), this.ledgerId, getEntryId());
    }

    /**
     * Calculates the globally-unique append sequence by combining the ledger sequence and the entry id.
     *
     * @param ledgerSequence The Ledger Sequence (in the log). This will make up the high-order 32 bits of the result.
     * @param entryId        The Entry Id inside the ledger. This will be interpreted as a 32-bit integer and will make
     *                       up the low-order 32 bits of the result.
     * @return The calculated value.
     */
    private static long calculateAppendSequence(int ledgerSequence, long entryId) {
        return ((long) ledgerSequence << 32) + (entryId & INT_MASK);
    }

    //endregion

    //region Comparable Implementation

    @Override
    public int hashCode() {
        return Long.hashCode(getSequence());
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof LedgerAddress) {
            return this.compareTo((LedgerAddress) obj) == 0;
        }

        return false;
    }

    @Override
    public int compareTo(LedgerAddress address) {
        return Long.compare(getSequence(), address.getSequence());
    }

    //endregion
}
