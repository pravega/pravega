/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */

package com.emc.pravega.service.storage.impl.bookkeeper;

import com.emc.pravega.common.Exceptions;
import com.emc.pravega.common.util.CollectionHelpers;
import com.google.common.base.Preconditions;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.Getter;
import lombok.val;

/**
 * Metadata for a Ledger-based log.
 */
class LogMetadata {
    //region Members

    private static final byte SERIALIZATION_VERSION = 0;
    private static final long INITIAL_EPOCH = 1;
    private static final int INITIAL_VERSION = -1;
    private static final int INITIAL_LEDGER_SEQUENCE = 1;

    /**
     * A LogAddress to be used when the log is not truncated (initially). Setting all values to 0 is OK as BookKeeper never
     * has a LedgerId that is 0, so this will never overlap with the first entry in the log.
     */
    private static final LedgerAddress INITIAL_TRUNCATION_ADDRESS = new LedgerAddress(INITIAL_LEDGER_SEQUENCE - 1, 0, 0);
    @Getter
    private final long epoch;
    @Getter
    private final List<LedgerMetadata> ledgers;
    @Getter
    private final LedgerAddress truncationAddress;
    private final AtomicInteger updateVersion;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the LogMetadata class with one Ledger and epoch set to the default value.
     *
     * @param initialLedgerId The Id of the Ledger to start the log with.
     */
    LogMetadata(long initialLedgerId) {
        this(INITIAL_EPOCH, Collections.singletonList(new LedgerMetadata(initialLedgerId, INITIAL_LEDGER_SEQUENCE)), INITIAL_TRUNCATION_ADDRESS);
    }

    /**
     * Creates a new instance of the LogMetadata class.
     *
     * @param epoch             The current Log epoch.
     * @param ledgers           The ordered list of Ledger Ids making up this log.
     * @param truncationAddress The truncation address for this log. This is the address of the last entry that has been
     *                          truncated out of the log.
     */
    private LogMetadata(long epoch, List<LedgerMetadata> ledgers, LedgerAddress truncationAddress) {
        Preconditions.checkArgument(epoch > 0, "epoch must be a positive number");
        Exceptions.checkNotNullOrEmpty(ledgers, "ledgers");
        Preconditions.checkNotNull(truncationAddress, "truncationAddress");
        this.epoch = epoch;
        this.ledgers = ledgers;
        this.truncationAddress = truncationAddress;
        this.updateVersion = new AtomicInteger(INITIAL_VERSION);
    }

    /**
     * Creates a new instance of the LogMetadata class which contains an additional ledger.
     *
     * @param ledgerId       The Id of the Ledger to add.
     * @param incrementEpoch If true, the new LogMetadata object will have its epoch incremented (compared to this object's).
     * @return A new instance of the LogMetadata class.
     */
    LogMetadata addLedger(long ledgerId, boolean incrementEpoch) {
        long newEpoch = this.epoch;
        if (incrementEpoch) {
            newEpoch++;
        }

        // Copy existing ledgers.
        List<LedgerMetadata> newLedgers = new ArrayList<>(this.ledgers.size() + 1);
        newLedgers.addAll(this.ledgers);

        // Create and add metadata for the new ledger.
        int sequence = this.ledgers.size() == 0 ? INITIAL_LEDGER_SEQUENCE : this.ledgers.get(this.ledgers.size() - 1).getSequence() + 1;
        newLedgers.add(new LedgerMetadata(ledgerId, sequence));
        LogMetadata newMetadata = new LogMetadata(newEpoch, Collections.unmodifiableList(newLedgers), this.truncationAddress);
        newMetadata.setUpdateVersion(this.updateVersion.get());
        return newMetadata;
    }

    //endregion

    int getUpdateVersion() {
        return this.updateVersion.get();
    }

    void setUpdateVersion(int value) {
        this.updateVersion.set(value);
    }

    LedgerMetadata getLedgerMetadata(long ledgerId) {
        int index = CollectionHelpers.binarySearch(this.ledgers, lm -> Long.compare(ledgerId, lm.getLedgerId()));
        if (index >= 0) {
            return this.ledgers.get(index);
        }

        return null;
    }

    LedgerAddress nextAddress(LedgerAddress address, long lastEntryId) {
        if (address.getEntryId() < lastEntryId) {
            // Same ledger, next entry.
            return new LedgerAddress(address.getLedgerSequence(), address.getLedgerId(), address.getEntryId() + 1);
        } else {
            LedgerMetadata ledgerMetadata = null;
            // Next ledger. First try a binary search, hoping the address actually exists.
            int index = CollectionHelpers.binarySearch(this.ledgers, lm -> Long.compare(address.getLedgerId(), lm.getLedgerId())) + 1;
            if (index > 0) {
                if (index < this.ledgers.size()) {
                    ledgerMetadata = this.ledgers.get(index);
                }
            } else {
                // Ledger was not in the list. We need to find the first ledger with an id larger than the one we have.
                for (LedgerMetadata lm : this.ledgers) {
                    if (lm.getLedgerId() > address.getLedgerId()) {
                        ledgerMetadata = lm;
                        break;
                    }
                }
            }

            if (ledgerMetadata != null) {
                return new LedgerAddress(ledgerMetadata.getSequence(), ledgerMetadata.getLedgerId(), 0);
            }
        }

        // Nothing was found.
        return null;
    }

    //region Serialization

    /**
     * Serializes this LogMetadata object into a byte array.
     *
     * @return A new byte array with the serialized contents of this object.
     */
    byte[] serialize() {
        // Serialization version (Byte), Epoch (Long), TruncationAddress (3*Long), Ledger Length (Int), Ledgers.
        val length = Byte.BYTES + Long.BYTES + Long.BYTES * 3 + Integer.BYTES + (Long.BYTES + Integer.BYTES) * this.ledgers.size();
        ByteBuffer bb = ByteBuffer.allocate(length);
        bb.put(SERIALIZATION_VERSION);
        bb.putLong(this.epoch);

        // Truncation Address.
        bb.putLong(this.truncationAddress.getSequence());
        bb.putLong(this.truncationAddress.getLedgerId());

        // Ledgers.
        bb.putInt(this.ledgers.size());
        this.ledgers.forEach(lm -> {
            bb.putLong(lm.getLedgerId());
            bb.putInt(lm.getSequence());
        });
        return bb.array();
    }

    /**
     * Attempts to deserialize the given byte array into a LogMetadata object.
     *
     * @param serialization The byte array to deserialize.
     * @return A new instance of the LogMetadata class with the contents of the given byte array.
     */
    static LogMetadata deserialize(byte[] serialization) {
        ByteBuffer bb = ByteBuffer.wrap(serialization);
        bb.get(); // We skip version for now because we only have one.
        long epoch = bb.getLong();

        // Truncation Address.
        long truncationSeqNo = bb.getLong();
        long truncationLedgerId = bb.getLong();

        // Ledgers
        int ledgerCount = bb.getInt();
        List<LedgerMetadata> ledgers = new ArrayList<>(ledgerCount);
        for (int i = 0; i < ledgerCount; i++) {
            long ledgerId = bb.getLong();
            int seq = bb.getInt();
            ledgers.add(new LedgerMetadata(ledgerId, seq));
        }

        return new LogMetadata(epoch, Collections.unmodifiableList(ledgers), new LedgerAddress(truncationSeqNo, truncationLedgerId));
    }

    //endregion

    @Override
    public String toString() {
        return String.format("Version = %d, Epoch = %d, LedgerCount = %d", this.updateVersion.get(), this.epoch, this.ledgers.size());
    }
}
