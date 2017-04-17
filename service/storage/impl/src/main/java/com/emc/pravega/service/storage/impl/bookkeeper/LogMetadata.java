/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */

package com.emc.pravega.service.storage.impl.bookkeeper;

import com.emc.pravega.common.Exceptions;
import com.google.common.base.Preconditions;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import lombok.Getter;

/**
 * Metadata for a Ledger-based log.
 */
class LogMetadata {
    //region Members

    private static final byte SERIALIZATION_VERSION = 0;
    private static final long INITIAL_EPOCH = 1;
    @Getter
    private final long epoch;
    @Getter
    private final List<Long> ledgerIds;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the LogMetadata class.
     *
     * @param epoch     The current Log epoch.
     * @param ledgerIds The ordered list of Ledger Ids making up this log.
     */
    private LogMetadata(long epoch, List<Long> ledgerIds) {
        Preconditions.checkArgument(epoch > 0, "epoch must be a positive number");
        Exceptions.checkNotNullOrEmpty(ledgerIds, "ledgerIds");
        this.epoch = epoch;
        this.ledgerIds = ledgerIds;
    }

    /**
     * Creates a new instance of the LogMetadata class with one Ledger and epoch set to the default value.
     *
     * @param initialLedgerId The Id of the Ledger to start the log with.
     */
    LogMetadata(long initialLedgerId) {
        this(INITIAL_EPOCH, Collections.singletonList(initialLedgerId));
    }

    /**
     * Creates a new instance of the LogMetadata class which contains an additional ledger id.
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

        List<Long> newLedgers = new ArrayList<>(this.ledgerIds.size() + 1);
        newLedgers.addAll(this.ledgerIds);
        newLedgers.add(ledgerId);
        return new LogMetadata(newEpoch, Collections.unmodifiableList(newLedgers));
    }

    //endregion

    //region Serialization

    /**
     * Serializes this LogMetadata object into a byte array.
     *
     * @return A new byte array with the serialized contents of this object.
     */
    public byte[] serialize() {
        // Serialization version (Byte), Epoch (Long), Ledger Length (Int), Ledgers (Long each).
        ByteBuffer bb = ByteBuffer.allocate(Byte.BYTES + Long.BYTES + Integer.BYTES + Long.BYTES * this.ledgerIds.size());
        bb.put(SERIALIZATION_VERSION);
        bb.putLong(this.epoch);
        bb.putInt(this.ledgerIds.size());
        this.ledgerIds.forEach(bb::putLong);
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
        int ledgerCount = bb.getInt();
        List<Long> ledgerIds = new ArrayList<>(ledgerCount);
        for (int i = 0; i < ledgerCount; i++) {
            ledgerIds.add(bb.getLong());
        }
        return new LogMetadata(epoch, Collections.unmodifiableList(ledgerIds));
    }

    //endregion

    @Override
    public String toString() {
        return String.format("Epoch = %s, LedgerCount = %s", this.epoch, this.ledgerIds.size());
    }
}
