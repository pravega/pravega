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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import lombok.Data;
import lombok.Getter;

/**
 * Metadata for a Ledger-based log.
 */
class LogMetadata {
    //region Members

    private static final byte SERIALIZATION_VERSION = 0;
    private static final long INITIAL_EPOCH = 1;
    private static final int INITIAL_VERSION = -1;

    /**
     * A LogAddress to be used when the log is not truncated (initially). Setting all values to 0 is OK as BookKeeper never
     * has a LedgerId that is 0, so this will never overlap with the first entry in the log.
     */
    private static final LedgerAddress INITIAL_TRUNCATION_ADDRESS = new LedgerAddress(0, 0, -1);
    @Getter
    private final long epoch;
    @Getter
    private final List<LedgerMetadata> ledgers;
    @Getter
    private final LedgerAddress truncationAddress;
    private final AtomicInteger updateVersion;
    private final AtomicLong lastAppendSequence;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the LogMetadata class with one Ledger and epoch set to the default value.
     *
     * @param initialLedgerId The Id of the Ledger to start the log with.
     */
    LogMetadata(long initialLedgerId) {
        this(INITIAL_EPOCH, Collections.singletonList(new LedgerMetadata(initialLedgerId, 0)), INITIAL_TRUNCATION_ADDRESS);
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
        this.lastAppendSequence = new AtomicLong();
        this.updateVersion = new AtomicInteger(INITIAL_VERSION);
    }

    /**
     * Creates a new instance of the LogMetadata class which contains an additional ledger.
     *
     * @param ledger         The LedgerMetadata of the Ledger to add.
     * @param incrementEpoch If true, the new LogMetadata object will have its epoch incremented (compared to this object's).
     * @return A new instance of the LogMetadata class.
     */
    LogMetadata addLedger(LedgerMetadata ledger, boolean incrementEpoch) {
        long newEpoch = this.epoch;
        if (incrementEpoch) {
            newEpoch++;
        }

        List<LedgerMetadata> newLedgers = new ArrayList<>(this.ledgers.size() + 1);
        newLedgers.addAll(this.ledgers);
        newLedgers.add(ledger);
        LogMetadata newMetadata = new LogMetadata(newEpoch, Collections.unmodifiableList(newLedgers), this.truncationAddress);
        newMetadata.setLastAppendSequence(this.lastAppendSequence.get());
        newMetadata.setUpdateVersion(this.updateVersion.get());
        return newMetadata;
    }

    //endregion

    long recordWrite(long length) {
        return this.lastAppendSequence.addAndGet(length);
    }

    void setLastAppendSequence(long value) {
        this.lastAppendSequence.set(value);
    }

    long getLastAppendSequence() {
        return this.lastAppendSequence.get();
    }

    int getUpdateVersion() {
        return this.updateVersion.get();
    }

    void setUpdateVersion(int value) {
        this.updateVersion.set(value);
    }

    boolean compareVersions(LogMetadata other) {
        if (other.updateVersion.get() == INITIAL_VERSION) {
            return true;
        } else {
            return this.updateVersion.get() == other.updateVersion.get();
        }
    }

    //region Serialization

    /**
     * Serializes this LogMetadata object into a byte array.
     *
     * @return A new byte array with the serialized contents of this object.
     */
    byte[] serialize() {
        // Serialization version (Byte), Epoch (Long), TruncationAddress (3*Long), Ledger Length (Int), Ledgers.
        // We do not serialize lastAppendSequence because it changes very frequently and can always be reconstructed upon recovery.
        ByteBuffer bb = ByteBuffer.allocate(Byte.BYTES + Long.BYTES + Long.BYTES * 3 + Integer.BYTES + LedgerMetadata.BYTES * this.ledgers.size());
        bb.put(SERIALIZATION_VERSION);
        bb.putLong(this.epoch);

        // Truncation Address.
        bb.putLong(this.truncationAddress.getSequence());
        bb.putLong(this.truncationAddress.getLedgerId());
        bb.putLong(this.truncationAddress.getEntryId());

        // Ledgers.
        bb.putInt(this.ledgers.size());
        this.ledgers.forEach(lm -> {
            bb.putLong(lm.ledgerId);
            bb.putLong(lm.startOffset);
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
        long truncationEntryId = bb.getLong();

        // Ledgers
        int ledgerCount = bb.getInt();
        List<LedgerMetadata> ledgers = new ArrayList<>(ledgerCount);
        for (int i = 0; i < ledgerCount; i++) {
            long ledgerId = bb.getLong();
            long offset = bb.getLong();
            ledgers.add(new LedgerMetadata(ledgerId, offset));
        }

        return new LogMetadata(epoch, Collections.unmodifiableList(ledgers), new LedgerAddress(truncationSeqNo, truncationLedgerId, truncationEntryId));
    }

    //endregion

    @Override
    public String toString() {
        return String.format("Version = %d, Epoch = %d, LedgerCount = %d", this.updateVersion.get(), this.epoch, this.ledgers.size());
    }

    @Data
    static class LedgerMetadata {
        private static final int BYTES = Long.BYTES * 2;
        private final long ledgerId;
        private final long startOffset;

        @Override
        public String toString() {
            return String.format("Id = %d, Offset = %d", this.ledgerId, this.startOffset);
        }
    }
}
