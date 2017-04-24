/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */

package com.emc.pravega.service.storage.impl.bookkeeper;

import com.emc.pravega.common.util.CollectionHelpers;
import com.google.common.base.Preconditions;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.val;

/**
 * Metadata for a Ledger-based log.
 */
class LogMetadata {
    //region Members

    private static final byte SERIALIZATION_VERSION = 0;
    /**
     * The initial epoch to use for the Log.
     */
    private static final long INITIAL_EPOCH = 1;

    /**
     * The initial version for the metadata (for an empty log). Every time the metadata is persisted, its version is incremented.
     */
    private static final int INITIAL_VERSION = -1;

    /**
     * Sequence number of the first ledger in the log.
     */
    private static final int INITIAL_LEDGER_SEQUENCE = 1;

    /**
     * A LogAddress to be used when the log is not truncated (initially). Setting all values to 0 is OK as BookKeeper never
     * has a LedgerId that is 0, so this will never overlap with the first entry in the log.
     */
    private static final LedgerAddress INITIAL_TRUNCATION_ADDRESS = new LedgerAddress(INITIAL_LEDGER_SEQUENCE - 1, 0, 0);

    /**
     * The current epoch of the metadata. The epoch is incremented upon every successful recovery (as opposed from version,
     * which is incremented every time the metadata is persisted).
     */
    @Getter
    private final long epoch;

    /**
     * An ordered list of LedgerMetadatas that represent the ledgers in the log.
     */
    @Getter
    private final List<LedgerMetadata> ledgers;

    /**
     * The Address of the last write that was truncated out of the log. Every read will start from the next element.
     */
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
        this.epoch = epoch;
        this.ledgers = Preconditions.checkNotNull(ledgers, "ledgers");
        this.truncationAddress = Preconditions.checkNotNull(truncationAddress, "truncationAddress");
        this.updateVersion = new AtomicInteger(INITIAL_VERSION);
    }

    //endregion

    //region Operations

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
        return new LogMetadata(newEpoch, Collections.unmodifiableList(newLedgers), this.truncationAddress)
                .withUpdateVersion(this.updateVersion.get());
    }

    /**
     * Creates a new instance of the LogMetadata class which contains all the ledgers after (and including) the given address.
     *
     * @param upToAddress The address to truncate to.
     * @return A new instance of the LogMetadata class.
     */
    LogMetadata truncate(LedgerAddress upToAddress) {
        // Exclude all those Ledgers that have a LedgerId less than the one we are given. An optimization to this would
        // involve trimming out the ledger which has a matching ledger id and the entry is is the last one, but that would
        // involve opening the Ledger in BookKeeper and inspecting it, which would take too long.
        val newLedgers = this.ledgers.stream().filter(lm -> lm.getLedgerId() >= upToAddress.getLedgerId()).collect(Collectors.toList());
        return new LogMetadata(this.epoch, Collections.unmodifiableList(newLedgers), upToAddress)
                .withUpdateVersion(this.updateVersion.get());
    }

    /**
     * Gets a value indicating the current version of the Metadata (this changes upon every successful metadata persist).
     * Note: this is different from getEpoch() - which gets incremented with every successful recovery.
     *
     * @return The current version.
     */
    int getUpdateVersion() {
        return this.updateVersion.get();
    }

    /**
     * Updates the current version of the metadata.
     *
     * @param value The new metadata version.
     * @return This instance.
     */
    LogMetadata withUpdateVersion(int value) {
        Preconditions.checkArgument(value >= this.updateVersion.get(), "versions must increase");
        this.updateVersion.set(value);
        return this;
    }

    /**
     * Gets the LedgerMetadata for the ledger with given ledger Id.
     *
     * @param ledgerId The Ledger Id to search.
     * @return The sought LedgerMetadata, or null if not found.
     */
    LedgerMetadata getLedger(long ledgerId) {
        int index = getLedgerMetadataIndex(ledgerId);
        if (index >= 0) {
            return this.ledgers.get(index);
        }

        return null;
    }

    /**
     * Gets the Ledger Address immediately following the given address.
     *
     * @param address     The current address.
     * @param lastEntryId If known, then Entry Id of the last entry in the ledger to which address is pointing. This is
     *                    used to determine if the next address should be returned on the next ledger. If not known,
     *                    this should be Long.MAX_VALUE, in which case the next address will always be on the same ledger.
     * @return The next address, or null if no such address exists (i.e., if we reached the end of the log).
     */
    LedgerAddress getNextAddress(LedgerAddress address, long lastEntryId) {
        if (this.ledgers.size() == 0) {
            // Quick bail-out. Nothing to return.
            return null;
        }

        LedgerAddress result = null;
        LedgerMetadata firstLedger = this.ledgers.get(0);
        if (address.getLedgerSequence() < firstLedger.getSequence()) {
            // Most likely an old address. The result is the first address of the first ledger we have.
            result = new LedgerAddress(firstLedger, 0);
        } else if (address.getEntryId() < lastEntryId) {
            // Same ledger, next entry.
            result = new LedgerAddress(address.getLedgerSequence(), address.getLedgerId(), address.getEntryId() + 1);
        } else {
            // Next ledger. First try a binary search, hoping the ledger in the address actually exists.
            LedgerMetadata ledgerMetadata = null;
            int index = getLedgerMetadataIndex(address.getLedgerId()) + 1;
            if (index > 0) {
                // Ledger is in the list. Make sure it's not the last one.
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
                result = new LedgerAddress(ledgerMetadata, 0);
            }
        }

        if (result != null && result.compareTo(this.truncationAddress) < 0) {
            result = this.truncationAddress;
        }

        return result;
    }

    private int getLedgerMetadataIndex(long ledgerId) {
        return CollectionHelpers.binarySearch(this.ledgers, lm -> Long.compare(ledgerId, lm.getLedgerId()));
    }

    //endregion

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
        return String.format("Version = %d, Epoch = %d, LedgerCount = %d, Truncate = (%d-%d)",
                this.updateVersion.get(), this.epoch, this.ledgers.size(), this.truncationAddress.getLedgerId(), this.truncationAddress.getEntryId());
    }
}
