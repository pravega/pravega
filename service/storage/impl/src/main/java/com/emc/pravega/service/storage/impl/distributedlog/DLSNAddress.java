/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package com.emc.pravega.service.storage.impl.distributedlog;

import com.emc.pravega.service.storage.LogAddress;
import com.google.common.base.Preconditions;
import com.twitter.distributedlog.DLSN;
import java.nio.ByteBuffer;

/**
 * LogAddress for DistributedLog. Wraps around DistributedLog-specific addressing scheme, using DLSNs, without exposing
 * such information to the outside.
 */
class DLSNAddress extends LogAddress {
    private static final byte SERIALIZATION_VERSION = 1;
    private final DLSN dlsn;

    /**
     * Creates a new instance of the LogAddress class.
     *
     * @param sequence The sequence of the address (location).
     */
    DLSNAddress(long sequence, DLSN dlsn) {
        super(sequence);
        Preconditions.checkNotNull(dlsn, "dlsn");
        this.dlsn = dlsn;
    }

    /**
     * Gets the DLSN associated with this LogAddress.
     */
    DLSN getDLSN() {
        return this.dlsn;
    }

    @Override
    public String toString() {
        return String.format("%s, DLSN = %s", super.toString(), this.dlsn);
    }

    /**
     * Serializes this instance of DLSNAddress into a byte array.
     *
     * @return A byte array with the result.
     */
    byte[] serialize() {
        ByteBuffer bb = ByteBuffer.allocate(Byte.BYTES + Long.BYTES * 4);
        bb.put(SERIALIZATION_VERSION);
        bb.putLong(getSequence());
        bb.putLong(this.dlsn.getLogSegmentSequenceNo());
        bb.putLong(this.dlsn.getEntryId());
        bb.putLong(this.dlsn.getSlotId());
        return bb.array();
    }

    /**
     * Deserializes the given array into a DLSNAddress.
     *
     * @param serialization The byte array to read from.
     * @return a DLSNAddress.
     */
    static DLSNAddress deserialize(byte[] serialization) {
        ByteBuffer bb = ByteBuffer.wrap(serialization);
        bb.get(); // We skip version for now because we only have one.
        long seqNo = bb.getLong();
        long logSegmentSeqNo = bb.getLong();
        long entryId = bb.getLong();
        long slotId = bb.getLong();
        return new DLSNAddress(seqNo, new DLSN(logSegmentSeqNo, entryId, slotId));
    }
}
