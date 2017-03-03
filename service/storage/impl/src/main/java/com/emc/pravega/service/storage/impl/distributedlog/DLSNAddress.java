/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package com.emc.pravega.service.storage.impl.distributedlog;

import com.emc.pravega.common.util.BitConverter;
import com.emc.pravega.service.storage.LogAddress;
import com.google.common.base.Preconditions;
import com.twitter.distributedlog.DLSN;

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
        byte[] result = new byte[Byte.BYTES + Long.BYTES * 4];
        result[0] = SERIALIZATION_VERSION;
        int offset = 1;
        BitConverter.writeLong(result, offset, getSequence());
        offset += Long.BYTES;
        BitConverter.writeLong(result, offset, this.dlsn.getLogSegmentSequenceNo());
        offset += Long.BYTES;
        BitConverter.writeLong(result, offset, this.dlsn.getEntryId());
        offset += Long.BYTES;
        BitConverter.writeLong(result, offset, this.dlsn.getSlotId());
        return result;
    }

    /**
     * Deserializes the given array into a DLSNAddress.
     *
     * @param serialization The byte array to read from.
     * @return a DLSNAddress.
     */
    static DLSNAddress deserialize(byte[] serialization) {
        // We skip version for now because we only have one.
        int offset = 1;
        long seqNo = BitConverter.readLong(serialization, offset);
        offset += Long.BYTES;
        long logSegmentSeqNo = BitConverter.readLong(serialization, offset);
        offset += Long.BYTES;
        long entryId = BitConverter.readLong(serialization, offset);
        offset += Long.BYTES;
        long slotId = BitConverter.readLong(serialization, offset);

        return new DLSNAddress(seqNo, new DLSN(logSegmentSeqNo, entryId, slotId));
    }
}
