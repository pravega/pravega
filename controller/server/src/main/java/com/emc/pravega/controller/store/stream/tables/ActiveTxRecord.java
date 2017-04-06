/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package com.emc.pravega.controller.store.stream.tables;


import com.emc.pravega.common.util.BitConverter;
import com.emc.pravega.controller.store.stream.TxnStatus;
import lombok.Data;

@Data
public class ActiveTxRecord {
    private static final int ACTIVE_TX_RECORD_SIZE = 4 * Long.BYTES + Integer.BYTES;
    private final long txCreationTimestamp;
    private final long leaseExpiryTime;
    private final long maxExecutionExpiryTime;
    private final long scaleGracePeriod;
    private final TxnStatus txnStatus;

    public static ActiveTxRecord parse(final byte[] bytes) {
        final int longSize = Long.BYTES;

        final long txCreationTimestamp = BitConverter.readLong(bytes, 0);

        final long leaseExpiryTime = BitConverter.readLong(bytes, longSize);

        final long maxExecutionExpiryTime = BitConverter.readLong(bytes, 2 * longSize);

        final long scaleGracePeriod = BitConverter.readLong(bytes, 3 * longSize);

        final TxnStatus status = TxnStatus.values()[BitConverter.readInt(bytes, 4 * longSize)];

        return new ActiveTxRecord(txCreationTimestamp, leaseExpiryTime, maxExecutionExpiryTime, scaleGracePeriod, status);
    }

    public byte[] toByteArray() {
        byte[] b = new byte[ACTIVE_TX_RECORD_SIZE];
        BitConverter.writeLong(b, 0, txCreationTimestamp);
        BitConverter.writeLong(b, Long.BYTES, leaseExpiryTime);
        BitConverter.writeLong(b, 2 * Long.BYTES, maxExecutionExpiryTime);
        BitConverter.writeLong(b, 3 * Long.BYTES, scaleGracePeriod);
        BitConverter.writeInt(b, 4 * Long.BYTES, txnStatus.ordinal());

        return b;
    }
}
