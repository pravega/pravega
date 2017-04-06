/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package com.emc.pravega.controller.store.stream.tables;


import com.emc.pravega.common.util.BitConverter;
import com.emc.pravega.controller.store.stream.TxnStatus;
import lombok.Data;

@Data
public class CompletedTxRecord {
    private static final int COMPLETED_TX_RECORD_SIZE = Long.BYTES + Integer.BYTES;

    private final long completeTime;
    private final TxnStatus completionStatus;

    public static CompletedTxRecord parse(final byte[] bytes) {
        final long completeTimeStamp = BitConverter.readLong(bytes, 0);

        final TxnStatus status = TxnStatus.values()[BitConverter.readInt(bytes, Long.BYTES)];

        return new CompletedTxRecord(completeTimeStamp, status);
    }

    public byte[] toByteArray() {
        byte[] b = new byte[COMPLETED_TX_RECORD_SIZE];
        BitConverter.writeLong(b, 0, completeTime);
        BitConverter.writeInt(b, Long.BYTES, completionStatus.ordinal());

        return b;
    }

}
