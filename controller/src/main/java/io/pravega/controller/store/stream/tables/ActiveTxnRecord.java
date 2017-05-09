/*
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.store.stream.tables;


import io.pravega.common.util.BitConverter;
import io.pravega.controller.store.stream.TxnStatus;
import lombok.Data;

@Data
public class ActiveTxnRecord {
    private static final int ACTIVE_TXN_RECORD_SIZE = 4 * Long.BYTES + Integer.BYTES;
    private final long txCreationTimestamp;
    private final long leaseExpiryTime;
    private final long maxExecutionExpiryTime;
    private final long scaleGracePeriod;
    private final TxnStatus txnStatus;

    public static ActiveTxnRecord parse(final byte[] bytes) {
        final int longSize = Long.BYTES;

        final long txCreationTimestamp = BitConverter.readLong(bytes, 0);

        final long leaseExpiryTime = BitConverter.readLong(bytes, longSize);

        final long maxExecutionExpiryTime = BitConverter.readLong(bytes, 2 * longSize);

        final long scaleGracePeriod = BitConverter.readLong(bytes, 3 * longSize);

        final TxnStatus status = TxnStatus.values()[BitConverter.readInt(bytes, 4 * longSize)];

        return new ActiveTxnRecord(txCreationTimestamp, leaseExpiryTime, maxExecutionExpiryTime, scaleGracePeriod, status);
    }

    public byte[] toByteArray() {
        byte[] b = new byte[ACTIVE_TXN_RECORD_SIZE];
        BitConverter.writeLong(b, 0, txCreationTimestamp);
        BitConverter.writeLong(b, Long.BYTES, leaseExpiryTime);
        BitConverter.writeLong(b, 2 * Long.BYTES, maxExecutionExpiryTime);
        BitConverter.writeLong(b, 3 * Long.BYTES, scaleGracePeriod);
        BitConverter.writeInt(b, 4 * Long.BYTES, txnStatus.ordinal());

        return b;
    }
}
