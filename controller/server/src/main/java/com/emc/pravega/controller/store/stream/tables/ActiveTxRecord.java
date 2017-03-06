/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.controller.store.stream.tables;

import com.emc.pravega.stream.impl.TxnStatus;
import lombok.Data;
import org.apache.commons.lang.ArrayUtils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

@Data
public class ActiveTxRecord {
    private final long txCreationTimestamp;
    private final long leaseExpiryTime;
    private final long maxExecutionExpiryTime;
    private final long scaleGracePeriod;
    private final TxnStatus txnStatus;

    public static ActiveTxRecord parse(final byte[] bytes) {
        final int longSize = Long.SIZE / 8;

        final long txCreationTimestamp = Utilities.toLong(ArrayUtils.subarray(bytes, 0, longSize));

        final long leaseExpiryTime = Utilities.toLong(ArrayUtils.subarray(bytes, longSize, 2 * longSize));

        final long maxExecutionExpiryTime = Utilities.toLong(ArrayUtils.subarray(bytes, 2 * longSize, 3 * longSize));

        final long scaleGracePeriod = Utilities.toLong(ArrayUtils.subarray(bytes, 3 * longSize, 4 * longSize));

        final TxnStatus status = TxnStatus.values()[Utilities.toInt(ArrayUtils.subarray(bytes, 4 * longSize, bytes.length))];

        return new ActiveTxRecord(txCreationTimestamp, leaseExpiryTime, maxExecutionExpiryTime, scaleGracePeriod, status);
    }

    public byte[] toByteArray() {
        final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

        try {
            outputStream.write(Utilities.toByteArray(txCreationTimestamp));
            outputStream.write(Utilities.toByteArray(leaseExpiryTime));
            outputStream.write(Utilities.toByteArray(maxExecutionExpiryTime));
            outputStream.write(Utilities.toByteArray(scaleGracePeriod));
            outputStream.write(Utilities.toByteArray(txnStatus.ordinal()));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return outputStream.toByteArray();
    }
}
