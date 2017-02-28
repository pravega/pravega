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
    private final TxnStatus txnStatus;

    public static ActiveTxRecord parse(final byte[] bytes) {
        final long txCreationTimestamp = Utilities.toLong(ArrayUtils.subarray(bytes, 0, Long.SIZE / 8));

        final TxnStatus status = TxnStatus.values()[Utilities.toInt(ArrayUtils.subarray(bytes, Long.SIZE / 8, bytes.length))];

        return new ActiveTxRecord(txCreationTimestamp, status);
    }

    public byte[] toByteArray() {
        final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

        try {
            outputStream.write(Utilities.toByteArray(txCreationTimestamp));

            outputStream.write(Utilities.toByteArray(txnStatus.ordinal()));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return outputStream.toByteArray();
    }
}
