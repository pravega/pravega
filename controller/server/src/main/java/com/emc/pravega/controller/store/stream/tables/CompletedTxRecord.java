/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package com.emc.pravega.controller.store.stream.tables;

import com.emc.pravega.common.util.BitConverter;
import com.emc.pravega.stream.impl.TxnStatus;
import lombok.Data;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

@Data
public class CompletedTxRecord {
    private final long completeTime;
    private final TxnStatus completionStatus;

    public static CompletedTxRecord parse(final byte[] bytes) {
        final long completeTimeStamp = BitConverter.readLong(bytes, 0);

        final TxnStatus status = TxnStatus.values()[BitConverter.readInt(bytes, Long.BYTES)];

        return new CompletedTxRecord(completeTimeStamp, status);
    }

    public byte[] toByteArray() {
        final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

        try {
            outputStream.write(Utilities.toByteArray(completeTime));

            outputStream.write(Utilities.toByteArray(completionStatus.ordinal()));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return outputStream.toByteArray();

    }

}
