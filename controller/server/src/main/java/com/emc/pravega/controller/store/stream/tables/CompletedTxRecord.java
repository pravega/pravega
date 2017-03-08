/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.controller.store.stream.tables;

import lombok.Data;
import org.apache.commons.lang.ArrayUtils;
import com.emc.pravega.controller.store.stream.TxnStatus;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

@Data
public class CompletedTxRecord {
    private final long completeTime;
    private final TxnStatus completionStatus;

    public static CompletedTxRecord parse(final byte[] bytes) {
        final long completeTimeStamp = Utilities.toLong(ArrayUtils.subarray(bytes, 0, Long.SIZE / 8));

        final TxnStatus status = TxnStatus.values()[Utilities.toInt(ArrayUtils.subarray(bytes, Long.SIZE / 8, bytes.length))];

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
