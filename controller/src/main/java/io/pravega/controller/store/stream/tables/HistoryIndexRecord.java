/**
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
import lombok.Data;

import java.util.Optional;

@Data
/**
 * Class corresponding to a record/row in Index table.
 * Each row is fixed size
 * Row: [pointer-into-history-table]
 */
public class HistoryIndexRecord {
    private static final int INDEX_RECORD_SIZE = Integer.BYTES;
    private final int epoch;
    private final int historyOffset;

    public static Optional<HistoryIndexRecord> readRecord(final byte[] indexTable, final int epoch) {
        if ((epoch + 1) * INDEX_RECORD_SIZE > indexTable.length || epoch < 0) {
            return Optional.empty();
        } else {
            return Optional.of(parse(indexTable, epoch));
        }
    }

    public static Optional<HistoryIndexRecord> readLatestRecord(final byte[] historyIndex) {
        return readRecord(historyIndex, historyIndex.length / HistoryIndexRecord.INDEX_RECORD_SIZE - 1);
    }

    private static HistoryIndexRecord parse(final byte[] bytes, int epoch) {
        int offset = epoch * INDEX_RECORD_SIZE;
        final int historyOffset = BitConverter.readInt(bytes, offset);
        return new HistoryIndexRecord(epoch, historyOffset);
    }

    public byte[] toByteArray() {
        byte[] b = new byte[INDEX_RECORD_SIZE];
        BitConverter.writeInt(b, 0, historyOffset);

        return b;
    }

    int getIndexOffset() {
        return epoch * INDEX_RECORD_SIZE;
    }
}
