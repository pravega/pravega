/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.store.stream.records;

import io.pravega.common.util.BitConverter;
import lombok.Data;

import java.util.Optional;

@Data
/**
 * Class corresponding to a record/row in Index table.
 * Each row is fixed size
 * Row: [eventTime, pointer-into-history-table]
 */
public class HistoryIndexRecord {
    private static final int INDEX_RECORD_SIZE = Long.BYTES + Integer.BYTES;
    private final int epoch;
    private final long eventTime;
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

    public static Optional<HistoryIndexRecord> search(final long timestamp, final byte[] indexTable) {
        final int lower = 0;
        final int upper = (indexTable.length - INDEX_RECORD_SIZE) / INDEX_RECORD_SIZE;
        return binarySearchIndex(lower, upper, timestamp, indexTable);
    }

    private static HistoryIndexRecord parse(final byte[] bytes, int epoch) {
        int offset = epoch * INDEX_RECORD_SIZE;
        final long eventTime = BitConverter.readLong(bytes, offset);
        final int historyOffset = BitConverter.readInt(bytes, offset + Long.BYTES);
        return new HistoryIndexRecord(epoch, eventTime, historyOffset);
    }

    private static Optional<HistoryIndexRecord> binarySearchIndex(final int lower,
                                                                                 final int upper,
                                                                                 final long timestamp,
                                                                                 final byte[] indexTable) {
        if (upper < lower || indexTable.length == 0) {
            return Optional.empty();
        }

        final int epoch = (lower + upper) / 2;

        final HistoryIndexRecord record = HistoryIndexRecord.readRecord(indexTable, epoch).get();

        final Optional<HistoryIndexRecord> next = HistoryIndexRecord.readRecord(indexTable, epoch + 1);

        if (record.getEventTime() <= timestamp) {
            if (!next.isPresent() || (next.get().getEventTime() > timestamp)) {
                return Optional.of(record);
            } else {
                return binarySearchIndex((lower + upper) / 2 + 1, upper, timestamp, indexTable);
            }
        } else {
            return binarySearchIndex(lower, (lower + upper) / 2 - 1, timestamp, indexTable);
        }
    }

    public byte[] toByteArray() {
        byte[] b = new byte[INDEX_RECORD_SIZE];
        BitConverter.writeLong(b, 0, eventTime);
        BitConverter.writeInt(b, Long.BYTES, historyOffset);

        return b;
    }
}
