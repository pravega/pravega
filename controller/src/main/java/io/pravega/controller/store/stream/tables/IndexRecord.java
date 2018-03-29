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
 * Row: [eventTime, pointer-into-history-table]
 */
public class IndexRecord {
    public static final int INDEX_RECORD_SIZE = Long.BYTES + Integer.BYTES;

    private final long eventTime;
    private final long epoch;
    private final int historyOffset;

    public static Optional<IndexRecord> readRecord(final byte[] indexTable, final int epoch) {
        int offset = epoch * INDEX_RECORD_SIZE;
        if (offset >= indexTable.length) {
            return Optional.empty();
        } else {
            return Optional.of(parse(indexTable, offset));
        }
    }

    public static Optional<IndexRecord> readLatestRecord(final byte[] indexTable) {
        final int lastIndexedRecordOffset = Integer.max(indexTable.length - IndexRecord.INDEX_RECORD_SIZE, 0);
        final int epoch = lastIndexedRecordOffset / INDEX_RECORD_SIZE;
        return readRecord(indexTable, epoch);
    }

    public static Optional<IndexRecord> search(final long timestamp, final byte[] indexTable) {
        final int lower = 0;
        final int upper = (indexTable.length - IndexRecord.INDEX_RECORD_SIZE) / IndexRecord.INDEX_RECORD_SIZE;
        return binarySearchIndex(lower, upper, timestamp, indexTable);
    }

    private static IndexRecord parse(final byte[] bytes, int offset) {
        final int epoch = offset / INDEX_RECORD_SIZE;
        final long eventTime = BitConverter.readLong(bytes, offset);
        final int historyOffset = BitConverter.readInt(bytes, offset + Long.BYTES);
        return new IndexRecord(eventTime, epoch, historyOffset);
    }

    private static Optional<IndexRecord> binarySearchIndex(final int lower,
                                         final int upper,
                                         final long timestamp,
                                         final byte[] indexTable) {
        if (upper < lower || indexTable.length == 0) {
            return Optional.empty();
        }

        final int middle = (lower + upper) / 2;

        final IndexRecord record = IndexRecord.readRecord(indexTable, middle).get();

        final Optional<IndexRecord> next = IndexRecord.readRecord(indexTable, middle);

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
