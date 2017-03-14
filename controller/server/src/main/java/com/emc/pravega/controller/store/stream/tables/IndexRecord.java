/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package com.emc.pravega.controller.store.stream.tables;

import com.emc.pravega.common.util.BitConverter;
import lombok.Data;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
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
    private final int historyOffset;

    public static Optional<IndexRecord> readRecord(final byte[] indexTable, final int offset) {
        if (offset >= indexTable.length) {
            return Optional.empty();
        } else {
            return Optional.of(parse(ArrayUtils.subarray(indexTable, offset, offset + INDEX_RECORD_SIZE)));
        }
    }

    public static Optional<IndexRecord> readLatestRecord(final byte[] indexTable) {
        final int lastIndexedRecordOffset = Integer.max(indexTable.length - IndexRecord.INDEX_RECORD_SIZE, 0);

        return readRecord(indexTable, lastIndexedRecordOffset);
    }

    public static Optional<IndexRecord> fetchPrevious(final byte[] indexTable, final int offset) {
        if (offset == 0) {
            return Optional.<IndexRecord>empty();
        } else {
            return IndexRecord.readRecord(indexTable, offset - IndexRecord.INDEX_RECORD_SIZE);
        }
    }

    public static Optional<IndexRecord> fetchNext(final byte[] indexTable, final int offset) {
        if (offset + IndexRecord.INDEX_RECORD_SIZE == indexTable.length) {
            return Optional.<IndexRecord>empty();
        } else {
            return readRecord(indexTable, offset + IndexRecord.INDEX_RECORD_SIZE);
        }
    }

    public static Pair<Integer, Optional<IndexRecord>> search(final long timestamp, final byte[] indexTable) {
        final int lower = 0;
        final int upper = (indexTable.length - IndexRecord.INDEX_RECORD_SIZE) / IndexRecord.INDEX_RECORD_SIZE;
        return binarySearchIndex(lower, upper, timestamp, indexTable);
    }

    private static IndexRecord parse(final byte[] bytes) {
        final long eventTime = BitConverter.readLong(bytes, 0);
        final int offset = BitConverter.readInt(bytes, Long.BYTES);
        return new IndexRecord(eventTime, offset);
    }

    private static Pair<Integer, Optional<IndexRecord>> binarySearchIndex(final int lower,
                                                                          final int upper,
                                                                          final long timestamp,
                                                                          final byte[] indexTable) {
        if (upper < lower || indexTable.length == 0) {
            return new ImmutablePair<>(0, Optional.empty());
        }

        final int offset = ((lower + upper) / 2) * IndexRecord.INDEX_RECORD_SIZE;

        final IndexRecord record = IndexRecord.readRecord(indexTable, offset).get();

        final Optional<IndexRecord> next = IndexRecord.fetchNext(indexTable, offset);

        if (record.getEventTime() <= timestamp) {
            if (!next.isPresent() || (next.get().getEventTime() > timestamp)) {
                return new ImmutablePair<>(offset, Optional.of(record));
            } else {
                return binarySearchIndex((lower + upper) / 2 + 1, upper, timestamp, indexTable);
            }
        } else {
            return binarySearchIndex(lower, (lower + upper) / 2 - 1, timestamp, indexTable);
        }
    }

    public byte[] toByteArray() {
        final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

        try {
            outputStream.write(Utilities.toByteArray(eventTime));
            outputStream.write(Utilities.toByteArray(historyOffset));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return outputStream.toByteArray();
    }
}
