/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package com.emc.pravega.controller.store.stream.tables;

import com.emc.pravega.common.util.BitConverter;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import org.apache.commons.lang.ArrayUtils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

@Data
@RequiredArgsConstructor
/**
 * Class corresponding to one row in the HistoryTable.
 * HistoryRecords are of variable length, so we will maintain markers for
 * start of row and end of row. We need it in both directions because we need to traverse
 * in both directions on the history table
 * Row : [eventTime, List-of-active-segment-numbers]
 */
public class HistoryRecord {
    private final int endOfRowPointer;
    private final long eventTime;
    private final List<Integer> segments;
    private final int startOfRowPointer;

    public HistoryRecord(final long eventTime, final List<Integer> segments, final int offset) {
        this.eventTime = eventTime;
        this.segments = segments;
        this.startOfRowPointer = offset;
        endOfRowPointer = offset + (Integer.BYTES + Long.BYTES + segments.size() * Integer.BYTES + Integer.BYTES) - 1;
    }

    public static Optional<HistoryRecord> readRecord(final byte[] historyTable, final int offset) {
        if (offset >= historyTable.length) {
            return Optional.empty();
        }

        final int rowEndOffset = BitConverter.readInt(historyTable,
                offset);

        return Optional.of(parse(ArrayUtils.subarray(historyTable,
                offset,
                rowEndOffset + 1)));
    }

    public static Optional<HistoryRecord> readLatestRecord(final byte[] historyTable) {
        if (historyTable.length == 0) {
            return Optional.empty();
        }

        final int lastRowStartOffset = BitConverter.readInt(historyTable,
                historyTable.length - (Integer.BYTES));

        return readRecord(historyTable, lastRowStartOffset);
    }

    public static Optional<HistoryRecord> fetchNext(final HistoryRecord record, final byte[] historyTable) {
        assert historyTable.length >= record.getEndOfRowPointer();

        if (historyTable.length == record.getEndOfRowPointer()) {
            return Optional.empty();
        } else {
            return HistoryRecord.readRecord(historyTable, record.getEndOfRowPointer() + 1);
        }
    }

    public static Optional<HistoryRecord> fetchPrevious(final HistoryRecord record,
                                                        final byte[] historyTable) {
        if (record.getStartOfRowPointer() == 0) {
            return Optional.empty();
        } else {
            final int rowStartOffset = BitConverter.readInt(historyTable,
                    record.getStartOfRowPointer() - (Integer.BYTES));

            return HistoryRecord.readRecord(historyTable, rowStartOffset);
        }
    }

    private static HistoryRecord parse(final byte[] b) {
        final int endOfRowPtr = BitConverter.readInt(b, 0);
        final long eventTime = BitConverter.readLong(b, Integer.BYTES);

        final List<Integer> segments = extractSegments(ArrayUtils.subarray(b,
                Integer.BYTES + Long.BYTES,
                b.length - Integer.BYTES));

        final int startOfRowPtr = BitConverter.readInt(b, b.length - Integer.BYTES);

        return new HistoryRecord(endOfRowPtr, eventTime, segments, startOfRowPtr);
    }

    private static List<Integer> extractSegments(final byte[] b) {
        final List<Integer> result = new ArrayList<>();
        for (int i = 0; i < b.length; i = i + Integer.BYTES) {
            result.add(BitConverter.readInt(b, i));
        }
        return result;
    }

    public byte[] toByteArray() {
        final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

        try {
            outputStream.write(Utilities.toByteArray(endOfRowPointer));
            outputStream.write(Utilities.toByteArray(eventTime));
            for (Integer segment : segments) {
                outputStream.write(Utilities.toByteArray(segment));
            }

            outputStream.write(Utilities.toByteArray(startOfRowPointer));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return outputStream.toByteArray();
    }
}
