/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package com.emc.pravega.controller.store.stream.tables;

import com.emc.pravega.common.util.BitConverter;
import com.google.common.base.Preconditions;
import lombok.Getter;
import org.apache.commons.lang.ArrayUtils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Class corresponding to one row in the HistoryTable.
 * HistoryRecords are of variable length, so we will maintain markers for
 * start of row and end of row. We need it in both directions because we need to traverse
 * in both directions on the history table
 * Row : [EoR ptr][List-of-active-segment-numbers, [SoL ptr], eventTime][SoR ptr]
 */
public class HistoryRecord {
    private final int endOfRowPointer;
    private final int numOfSegments;
    @Getter
    private final List<Integer> segments;
    private final int startOfRowPointer1;
    @Getter
    private final long eventTime;
    private final int startOfRowPointer2;

    public HistoryRecord(final List<Integer> segments, final int offset) {
        this(segments, 0L, offset);
    }

    public HistoryRecord(final List<Integer> segments, final long eventTime, final int offset) {
        // Endofrow pointer is deterministic even for partial records.
        // All partial records have to be completed before a new record can be written.
        this.endOfRowPointer = offset + (Integer.BYTES + Integer.BYTES + segments.size() * Integer.BYTES +
                Integer.BYTES + Long.BYTES + Integer.BYTES) - 1;
        this.numOfSegments = segments.size();
        this.segments = segments;
        this.startOfRowPointer1 = offset;
        this.eventTime = eventTime;
        this.startOfRowPointer2 = offset;
    }

    public static Optional<HistoryRecord> readRecord(final byte[] historyTable, final int offset, boolean ignorePartial) {
        if (offset >= historyTable.length) {
            return Optional.empty();
        }

        // TODO: clean the method
        final int rowEndOffset = BitConverter.readInt(historyTable, offset);

        if (rowEndOffset > historyTable.length) {
            // this is a partial record
            if (ignorePartial) {
                return Optional.empty();
            } else {
                return Optional.of(parsePartial(ArrayUtils.subarray(historyTable,
                        offset,
                        historyTable.length)));
            }
        }

        return Optional.of(parse(ArrayUtils.subarray(historyTable,
                offset,
                rowEndOffset + 1)));
    }

    public static Optional<HistoryRecord> readLatestRecord(final byte[] historyTable, boolean ignorePartial) {
        if (historyTable.length == 0) {
            return Optional.empty();
        }

        // This will either be rowStartPointer1 or rowStartPointer2 both of which point to start of the row.
        final int lastRowStartOffset = BitConverter.readInt(historyTable,
                historyTable.length - (Integer.BYTES));

        // ignore partial means if latest record is partial, read the previous record
        Optional<HistoryRecord> record = readRecord(historyTable, lastRowStartOffset, false);
        assert record.isPresent();

        if (ignorePartial && record.get().isPartial()) {
            return fetchPrevious(record.get(), historyTable);
        } else {
            return record;
        }
    }

    public static Optional<HistoryRecord> fetchNext(final HistoryRecord record, final byte[] historyTable,
                                                    boolean ignorePartial) {
        Preconditions.checkArgument(historyTable.length >= record.getOffset());

        if (historyTable.length <= record.endOfRowPointer) {
            return Optional.empty();
        } else {
            return HistoryRecord.readRecord(historyTable, record.endOfRowPointer + 1, ignorePartial);
        }
    }

    public static Optional<HistoryRecord> fetchPrevious(final HistoryRecord record, final byte[] historyTable) {
        if (record.getOffset() == 0) { // if first record
            return Optional.empty();
        } else {
            final int rowStartOffset = BitConverter.readInt(historyTable,
                    record.getOffset() - (Integer.BYTES));

            return HistoryRecord.readRecord(historyTable, rowStartOffset, true);
        }
    }

    private static HistoryRecord parsePartial(final byte[] b) {
        final int endOfRowPtr = BitConverter.readInt(b, 0);
        final int count = BitConverter.readInt(b, Integer.BYTES);
        final List<Integer> segments = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            segments.add(BitConverter.readInt(b, 2 * Integer.BYTES + i * Integer.BYTES));
        }

        final int offset = BitConverter.readInt(b, (2 + count) * Integer.BYTES);

        HistoryRecord historyRecord = new HistoryRecord(segments, offset);
        assert historyRecord.endOfRowPointer == endOfRowPtr;
        return historyRecord;
    }

    private static HistoryRecord parse(final byte[] b) {
        HistoryRecord partial = parsePartial(b);

        final long eventTime = BitConverter.readLong(b, (3 + partial.numOfSegments) * Integer.BYTES);

        final int offset = BitConverter.readInt(b, b.length - Integer.BYTES);

        assert partial.endOfRowPointer - offset + 1 == b.length;

        return new HistoryRecord(partial.segments, eventTime, offset);
    }

    public byte[] toBytePartial() {
        final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        try {
            outputStream.write(Utilities.toByteArray(endOfRowPointer));
            outputStream.write(Utilities.toByteArray(segments.size()));
            for (Integer segment : segments) {
                outputStream.write(Utilities.toByteArray(segment));
            }
            outputStream.write(Utilities.toByteArray(startOfRowPointer1));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return outputStream.toByteArray();
    }

    public byte[] remainingByteArray() {
        final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

        try {
            outputStream.write(Utilities.toByteArray(eventTime));

            outputStream.write(Utilities.toByteArray(startOfRowPointer2));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return outputStream.toByteArray();
    }

    public byte[] toByteArray() {
        final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

        try {
            outputStream.write(Utilities.toByteArray(endOfRowPointer));
            outputStream.write(Utilities.toByteArray(segments.size()));
            for (Integer segment : segments) {
                outputStream.write(Utilities.toByteArray(segment));
            }
            outputStream.write(Utilities.toByteArray(startOfRowPointer1));

            outputStream.write(Utilities.toByteArray(eventTime));

            outputStream.write(Utilities.toByteArray(startOfRowPointer2));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return outputStream.toByteArray();
    }

    public int getOffset() {
        return startOfRowPointer1;
    }

    public boolean isPartial() {
        return eventTime == 0L;
    }

}
