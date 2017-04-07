/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package com.emc.pravega.controller.store.stream.tables;

import com.emc.pravega.common.util.BitConverter;
import com.google.common.base.Preconditions;
import lombok.Getter;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Class corresponding to one row in the HistoryTable.
 * HistoryRecords are of variable length, so we will maintain markers for
 * start of row and end of row. We need it in both directions because we need to traverse
 * in both directions on the history table
 * Row : [length][List-of-active-segment-numbers, [length-so-far], scaleTime][length]
 */
public class HistoryRecord {

    private static final int FIXED_FIELDS_LENGTH = Integer.BYTES + Integer.BYTES + Long.BYTES + Integer.BYTES;
    private final int length;
    @Getter
    private final List<Integer> segments;
    @Getter
    private final int offset;
    @Getter
    private final long scaleTime;
    @Getter
    private final boolean partial;

    public HistoryRecord(final List<Integer> segments, final int offset) {
        this(segments, 0L, offset, true);
    }

    public HistoryRecord(final List<Integer> segments, final long scaleTime, final int offset) {
        this(segments, scaleTime, offset, false);
    }

    public HistoryRecord(final List<Integer> segments, final long scaleTime, final int offset, boolean partial) {
        this.offset = offset;
        this.length = FIXED_FIELDS_LENGTH + segments.size() * Integer.BYTES;
        this.segments = segments;
        this.scaleTime = scaleTime;
        this.partial = partial;
    }

    /**
     * Read record from the history table at the specified offset.
     *
     * @param historyTable  history table
     * @param offset        offset to read from
     * @param ignorePartial if set, ignore if the record is partial
     * @return
     */
    public static Optional<HistoryRecord> readRecord(final byte[] historyTable, final int offset, boolean ignorePartial) {
        if (offset >= historyTable.length) {
            return Optional.empty();
        }

        final int length = BitConverter.readInt(historyTable, offset);

        if (offset + length > historyTable.length) {
            // this is a partial record
            if (ignorePartial) {
                return Optional.empty();
            } else {
                return Optional.of(parsePartial(historyTable, offset));
            }
        }

        return Optional.of(parse(historyTable, offset));
    }

    /**
     * Return latest record in the history table.
     *
     * @param historyTable  history table
     * @param ignorePartial If latest entry is partial entry, if ignore is set then read the previous
     * @return returns the history record if it is found.
     */
    public static Optional<HistoryRecord> readLatestRecord(final byte[] historyTable, boolean ignorePartial) {
        if (historyTable.length == 0) {
            return Optional.empty();
        }

        final int backToTop = BitConverter.readInt(historyTable, historyTable.length - (Integer.BYTES));

        // ignore partial means if latest record is partial, read the previous record
        Optional<HistoryRecord> record = readRecord(historyTable, historyTable.length - backToTop, false);
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

        if (historyTable.length < record.offset + record.length) {
            return Optional.empty();
        } else {
            return HistoryRecord.readRecord(historyTable, record.offset + record.length, ignorePartial);
        }
    }

    public static Optional<HistoryRecord> fetchPrevious(final HistoryRecord record, final byte[] historyTable) {
        if (record.getOffset() == 0) { // if first record
            return Optional.empty();
        } else {
            final int rowStartOffset = record.offset - BitConverter.readInt(historyTable,
                    record.getOffset() - (Integer.BYTES));
            assert rowStartOffset >= 0;
            return HistoryRecord.readRecord(historyTable, rowStartOffset, true);
        }
    }

    private static HistoryRecord parsePartial(final byte[] table, final int offset) {
        final int length = BitConverter.readInt(table, offset);
        assert length > FIXED_FIELDS_LENGTH && (length - FIXED_FIELDS_LENGTH) % Integer.BYTES == 0;
        int count = (length - FIXED_FIELDS_LENGTH) / Integer.BYTES;
        final List<Integer> segments = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            segments.add(BitConverter.readInt(table, offset + (1 + i) * Integer.BYTES));
        }

        final int backToTop = BitConverter.readInt(table, offset + ((1 + count) * Integer.BYTES));
        assert backToTop == (count + 2) * Integer.BYTES;

        return new HistoryRecord(segments, offset);
    }

    private static HistoryRecord parse(final byte[] table, final int offset) {
        HistoryRecord partial = parsePartial(table, offset);

        final long eventTime = BitConverter.readLong(table, offset + (2 + partial.segments.size()) * Integer.BYTES);

        final int backToTop = BitConverter.readInt(table, offset + (2 + partial.segments.size()) * Integer.BYTES + Long.BYTES);

        assert backToTop == partial.length;

        return new HistoryRecord(partial.segments, eventTime, offset, false);
    }

    public byte[] toBytePartial() {
        byte[] b = new byte[(2 + segments.size()) * Integer.BYTES];

        BitConverter.writeInt(b, 0, length);
        for (int i = 0; i < segments.size(); i++) {
            BitConverter.writeInt(b, (1 + i) * Integer.BYTES, segments.get(i));
        }
        BitConverter.writeInt(b, (1 + segments.size()) * Integer.BYTES, length - Long.BYTES - Integer.BYTES);
        return b;
    }

    public byte[] remainingByteArray() {
        byte[] b = new byte[Long.BYTES + Integer.BYTES];
        BitConverter.writeLong(b, 0, scaleTime);
        BitConverter.writeInt(b, Long.BYTES, length);
        return b;
    }

    public byte[] toByteArray() {
        byte[] b = new byte[(segments.size()) * Integer.BYTES + FIXED_FIELDS_LENGTH];
        BitConverter.writeInt(b, 0, length);
        for (int i = 0; i < segments.size(); i++) {
            BitConverter.writeInt(b, (1 + i) * Integer.BYTES, segments.get(i));
        }
        BitConverter.writeInt(b, (1 + segments.size()) * Integer.BYTES, (2 + segments.size()) * Integer.BYTES);
        BitConverter.writeLong(b, (2 + segments.size()) * Integer.BYTES, scaleTime);
        BitConverter.writeInt(b, (2 + segments.size()) * Integer.BYTES + Long.BYTES, (3 + segments.size()) * Integer.BYTES + Long.BYTES);

        return b;
    }
}
