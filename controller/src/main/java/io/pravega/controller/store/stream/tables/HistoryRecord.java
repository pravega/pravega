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
 * Row : [length][epoch][List-of-active-segment-numbers], [length-so-far], scaleTime][length]
 */
public class HistoryRecord {

    private static final int PARTIAL_FIELDS_FIXED_LENGTH = Integer.BYTES + Long.BYTES + Integer.BYTES;
    private static final int REMAINING_FIELDS_FIXED_LENGTH = Long.BYTES + Integer.BYTES;
    private static final int FIXED_FIELDS_LENGTH = PARTIAL_FIELDS_FIXED_LENGTH + REMAINING_FIELDS_FIXED_LENGTH;

    private final int length;
    @Getter
    private final long epoch;
    @Getter
    private final List<Integer> segments;
    @Getter
    private final int offset;
    @Getter
    private final long scaleTime;
    @Getter
    private final boolean partial;

    public HistoryRecord(long epoch, final List<Integer> segments, final int offset) {
        this(epoch, segments, 0L, offset, true);
    }

    public HistoryRecord(final List<Integer> segments, long epoch, final long scaleTime, final int offset) {
        this(epoch, segments, scaleTime, offset, false);
    }

    public HistoryRecord(long epoch, final List<Integer> segments, final long scaleTime, final int offset, boolean partial) {
        this.epoch = epoch;
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
        if (historyTable.length < PARTIAL_FIELDS_FIXED_LENGTH) {
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
        final int length = BitConverter.readInt(table, offset + lengthOffset());
        assert length > FIXED_FIELDS_LENGTH && (length - FIXED_FIELDS_LENGTH) % Integer.BYTES == 0;
        final long epoch = BitConverter.readLong(table, offset + epochOffset());
        int count = getCount(length);
        final List<Integer> segments = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            segments.add(BitConverter.readInt(table, offset + segmentOffset() + i * Integer.BYTES));
        }

        final int backToTop = BitConverter.readInt(table, offset + offsetOffset(count));
        assert backToTop == (count + 2) * Integer.BYTES + Long.BYTES;

        return new HistoryRecord(epoch, segments, offset);
    }

    private static HistoryRecord parse(final byte[] table, final int offset) {
        HistoryRecord partial = parsePartial(table, offset);

        final long eventTime = BitConverter.readLong(table, offset + scaleTimeOffset(partial.getSegments().size()));

        final int backToTop = BitConverter.readInt(table, offset + tailLengthOffset(partial.getSegments().size()));

        assert backToTop == partial.length;

        return new HistoryRecord(partial.epoch, partial.segments, eventTime, offset, false);
    }

    public byte[] toBytePartial() {
        byte[] b = new byte[PARTIAL_FIELDS_FIXED_LENGTH + segments.size() * Integer.BYTES];
        toBytePartial(b);
        return b;
    }

    private void toBytePartial(byte[] b) {
        // length
        BitConverter.writeInt(b, lengthOffset(), length);
        // epoch
        BitConverter.writeLong(b, epochOffset(), epoch);
        // segments
        for (int i = 0; i < segments.size(); i++) {
            BitConverter.writeInt(b, segmentOffset() + i * Integer.BYTES, segments.get(i));
        }
        // start offset
        BitConverter.writeInt(b, offsetOffset(segments.size()), length - Long.BYTES - Integer.BYTES);
    }

    public byte[] remainingByteArray() {
        byte[] b = new byte[REMAINING_FIELDS_FIXED_LENGTH];
        remainingByteArray(b, 0);
        return b;
    }

    private void remainingByteArray(byte[] b, int start) {
        BitConverter.writeLong(b, start, scaleTime);
        BitConverter.writeInt(b, start + Long.BYTES, length);
    }

    public byte[] toByteArray() {
        byte[] b = new byte[FIXED_FIELDS_LENGTH + segments.size() * Integer.BYTES];
        toBytePartial(b);
        remainingByteArray(b, PARTIAL_FIELDS_FIXED_LENGTH + segments.size() * Integer.BYTES);
        return b;
    }

    private static int getCount(int length) {
        return (length - FIXED_FIELDS_LENGTH) / Integer.BYTES;
    }

    private static int lengthOffset() {
        return 0;
    }

    private static int epochOffset() {
        return lengthOffset() + Integer.BYTES;
    }

    private static int segmentOffset() {
        return epochOffset() + Long.BYTES;
    }

    private static int offsetOffset(int count) {
        return segmentOffset() + count * Integer.BYTES;
    }

    private static int scaleTimeOffset(int segmentCount) {
        return offsetOffset(segmentCount) + Integer.BYTES;
    }

    private static int tailLengthOffset(int segmentCount) {
        return  scaleTimeOffset(segmentCount) + Long.BYTES;
    }
}
