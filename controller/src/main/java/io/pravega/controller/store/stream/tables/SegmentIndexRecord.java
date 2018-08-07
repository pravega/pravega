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

/**
 * Class corresponding to a record/row in SegmentIndex.
 * Each row is fixed size
 * Row: [offset-in-Segment-table]
 */
@Data
public class SegmentIndexRecord {
    // The first (integer) record of this table contains the starting segment number for this stream. All the segments
    // in this stream will contain ids starting from the the value stored in the first record of this table.
    static final int STARTING_SEGMENT_NUMBER_POSITION = 0;
    static final int INDEX_RECORD_SIZE = Integer.BYTES;
    private final int segmentNumber;
    private final int segmentOffset;
    private final int startingSegmentNumber;

    public static Optional<SegmentIndexRecord> readRecord(final byte[] indexTable, final int segmentNumber) {
        // To get the appropriate index for segmentNumber, we need to subtract the starting segment number for this
        // stream and add 1 to take into account the storage space of the starting segment number value (first element).
        final int actualSegmentIndex = fromSegmentNumberToTableIndex(indexTable, segmentNumber);

        // Note that the segment index has now an extra header integer to store the starting segment number.
        if ((actualSegmentIndex + 1) * INDEX_RECORD_SIZE > indexTable.length || actualSegmentIndex < 0) {
            return Optional.empty();
        } else {
            return Optional.of(parse(indexTable, actualSegmentIndex));
        }
    }

    public static Optional<SegmentIndexRecord> readLatestRecord(final byte[] indexTable) {
        final int startingSegmentNumber = BitConverter.readInt(indexTable, STARTING_SEGMENT_NUMBER_POSITION);
        return readRecord(indexTable, startingSegmentNumber + indexTable.length / INDEX_RECORD_SIZE - 2);
    }

    private static SegmentIndexRecord parse(final byte[] bytes, int segmentIndexInTable) {
        int offset = segmentIndexInTable * INDEX_RECORD_SIZE;
        final int segmentOffset = BitConverter.readInt(bytes, offset);
        final int startingSegmentNumber = BitConverter.readInt(bytes, STARTING_SEGMENT_NUMBER_POSITION);
        return new SegmentIndexRecord(fromTableIndexToSegmentNumber(bytes, segmentIndexInTable), segmentOffset, startingSegmentNumber);
    }

    private static int fromSegmentNumberToTableIndex(final byte[] indexTable, final int segmentNumber) {
        return segmentNumber - BitConverter.readInt(indexTable, STARTING_SEGMENT_NUMBER_POSITION) + 1;
    }

    private static int fromTableIndexToSegmentNumber(final byte[] indexTable, final int segmentIndexInTable) {
        return segmentIndexInTable + BitConverter.readInt(indexTable, STARTING_SEGMENT_NUMBER_POSITION) - 1;
    }

    public byte[] toByteArray() {
        byte[] b = new byte[INDEX_RECORD_SIZE];
        BitConverter.writeInt(b, 0, segmentOffset);

        return b;
    }

    int getIndexOffset() {
        return (segmentNumber - startingSegmentNumber) * INDEX_RECORD_SIZE;
    }
}
