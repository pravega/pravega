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
    private static final int INDEX_RECORD_SIZE = Integer.BYTES;
    private final int segmentNumber;
    private final int segmentOffset;

    public static Optional<SegmentIndexRecord> readRecord(final byte[] indexTable, final int segmentNumber) {
        if ((segmentNumber + 1) * INDEX_RECORD_SIZE > indexTable.length || segmentNumber < 0) {
            return Optional.empty();
        } else {
            return Optional.of(parse(indexTable, segmentNumber));
        }
    }

    public static Optional<SegmentIndexRecord> readLatestRecord(final byte[] indexTable) {
        return readRecord(indexTable, indexTable.length / INDEX_RECORD_SIZE - 1);
    }

    private static SegmentIndexRecord parse(final byte[] bytes, int segmentNumber) {
        int offset = segmentNumber * INDEX_RECORD_SIZE;
        final int segmentOffset = BitConverter.readInt(bytes, offset);
        return new SegmentIndexRecord(segmentNumber, segmentOffset);
    }

    public byte[] toByteArray() {
        byte[] b = new byte[INDEX_RECORD_SIZE];
        BitConverter.writeInt(b, 0, segmentOffset);

        return b;
    }

    int getIndexOffset() {
        return segmentNumber * INDEX_RECORD_SIZE;
    }

    static int getStartingSegmentNumberFromIndex(byte[] segmentIndex) {
        int offset = 0;
        while (offset < segmentIndex.length && BitConverter.readInt(segmentIndex, offset) == 0) {
            offset += INDEX_RECORD_SIZE;
        }

        return offset / INDEX_RECORD_SIZE - 1;
    }
}
