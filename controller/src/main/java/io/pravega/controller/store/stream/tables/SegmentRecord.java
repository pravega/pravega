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
 * Class represents one row/record in SegmentTable.
 * Segment table is chunked into multiple files, each containing #SEGMENT_CHUNK_SIZE records.
 * New segment chunk-name is highest-chunk-name + 1
 * Row: [segment-number, segment-creation-time, routing-key-floor-inclusive, routing-key-ceiling-exclusive]
 */
public class SegmentRecord {
    public static final int SEGMENT_RECORD_SIZE = Integer.BYTES + Integer.BYTES + Long.BYTES + Double.BYTES + Double.BYTES;

    private final int segmentNumber;
    private final int epoch;
    private final long startTime;
    private final double routingKeyStart;
    private final double routingKeyEnd;

    /**
     * Method to read record for a specific segment number. 
     * @param segmentTable segment table
     * @param number segment number to read
     * @return returns segment record
     */
    static Optional<SegmentRecord> readRecord(final byte[] segmentTable, final int number) {
        int offset = number * SegmentRecord.SEGMENT_RECORD_SIZE;

        if (offset >= segmentTable.length) {
            return Optional.empty();
        }
        return Optional.of(parse(segmentTable, offset));
    }

    private static SegmentRecord parse(final byte[] table, final int offset) {
        return new SegmentRecord(BitConverter.readInt(table, offset),
                BitConverter.readInt(table, offset + Integer.BYTES),
                BitConverter.readLong(table, offset + 2 * Integer.BYTES),
                toDouble(table, offset + 2 * Integer.BYTES + Long.BYTES),
                toDouble(table, offset + 2 * Integer.BYTES + Long.BYTES + Double.BYTES));
    }

    private static double toDouble(byte[] b, int offset) {
        return Double.longBitsToDouble(BitConverter.readLong(b, offset));
    }

    byte[] toByteArray() {
        byte[] b = new byte[SEGMENT_RECORD_SIZE];
        BitConverter.writeInt(b, 0, segmentNumber);
        BitConverter.writeInt(b, Integer.BYTES, epoch);
        BitConverter.writeLong(b, 2 * Integer.BYTES, startTime);
        BitConverter.writeLong(b, 2 * Integer.BYTES + Long.BYTES, Double.doubleToRawLongBits(routingKeyStart));
        BitConverter.writeLong(b, 2 * Integer.BYTES + 2 * Long.BYTES, Double.doubleToRawLongBits(routingKeyEnd));

        return b;
    }
}
