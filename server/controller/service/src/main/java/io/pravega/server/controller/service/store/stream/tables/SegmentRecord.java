/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package io.pravega.server.controller.service.store.stream.tables;

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
    public static final int SEGMENT_RECORD_SIZE = Integer.BYTES + Long.BYTES + Double.BYTES + Double.BYTES;
    public static final int SEGMENT_CHUNK_SIZE = 100000;

    private final int segmentNumber;
    private final long startTime;
    private final double routingKeyStart;
    private final double routingKeyEnd;

    public static Optional<SegmentRecord> readRecord(final byte[] segmentTable, final int number) {
        int offset = (number % SegmentRecord.SEGMENT_CHUNK_SIZE) * SegmentRecord.SEGMENT_RECORD_SIZE;

        if (offset >= segmentTable.length) {
            return Optional.empty();
        }
        return Optional.of(parse(segmentTable, offset));
    }

    private static SegmentRecord parse(final byte[] table, final int offset) {
        return new SegmentRecord(BitConverter.readInt(table, offset),
                BitConverter.readLong(table, offset + Integer.BYTES),
                toDouble(table, offset + Integer.BYTES + Long.BYTES),
                toDouble(table, offset + Integer.BYTES + Long.BYTES + Double.BYTES));
    }

    private static double toDouble(byte[] b, int offset) {
        return Double.longBitsToDouble(BitConverter.readLong(b, offset));
    }

    public byte[] toByteArray() {
        byte[] b = new byte[SEGMENT_RECORD_SIZE];
        BitConverter.writeInt(b, 0, segmentNumber);
        BitConverter.writeLong(b, Integer.BYTES, startTime);
        BitConverter.writeLong(b, Integer.BYTES + Long.BYTES, Double.doubleToRawLongBits(routingKeyStart));
        BitConverter.writeLong(b, Integer.BYTES + 2 * Long.BYTES, Double.doubleToRawLongBits(routingKeyEnd));

        return b;
    }
}
