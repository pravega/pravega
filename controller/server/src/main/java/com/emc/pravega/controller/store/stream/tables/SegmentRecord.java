/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package com.emc.pravega.controller.store.stream.tables;

import com.emc.pravega.common.util.BitConverter;
import lombok.Data;
import org.apache.commons.lang3.ArrayUtils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
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
        return Optional.of(parse(ArrayUtils.subarray(segmentTable, offset, offset + SEGMENT_RECORD_SIZE)));
    }

    private static SegmentRecord parse(final byte[] bytes) {
        assert bytes.length == SEGMENT_RECORD_SIZE;

        return new SegmentRecord(BitConverter.readInt(bytes, 0),
                BitConverter.readLong(bytes, Integer.BYTES),
                Utilities.toDouble(bytes, Integer.BYTES + Long.BYTES),
                Utilities.toDouble(bytes, Integer.BYTES + Long.BYTES + Double.BYTES));
    }

    public byte[] toByteArray() {
        final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

        try {
            outputStream.write(Utilities.toByteArray(segmentNumber));
            outputStream.write(Utilities.toByteArray(startTime));
            outputStream.write(Utilities.toByteArray(routingKeyStart));
            outputStream.write(Utilities.toByteArray(routingKeyEnd));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return outputStream.toByteArray();
    }
}
