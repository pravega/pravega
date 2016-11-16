/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.emc.pravega.controller.store.stream.tables;

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
    public static final int SEGMENT_RECORD_SIZE = (Integer.SIZE + Long.SIZE + Double.SIZE + Double.SIZE) / 8;
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

        return new SegmentRecord(Utilities.toInt(ArrayUtils.subarray(bytes, 0, Integer.SIZE / 8)),
                Utilities.toLong(ArrayUtils.subarray(bytes, Integer.SIZE / 8, (Integer.SIZE + Long.SIZE) / 8)),
                Utilities.toDouble(ArrayUtils.subarray(bytes, (Integer.SIZE + Long.SIZE) / 8,
                        (Integer.SIZE + Long.SIZE + Double.SIZE) / 8)), Utilities.toDouble(
                ArrayUtils.subarray(bytes, (Integer.SIZE + Long.SIZE + Double.SIZE) / 8,
                        (Integer.SIZE + Long.SIZE + Double.SIZE + Double.SIZE) / 8)));
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
