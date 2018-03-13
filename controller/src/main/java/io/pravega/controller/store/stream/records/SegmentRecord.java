/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.store.stream.records;

import io.pravega.common.ObjectBuilder;
import io.pravega.common.io.serialization.VersionedSerializer;
import io.pravega.controller.store.stream.records.serializers.SegmentRecordSerializer;
import lombok.Builder;
import lombok.Data;
import lombok.Lombok;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Optional;

@Data
@Builder
/**
 * Class represents one row/record in SegmentTable. Since segment table records are versioned, we can have variable sized
 * records. So we maintain a segment index which identifies start offset for each row in the table.
 */
public class SegmentRecord {
    public static final VersionedSerializer.WithBuilder<SegmentRecord, SegmentRecord.SegmentRecordBuilder>
            SERIALIZER = new SegmentRecordSerializer();

    private final int segmentNumber;
    private final long startTime;
    private final int creationEpoch;
    private final double routingKeyStart;
    private final double routingKeyEnd;

    /**
     * Method to read record for a specific segment number. 
     * @param segmentIndex segment index
     * @param segmentTable segment table
     * @param number segment number to read
     * @return returns segment record
     */
    static Optional<SegmentRecord> readRecord(final byte[] segmentIndex, final byte[] segmentTable, final int number) {
        Optional<SegmentIndexRecord> indexRecord = SegmentIndexRecord.readRecord(segmentIndex, number);
        return indexRecord.map(segmentIndexRecord -> {
            if (indexRecord.get().getSegmentOffset() >= segmentTable.length) {
                // index may be ahead of segment table. So we may not be able to read the record.
                return null;
            } else {
                return parse(segmentTable, segmentIndexRecord.getSegmentOffset());
            }
        });
    }

    /**
     * Method to read latest record in segment table.
     * Note: index may be ahead of segment table. So we will find the last record in segment table and ignore index entries
     * that are not written to segment table yet.
     * @param segmentIndex segment index
     * @param segmentTable segment table
     * @return returns latest segment record in segment table
     */
    static Optional<SegmentRecord> readLatest(final byte[] segmentIndex, final byte[] segmentTable) {
        Optional<SegmentIndexRecord> indexRecord = SegmentIndexRecord.readLatestRecord(segmentIndex);

        while (indexRecord.isPresent()) {
            int segmentNumber = indexRecord.get().getSegmentNumber();
            if (indexRecord.get().getSegmentOffset() >= segmentTable.length) {
                indexRecord = SegmentIndexRecord.readRecord(segmentIndex, segmentNumber - 1);
            } else {
                return Optional.of(parse(segmentTable, indexRecord.get().getSegmentOffset()));
            }
        }

        return Optional.empty();
    }

    private static SegmentRecord parse(final byte[] table, final int offset) {
        InputStream bas = new ByteArrayInputStream(table, offset, table.length - offset);
        try {
            return SERIALIZER.deserialize(bas);
        } catch (IOException e) {
            throw Lombok.sneakyThrow(e);
        }
    }

    byte[] toByteArray() {
        try {
            return SERIALIZER.serialize(this).array();
        } catch (IOException e) {
            throw Lombok.sneakyThrow(e);
        }
    }

    public static class SegmentRecordBuilder implements ObjectBuilder<SegmentRecord> {

    }

}
