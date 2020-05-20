/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.store.kvtable.records;

import com.google.common.annotations.VisibleForTesting;
import io.pravega.common.ObjectBuilder;
import io.pravega.common.io.serialization.RevisionDataInput;
import io.pravega.common.io.serialization.RevisionDataOutput;
import io.pravega.common.io.serialization.VersionedSerializer;
import io.pravega.shared.NameUtils;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

import java.io.IOException;
import java.util.Map;

/**
 * Data class for KVTable segment record.
 */
@Data
@Builder
@AllArgsConstructor
public class KVTSegmentRecord {
    public static final KVTSegmentRecordSerializer SERIALIZER = new KVTSegmentRecordSerializer();

    private final int segmentNumber;
    private final int creationEpoch;
    private final long creationTime;
    private final double keyStart;
    private final double keyEnd;

    public static class KVTSegmentRecordBuilder implements ObjectBuilder<KVTSegmentRecord> {

    }

    public long segmentId() {
        return NameUtils.computeSegmentId(segmentNumber, creationEpoch);
    }

    /**
     * Method to check if given segment overlaps with this segment.
     * @param segment segment to check overlap for
     * @return true if they overlap, false otherwise
     */
    public boolean overlaps(final KVTSegmentRecord segment) {
        return segment.getKeyStart() < keyEnd && segment.getKeyEnd() > keyStart;
    }

    /**
     * Method to check if this segment overlaps with given range.
     * @param keyStart key start
     * @param keyEnd key end
     * @return true if they overlap, false otherwise
     */
    public boolean overlaps(final double keyStart, final double keyEnd) {
        return keyEnd > this.keyStart && keyStart < this.keyEnd;
    }

    /**
     * Method to check if two  segment overlaps.
     * @param first first segment
     * @param second second segment
     * @return true if they overlap, false otherwise
     */
    public static boolean overlaps(final Map.Entry<Double, Double> first,
                                   final Map.Entry<Double, Double> second) {
        return second.getValue() > first.getKey() && second.getKey() < first.getValue();
    }

    @VisibleForTesting
    public static KVTSegmentRecord newSegmentRecord(int num, int epoch, long time, double start, double end) {
        return KVTSegmentRecord.builder().segmentNumber(num).creationEpoch(epoch).creationTime(time).keyStart(start).keyEnd(end).build();
    }

    static class KVTSegmentRecordSerializer extends VersionedSerializer.WithBuilder<KVTSegmentRecord, KVTSegmentRecord.KVTSegmentRecordBuilder> {
        @Override
        protected byte getWriteVersion() {
            return 0;
        }

        @Override
        protected void declareVersions() {
            version(0).revision(0, this::write00, this::read00);
        }

        private void read00(RevisionDataInput revisionDataInput, KVTSegmentRecord.KVTSegmentRecordBuilder builder) throws IOException {
            builder.segmentNumber(revisionDataInput.readInt())
                   .creationTime(revisionDataInput.readLong())
                   .creationEpoch(revisionDataInput.readInt())
                   .keyStart(Double.longBitsToDouble(revisionDataInput.readLong()))
                   .keyEnd(Double.longBitsToDouble(revisionDataInput.readLong()));
        }

        private void write00(KVTSegmentRecord segment, RevisionDataOutput revisionDataOutput) throws IOException {
            revisionDataOutput.writeInt(segment.getSegmentNumber());
            revisionDataOutput.writeLong(segment.getCreationTime());
            revisionDataOutput.writeInt(segment.getCreationEpoch());
            revisionDataOutput.writeLong(Double.doubleToRawLongBits(segment.getKeyStart()));
            revisionDataOutput.writeLong(Double.doubleToRawLongBits(segment.getKeyEnd()));
        }

        @Override
        protected KVTSegmentRecord.KVTSegmentRecordBuilder newBuilder() {
            return KVTSegmentRecord.builder();
        }
    }

}
