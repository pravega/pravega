/**
 * Copyright Pravega Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.pravega.controller.store.stream.records;

import com.google.common.annotations.VisibleForTesting;
import io.pravega.common.ObjectBuilder;
import io.pravega.common.io.serialization.RevisionDataInput;
import io.pravega.common.io.serialization.RevisionDataOutput;
import io.pravega.common.io.serialization.VersionedSerializer;
import io.pravega.controller.store.SegmentRecord;
import io.pravega.shared.NameUtils;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

import java.io.IOException;
import java.util.Map;

/**
 * Data class for Stream segment record.
 */
@Data
@Builder
@AllArgsConstructor
public class StreamSegmentRecord implements SegmentRecord {
    public static final StreamSegmentRecordSerializer SERIALIZER = new StreamSegmentRecordSerializer();

    private final int segmentNumber;
    private final int creationEpoch;
    private final long creationTime;
    private final double keyStart;
    private final double keyEnd;

    public static class StreamSegmentRecordBuilder implements ObjectBuilder<StreamSegmentRecord> {

    }

    @Override
    public long segmentId() {
        return NameUtils.computeSegmentId(segmentNumber, creationEpoch);
    }

    /**
     * Method to check if given segment overlaps with this segment.
     * @param segment segment to check overlap for
     * @return true if they overlap, false otherwise
     */
    public boolean overlaps(final StreamSegmentRecord segment) {
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

    @Override
    public String toString() {
        return String.format("%s = %s", "segmentNumber", segmentNumber) + "\n" +
                String.format("%s = %s", "creationEpoch", creationEpoch) + "\n" +
                String.format("%s = %s", "creationTime", creationTime) + "\n" +
                String.format("%s = %s", "keyStart", keyStart) + "\n" +
                String.format("%s = %s", "keyEnd", keyEnd);
    }

    @VisibleForTesting
    public static StreamSegmentRecord newSegmentRecord(int num, int epoch, long time, double start, double end) {
        return StreamSegmentRecord.builder().segmentNumber(num).creationEpoch(epoch).creationTime(time).keyStart(start).keyEnd(end).build();
    }

    static class StreamSegmentRecordSerializer extends VersionedSerializer.WithBuilder<StreamSegmentRecord, StreamSegmentRecord.StreamSegmentRecordBuilder> {
        @Override
        protected byte getWriteVersion() {
            return 0;
        }

        @Override
        protected void declareVersions() {
            version(0).revision(0, this::write00, this::read00);
        }

        private void read00(RevisionDataInput revisionDataInput, StreamSegmentRecord.StreamSegmentRecordBuilder builder) throws IOException {
            builder.segmentNumber(revisionDataInput.readInt())
                   .creationTime(revisionDataInput.readLong())
                   .creationEpoch(revisionDataInput.readInt())
                   .keyStart(Double.longBitsToDouble(revisionDataInput.readLong()))
                   .keyEnd(Double.longBitsToDouble(revisionDataInput.readLong()));
        }

        private void write00(StreamSegmentRecord segment, RevisionDataOutput revisionDataOutput) throws IOException {
            revisionDataOutput.writeInt(segment.getSegmentNumber());
            revisionDataOutput.writeLong(segment.getCreationTime());
            revisionDataOutput.writeInt(segment.getCreationEpoch());
            revisionDataOutput.writeLong(Double.doubleToRawLongBits(segment.getKeyStart()));
            revisionDataOutput.writeLong(Double.doubleToRawLongBits(segment.getKeyEnd()));
        }

        @Override
        protected StreamSegmentRecord.StreamSegmentRecordBuilder newBuilder() {
            return StreamSegmentRecord.builder();
        }
    }

}
