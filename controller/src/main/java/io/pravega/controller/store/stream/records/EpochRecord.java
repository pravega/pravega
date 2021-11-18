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

import com.google.common.collect.ImmutableList;
import io.pravega.common.ObjectBuilder;
import io.pravega.common.io.serialization.RevisionDataInput;
import io.pravega.common.io.serialization.RevisionDataOutput;
import io.pravega.common.io.serialization.VersionedSerializer;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.Data;
import lombok.Getter;
import lombok.NonNull;
import lombok.SneakyThrows;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Serializable class that captures an epoch record.
 */
@Data
public class EpochRecord {
    public static final EpochRecordSerializer SERIALIZER = new EpochRecordSerializer();
    public static final long DEFAULT_COUNT_VALUE = -1L;

    private final int epoch;
    /**
     * Reference epoch is either the same as epoch or the epoch that originated a chain of duplicates 
     * that includes this epoch. If we look at it as a graph, then it is a tree of depth one, where 
     * the root is the original epoch and the children are duplicates.
     */
    private final int referenceEpoch;
    private final ImmutableList<StreamSegmentRecord> segments;
    private final long creationTime;
    @Getter(AccessLevel.PRIVATE)
    private final Map<Long, StreamSegmentRecord> segmentMap;
    /**
     * Cumulative number of splits till this Epoch.
     */
    private final long splits;
    /**
     * Cumulative number of merges till this Epoch.
     */
    private final long merges;
    
    @Builder
    public EpochRecord(int epoch, int referenceEpoch, @NonNull ImmutableList<StreamSegmentRecord> segments, long creationTime, long splits, long merges) {
        this.epoch = epoch;
        this.referenceEpoch = referenceEpoch;
        this.segments = segments;
        this.creationTime = creationTime;
        this.segmentMap = segments.stream().collect(Collectors.toMap(StreamSegmentRecord::segmentId, x -> x));
        this.splits = splits;
        this.merges = merges;
    }

    public boolean hasSplitMergeCounts() {
        if (this.getSplits() != DEFAULT_COUNT_VALUE && this.getMerges() != DEFAULT_COUNT_VALUE) {
                return true;
            }
        return false;
    }
    
    @SneakyThrows(IOException.class)
    public byte[] toBytes() {
        return SERIALIZER.serialize(this).getCopy();
    }

    @Override
    public String toString() {
        return String.format("%s = %s", "epoch", epoch) + "\n" +
                String.format("%s = %s", "referenceEpoch", referenceEpoch) + "\n" +
                String.format("%s = [%n    %s%n]", "segments", segments.stream()
                        .map(streamSegmentRecord -> streamSegmentRecord.toString().replace("\n", "\n    "))
                        .collect(Collectors.joining("\n,\n    "))) + "\n" +
                String.format("%s = %s", "creationTime", creationTime) + "\n" +
                String.format("%s = %s", "splits", splits) + "\n" +
                String.format("%s = %s", "merges", merges);
    }

    public boolean isDuplicate() {
        return epoch != referenceEpoch;
    }

    @SneakyThrows(IOException.class)
    public static EpochRecord fromBytes(final byte[] record) {
        InputStream inputStream = new ByteArrayInputStream(record, 0, record.length);
        return SERIALIZER.deserialize(inputStream);
    }
    
    public Set<Long> getSegmentIds() {
        return segmentMap.keySet();
    }

    public StreamSegmentRecord getSegment(long segmentId) {
        return segmentMap.get(segmentId);
    }
    
    public boolean containsSegment(long segmentId) {
        return segmentMap.containsKey(segmentId);
    }

    private static class EpochRecordBuilder implements ObjectBuilder<EpochRecord> {
        private long splits = DEFAULT_COUNT_VALUE;
        private long merges = DEFAULT_COUNT_VALUE;

        private EpochRecordBuilder splits(final long splitCount) {
            this.splits = splitCount;
            return this;
        }

        private EpochRecordBuilder merges(final long mergeCount) {
            this.merges = mergeCount;
            return this;
        }

        @Override
        public EpochRecord build() {
            return new EpochRecord(epoch, referenceEpoch, segments, creationTime, splits, merges);
        }
    }
    
    private static class EpochRecordSerializer extends VersionedSerializer.WithBuilder<EpochRecord, EpochRecord.EpochRecordBuilder> {
        @Override
        protected byte getWriteVersion() {
            return 0;
        }

        @Override
        protected void declareVersions() {
            version(0).revision(0, this::write00, this::read00).revision(1, this::write01, this::read01);
        }

        private void read00(RevisionDataInput revisionDataInput, EpochRecord.EpochRecordBuilder builder) throws IOException {
            builder.epoch(revisionDataInput.readInt())
                   .referenceEpoch(revisionDataInput.readInt());
            ImmutableList.Builder<StreamSegmentRecord> segmentsBuilder = ImmutableList.builder();
            revisionDataInput.readCollection(StreamSegmentRecord.SERIALIZER::deserialize, segmentsBuilder);
            builder.segments(segmentsBuilder.build())
                   .creationTime(revisionDataInput.readLong());
        }

        private void write00(EpochRecord history, RevisionDataOutput revisionDataOutput) throws IOException {
            revisionDataOutput.writeInt(history.getEpoch());
            revisionDataOutput.writeInt(history.getReferenceEpoch());
            revisionDataOutput.writeCollection(history.getSegments(), StreamSegmentRecord.SERIALIZER::serialize);
            revisionDataOutput.writeLong(history.getCreationTime());
        }

        private void read01(RevisionDataInput revisionDataInput, EpochRecord.EpochRecordBuilder builder) throws IOException {
            builder.splits(revisionDataInput.readLong()).merges(revisionDataInput.readLong());
        }

        private void write01(EpochRecord epochRecord, RevisionDataOutput revisionDataOutput) throws IOException {
            revisionDataOutput.writeLong(epochRecord.getSplits());
            revisionDataOutput.writeLong(epochRecord.getMerges());
        }

        @Override
        protected EpochRecord.EpochRecordBuilder newBuilder() {
            return EpochRecord.builder();
        }
    }
}
