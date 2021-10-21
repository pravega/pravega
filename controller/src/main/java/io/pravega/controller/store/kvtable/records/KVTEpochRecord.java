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
package io.pravega.controller.store.kvtable.records;

import com.google.common.collect.ImmutableList;
import io.pravega.common.ObjectBuilder;
import io.pravega.common.io.serialization.RevisionDataInput;
import io.pravega.common.io.serialization.RevisionDataOutput;
import io.pravega.common.io.serialization.VersionedSerializer;
import lombok.Data;
import lombok.Getter;
import lombok.Builder;
import lombok.AccessLevel;
import lombok.NonNull;
import lombok.SneakyThrows;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Serializable class that captures an epoch record for a KeyValueTable.
 */
@Data
public class KVTEpochRecord {
    public static final EpochRecordSerializer SERIALIZER = new EpochRecordSerializer();

    private final int epoch;
    private final ImmutableList<KVTSegmentRecord> segments;
    private final long creationTime;
    @Getter(AccessLevel.PRIVATE)
    private final Map<Long, KVTSegmentRecord> segmentMap;

    @Builder
    public KVTEpochRecord(int epoch, @NonNull ImmutableList<KVTSegmentRecord> segments, long creationTime) {
        this.epoch = epoch;
        this.segments = segments;
        this.creationTime = creationTime;
        this.segmentMap = segments.stream().collect(Collectors.toMap(KVTSegmentRecord::segmentId, x -> x));
    }
    
    @SneakyThrows(IOException.class)
    public byte[] toBytes() {
        return SERIALIZER.serialize(this).getCopy();
    }

    @SneakyThrows(IOException.class)
    public static KVTEpochRecord fromBytes(final byte[] record) {
        InputStream inputStream = new ByteArrayInputStream(record, 0, record.length);
        return SERIALIZER.deserialize(inputStream);
    }
    
    public Set<Long> getSegmentIds() {
        return segmentMap.keySet();
    }

    public KVTSegmentRecord getSegment(long segmentId) {
        return segmentMap.get(segmentId);
    }
    
    public boolean containsSegment(long segmentId) {
        return segmentMap.containsKey(segmentId);
    }

    private static class KVTEpochRecordBuilder implements ObjectBuilder<KVTEpochRecord> {

    }

    private static class EpochRecordSerializer extends VersionedSerializer.WithBuilder<KVTEpochRecord, KVTEpochRecord.KVTEpochRecordBuilder> {
        @Override
        protected byte getWriteVersion() {
            return 0;
        }

        @Override
        protected void declareVersions() {
            version(0).revision(0, this::write00, this::read00);
        }

        private void read00(RevisionDataInput revisionDataInput, KVTEpochRecord.KVTEpochRecordBuilder builder) throws IOException {
            builder.epoch(revisionDataInput.readInt());
            ImmutableList.Builder<KVTSegmentRecord> segmentsBuilder = ImmutableList.builder();
            revisionDataInput.readCollection(KVTSegmentRecord.SERIALIZER::deserialize, segmentsBuilder);
            builder.segments(segmentsBuilder.build())
                   .creationTime(revisionDataInput.readLong());
        }

        private void write00(KVTEpochRecord history, RevisionDataOutput revisionDataOutput) throws IOException {
            revisionDataOutput.writeInt(history.getEpoch());
            revisionDataOutput.writeCollection(history.getSegments(), KVTSegmentRecord.SERIALIZER::serialize);
            revisionDataOutput.writeLong(history.getCreationTime());
        }

        @Override
        protected KVTEpochRecord.KVTEpochRecordBuilder newBuilder() {
            return KVTEpochRecord.builder();
        }
    }
}
