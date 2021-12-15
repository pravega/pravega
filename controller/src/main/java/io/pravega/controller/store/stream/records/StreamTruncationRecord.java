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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.pravega.common.ObjectBuilder;
import io.pravega.common.io.serialization.RevisionDataInput;
import io.pravega.common.io.serialization.RevisionDataOutput;
import io.pravega.common.io.serialization.VersionedSerializer;
import lombok.Builder;
import lombok.Data;
import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Comparator;
import java.util.stream.Collectors;

/**
 * Data class for storing information about stream's truncation point.
 */
@Data
@Slf4j
public class StreamTruncationRecord {
    public static final TruncationRecordSerializer SERIALIZER = new TruncationRecordSerializer();

    public static final StreamTruncationRecord EMPTY = new StreamTruncationRecord(ImmutableMap.of(),
            ImmutableMap.of(), ImmutableSet.of(), ImmutableSet.of(), 0L, false);

    /**
     * Stream cut that is applied as part of this truncation.
     */
    private final ImmutableMap<Long, Long> streamCut;

    /**
     * If a stream cut spans across multiple epochs then this map captures mapping of segments from the stream cut to
     * epochs they were found in closest to truncation point.
     * This data structure is used to find active segments wrt a stream cut.
     * So for example:
     * epoch 0: 0, 1
     * epoch 1: 0, 2, 3
     * epoch 2: 0, 2, 4, 5
     * epoch 3: 0, 4, 5, 6, 7
     *
     * Following is a valid stream cut {0/offset, 3/offset, 6/offset, 7/offset}
     * This spans from epoch 1 till epoch 3. Any request for segments at epoch 1 or 2 or 3 will need to have this stream cut
     * applied on it to find segments that are available for consumption.
     * Refer to TableHelper.getActiveSegmentsAt
     */
    private final ImmutableMap<StreamSegmentRecord, Integer> span;
    private final int spanEpochLow;
    private final int spanEpochHigh;
    /**
     * All segments that have been deleted for this stream so far.
     */
    private final ImmutableSet<Long> deletedSegments;
    /**
     * Segments to delete as part of this truncation.
     * This is non empty while truncation is ongoing.
     * This is reset to empty once truncation completes by calling mergeDeleted method.
     */
    private final ImmutableSet<Long> toDelete;
    /**
     * Size till stream cut.
     */
    private final long sizeTill;
    
    private final boolean updating;

    @Builder
    public StreamTruncationRecord(@NonNull ImmutableMap<Long, Long> streamCut, @NonNull ImmutableMap<StreamSegmentRecord, Integer> span,
                                  @NonNull ImmutableSet<Long> deletedSegments, @NonNull ImmutableSet<Long> toDelete, 
                                  long sizeTill, boolean updating) {
        this.streamCut = streamCut;
        this.span = span;
        this.deletedSegments = deletedSegments;
        this.toDelete = toDelete;
        this.sizeTill = sizeTill;
        this.updating = updating;
        this.spanEpochLow = span.values().stream().min(Comparator.naturalOrder()).orElse(Integer.MIN_VALUE);
        this.spanEpochHigh = span.values().stream().max(Comparator.naturalOrder()).orElse(Integer.MIN_VALUE);
    }
    
    /**
     * Method to complete a given ongoing truncation record by setting updating flag to false and merging toDelete in deletedSegments. 
     * @param toComplete record to complete
     * @return new record that has the updating flag set to false
     */
    public static StreamTruncationRecord complete(StreamTruncationRecord toComplete) {
        Preconditions.checkState(toComplete.updating);
        ImmutableSet.Builder<Long> builder = ImmutableSet.builder();
        
        builder.addAll(toComplete.deletedSegments);
        builder.addAll(toComplete.toDelete);

        return StreamTruncationRecord.builder()
                                     .updating(false)
                                     .span(toComplete.span)
                                     .streamCut(toComplete.streamCut)
                                     .deletedSegments(builder.build())
                                     .toDelete(ImmutableSet.of())
                                     .sizeTill(toComplete.sizeTill)
                                     .build();
    }

    private static class StreamTruncationRecordBuilder implements ObjectBuilder<StreamTruncationRecord> {

    }

    @SneakyThrows(IOException.class)
    public static StreamTruncationRecord fromBytes(final byte[] data) {
        return SERIALIZER.deserialize(data);
    }

    @SneakyThrows(IOException.class)
    public byte[] toBytes() {
        return SERIALIZER.serialize(this).getCopy();
    }

    @Override
    public String toString() {
        return String.format("%s = %s", "streamCut", streamCut.keySet().stream()
                .map(key -> key + " : " + streamCut.get(key))
                .collect(Collectors.joining(", ", "{", "}"))) + "\n" +
                String.format("%s = {%n    %s%n}", "span", span.keySet().stream()
                        .map(streamSegmentRecord ->
                                String.format("key: %n    %s%nvalue: %s", streamSegmentRecord.toString().replace("\n", "\n    "),
                                        span.get(streamSegmentRecord)).replace("\n", "\n    "))
                        .collect(Collectors.joining("\n,\n    "))) + "\n" +
                String.format("%s = %s", "deletedSegments", deletedSegments) + "\n" +
                String.format("%s = %s", "toDelete", toDelete) + "\n" +
                String.format("%s = %s", "sizeTill", sizeTill) + "\n" +
                String.format("%s = %s", "updating", updating);
    }
    
    private static class TruncationRecordSerializer
            extends VersionedSerializer.WithBuilder<StreamTruncationRecord, StreamTruncationRecordBuilder> {
        @Override
        protected byte getWriteVersion() {
            return 0;
        }

        @Override
        protected void declareVersions() {
            version(0).revision(0, this::write00, this::read00);
        }

        private void read00(RevisionDataInput revisionDataInput,
                            StreamTruncationRecordBuilder truncationRecordBuilder)
                throws IOException {
            ImmutableMap.Builder<Long, Long> streamCutBuilder = ImmutableMap.builder();
            revisionDataInput.readMap(DataInput::readLong, DataInput::readLong, streamCutBuilder);
            truncationRecordBuilder
                    .streamCut(streamCutBuilder.build());
            ImmutableMap.Builder<StreamSegmentRecord, Integer> spanBuilder = ImmutableMap.builder();
            revisionDataInput.readMap(StreamSegmentRecord.SERIALIZER::deserialize, DataInput::readInt, spanBuilder);
            truncationRecordBuilder.span(spanBuilder.build());
            ImmutableSet.Builder<Long> deletedSegmentsBuilder = ImmutableSet.builder();
            revisionDataInput.readCollection(DataInput::readLong, deletedSegmentsBuilder);
            truncationRecordBuilder.deletedSegments(deletedSegmentsBuilder.build());

            ImmutableSet.Builder<Long> toDeleteBuilder = ImmutableSet.builder();
            revisionDataInput.readCollection(DataInput::readLong, toDeleteBuilder);
            truncationRecordBuilder.toDelete(toDeleteBuilder.build());
            truncationRecordBuilder
                    .sizeTill(revisionDataInput.readLong())
                    .updating(revisionDataInput.readBoolean());
        }

        private void write00(StreamTruncationRecord streamTruncationRecord, RevisionDataOutput revisionDataOutput)
                throws IOException {
            revisionDataOutput.writeMap(streamTruncationRecord.getStreamCut(), DataOutput::writeLong, DataOutput::writeLong);
            revisionDataOutput.writeMap(streamTruncationRecord.getSpan(), StreamSegmentRecord.SERIALIZER::serialize, DataOutput::writeInt);
            revisionDataOutput.writeCollection(streamTruncationRecord.getDeletedSegments(), DataOutput::writeLong);
            revisionDataOutput.writeCollection(streamTruncationRecord.getToDelete(), DataOutput::writeLong);
            revisionDataOutput.writeLong(streamTruncationRecord.sizeTill);
            revisionDataOutput.writeBoolean(streamTruncationRecord.isUpdating());
        }

        @Override
        protected StreamTruncationRecordBuilder newBuilder() {
            return StreamTruncationRecord.builder();
        }
    }
}
