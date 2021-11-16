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
import io.pravega.common.Exceptions;
import io.pravega.common.ObjectBuilder;
import io.pravega.common.io.serialization.RevisionDataInput;
import io.pravega.common.io.serialization.RevisionDataOutput;
import io.pravega.common.io.serialization.VersionedSerializer;
import lombok.Builder;
import lombok.Data;
import lombok.Getter;
import lombok.NonNull;
import lombok.SneakyThrows;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.stream.Collectors;

/**
 * Each HistoryTimeSeriesRecord captures delta between two consecutive epoch records.
 * To compute an epoch record from this time series, we need at least one complete epoch record and then we can
 * apply deltas on it iteratively until we reach the desired epoch record.
 */
@Data
public class HistoryTimeSeriesRecord {
    public static final HistoryTimeSeriesRecordSerializer SERIALIZER = new HistoryTimeSeriesRecordSerializer();

    @Getter
    private final int epoch;
    @Getter
    private final int referenceEpoch;
    private final ImmutableList<StreamSegmentRecord> segmentsSealed;
    private final ImmutableList<StreamSegmentRecord> segmentsCreated;
    @Getter
    private final long scaleTime;

    @Builder
    public HistoryTimeSeriesRecord(int epoch, int referenceEpoch, @NonNull ImmutableList<StreamSegmentRecord> segmentsSealed,
                                    @NonNull ImmutableList<StreamSegmentRecord> segmentsCreated, long creationTime) {
        if (epoch == referenceEpoch) {
            if (epoch != 0) {
                Exceptions.checkNotNullOrEmpty(segmentsSealed, "segments sealed");
            }

            Exceptions.checkNotNullOrEmpty(segmentsCreated, "segments created");
        } else {
            Exceptions.checkArgument(segmentsSealed == null || segmentsSealed.isEmpty(), "sealed segments", "should be null for duplicate epoch");
            Exceptions.checkArgument(segmentsCreated == null || segmentsCreated.isEmpty(), "created segments", "should be null for duplicate epoch");
        }
        this.epoch = epoch;
        this.referenceEpoch = referenceEpoch;
        this.segmentsSealed = segmentsSealed;
        this.segmentsCreated = segmentsCreated;
        this.scaleTime = creationTime;
    }

    HistoryTimeSeriesRecord(int epoch, int referenceEpoch, long creationTime) {
        this(epoch, referenceEpoch, ImmutableList.of(), ImmutableList.of(), creationTime);
    }

    public boolean isDuplicate() {
        return epoch != referenceEpoch;
    }

    @SneakyThrows(IOException.class)
    public byte[] toBytes() {
        return SERIALIZER.serialize(this).getCopy();
    }

    @Override
    public String toString() {
        return String.format("%s = %s", "epoch", epoch) + "\n" +
                String.format("%s = %s", "referenceEpoch", referenceEpoch) + "\n" +
                String.format("%s = [%n    %s%n]", "segmentsSealed", segmentsSealed.stream()
                        .map(streamSegmentRecord -> streamSegmentRecord.toString().replace("\n", "\n    "))
                        .collect(Collectors.joining("\n,\n    "))) + "\n" +
                String.format("%s = [%n    %s%n]", "segmentsCreated", segmentsCreated.stream()
                        .map(streamSegmentRecord -> streamSegmentRecord.toString().replace("\n", "\n    "))
                        .collect(Collectors.joining("\n,\n    "))) + "\n" +
                String.format("%s = %s", "scaleTime", scaleTime);
    }

    @SneakyThrows(IOException.class)
    public static HistoryTimeSeriesRecord fromBytes(final byte[] record) {
        InputStream inputStream = new ByteArrayInputStream(record, 0, record.length);
        return SERIALIZER.deserialize(inputStream);
    }

    private static class HistoryTimeSeriesRecordBuilder implements ObjectBuilder<HistoryTimeSeriesRecord> {

    }

    static class HistoryTimeSeriesRecordSerializer extends
            VersionedSerializer.WithBuilder<HistoryTimeSeriesRecord, HistoryTimeSeriesRecord.HistoryTimeSeriesRecordBuilder> {
        @Override
        protected byte getWriteVersion() {
            return 0;
        }

        @Override
        protected void declareVersions() {
            version(0).revision(0, this::write00, this::read00);
        }

        private void read00(RevisionDataInput revisionDataInput, HistoryTimeSeriesRecord.HistoryTimeSeriesRecordBuilder builder) throws IOException {
            builder.epoch(revisionDataInput.readInt())
                   .referenceEpoch(revisionDataInput.readInt());

            ImmutableList.Builder<StreamSegmentRecord> sealedSegmentsBuilders = ImmutableList.builder();
            revisionDataInput.readCollection(StreamSegmentRecord.SERIALIZER::deserialize, sealedSegmentsBuilders);
            builder.segmentsSealed(sealedSegmentsBuilders.build());

            ImmutableList.Builder<StreamSegmentRecord> segmentsCreatedBuilder = ImmutableList.builder();
            revisionDataInput.readCollection(StreamSegmentRecord.SERIALIZER::deserialize, segmentsCreatedBuilder);
            builder.segmentsCreated(segmentsCreatedBuilder.build());

            builder.creationTime(revisionDataInput.readLong());
        }

        private void write00(HistoryTimeSeriesRecord history, RevisionDataOutput revisionDataOutput) throws IOException {
            revisionDataOutput.writeInt(history.getEpoch());
            revisionDataOutput.writeInt(history.getReferenceEpoch());
            revisionDataOutput.writeCollection(history.getSegmentsSealed(), StreamSegmentRecord.SERIALIZER::serialize);
            revisionDataOutput.writeCollection(history.getSegmentsCreated(), StreamSegmentRecord.SERIALIZER::serialize);
            revisionDataOutput.writeLong(history.getScaleTime());
        }

        @Override
        protected HistoryTimeSeriesRecord.HistoryTimeSeriesRecordBuilder newBuilder() {
            return HistoryTimeSeriesRecord.builder();
        }
    }
}
