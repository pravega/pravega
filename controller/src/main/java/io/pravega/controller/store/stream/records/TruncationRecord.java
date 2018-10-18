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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.pravega.common.ObjectBuilder;
import io.pravega.common.io.serialization.RevisionDataInput;
import io.pravega.common.io.serialization.RevisionDataOutput;
import io.pravega.common.io.serialization.VersionedSerializer;
import lombok.Builder;
import lombok.Data;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Data class for storing information about stream's truncation point.
 */
@Data
@Slf4j
public class TruncationRecord {
    public static final TruncationRecordSerializer SERIALIZER = new TruncationRecordSerializer();

    public static final TruncationRecord EMPTY = new TruncationRecord(ImmutableMap.of(),
            ImmutableMap.of(), ImmutableSet.of(), ImmutableSet.of(), false);

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

    private final boolean updating;

    @Builder
    public TruncationRecord(Map<Long, Long> streamCut, Map<StreamSegmentRecord, Integer> span,
                            Set<Long> deletedSegments, Set<Long> toDelete, boolean updating) {
        this.streamCut = ImmutableMap.copyOf(streamCut);
        this.span = ImmutableMap.copyOf(span);
        this.deletedSegments = ImmutableSet.copyOf(deletedSegments);
        this.toDelete = ImmutableSet.copyOf(toDelete);
        this.updating = updating;
    }

    int getTruncationEpochLow() {
        return span.values().stream().min(Comparator.naturalOrder()).orElse(Integer.MIN_VALUE);
    }

    int getTruncationEpochHigh() {
        return span.values().stream().max(Comparator.naturalOrder()).orElse(Integer.MIN_VALUE);
    }

    public ImmutableMap<Long, Long> getStreamCut() {
        return streamCut;
    }

    public ImmutableMap<StreamSegmentRecord, Integer> getSpan() {
        return span;
    }

    public ImmutableSet<Long> getDeletedSegments() {
        return deletedSegments;
    }

    public ImmutableSet<Long> getToDelete() {
        return toDelete;
    }

    public static TruncationRecord complete(TruncationRecord toComplete) {
        Preconditions.checkState(toComplete.updating);
        Set<Long> deleted = new HashSet<>(toComplete.deletedSegments);
        deleted.addAll(toComplete.toDelete);

        return TruncationRecord.builder()
                .updating(false)
                .span(toComplete.span)
                .streamCut(toComplete.streamCut)
                .deletedSegments(deleted)
                .toDelete(ImmutableSet.of())
                .build();
    }

    public static class TruncationRecordBuilder implements ObjectBuilder<TruncationRecord> {

    }

    @SneakyThrows(IOException.class)
    public static TruncationRecord fromBytes(final byte[] data) {
        return SERIALIZER.deserialize(data);
    }

    @SneakyThrows(IOException.class)
    public byte[] toBytes() {
        return SERIALIZER.serialize(this).getCopy();
    }
    
    private static class TruncationRecordSerializer
            extends VersionedSerializer.WithBuilder<TruncationRecord, TruncationRecord.TruncationRecordBuilder> {
        @Override
        protected byte getWriteVersion() {
            return 0;
        }

        @Override
        protected void declareVersions() {
            version(0).revision(0, this::write00, this::read00);
        }

        private void read00(RevisionDataInput revisionDataInput,
                            TruncationRecord.TruncationRecordBuilder streamTruncationRecordBuilder)
                throws IOException {
            streamTruncationRecordBuilder
                    .streamCut(revisionDataInput.readMap(DataInput::readLong, DataInput::readLong))
                    .span(revisionDataInput.readMap(StreamSegmentRecord.SERIALIZER::deserialize, DataInput::readInt))
                    .deletedSegments(ImmutableSet.copyOf(revisionDataInput.readCollection(DataInput::readLong)))
                    .toDelete(ImmutableSet.copyOf(revisionDataInput.readCollection(DataInput::readLong)))
                    .updating(revisionDataInput.readBoolean());
        }

        private void write00(TruncationRecord streamTruncationRecord, RevisionDataOutput revisionDataOutput)
                throws IOException {
            revisionDataOutput.writeMap(streamTruncationRecord.getStreamCut(), DataOutput::writeLong, DataOutput::writeLong);
            revisionDataOutput.writeMap(streamTruncationRecord.getSpan(), StreamSegmentRecord.SERIALIZER::serialize, DataOutput::writeInt);
            revisionDataOutput.writeCollection(streamTruncationRecord.getDeletedSegments(), DataOutput::writeLong);
            revisionDataOutput.writeCollection(streamTruncationRecord.getToDelete(), DataOutput::writeLong);
            revisionDataOutput.writeBoolean(streamTruncationRecord.isUpdating());
        }

        @Override
        protected TruncationRecord.TruncationRecordBuilder newBuilder() {
            return TruncationRecord.builder();
        }
    }
}
