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
public class StreamTruncationRecord {
    public static final TruncationRecordSerializer SERIALIZER = new TruncationRecordSerializer();

    public static final StreamTruncationRecord EMPTY = new StreamTruncationRecord(ImmutableMap.of(),
            ImmutableMap.of(), ImmutableSet.of(), ImmutableSet.of(), 0L, false);

    /**
     * Stream cut that is applied as part of this truncation.
     */
    private final Map<Long, Long> streamCut;

    private final int spanEpochLow;
    private final int spanEpochHigh;
    /**
     * All segments that have been deleted for this stream so far.
     */
    private final Set<Long> deletedSegments;
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
    public StreamTruncationRecord(Map<Long, Long> streamCut, Map<StreamSegmentRecord, Integer> span,
                                  Set<Long> deletedSegments, Set<Long> toDelete, long sizeTill, boolean updating) {
        this.streamCut = ImmutableMap.copyOf(streamCut);
        this.deletedSegments = ImmutableSet.copyOf(deletedSegments);
        this.toDelete = ImmutableSet.copyOf(toDelete);
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
        Set<Long> deleted = new HashSet<>(toComplete.deletedSegments);
        deleted.addAll(toComplete.toDelete);

        return StreamTruncationRecord.builder()
                                     .updating(false)
                                     .streamCut(toComplete.streamCut)
                                     .deletedSegments(deleted)
                                     .toDelete(ImmutableSet.of())
                                     .sizeTill(toComplete.sizeTill)
                                     .build();
    }

    public static class StreamTruncationRecordBuilder implements ObjectBuilder<StreamTruncationRecord> {

    }

    @SneakyThrows(IOException.class)
    public static StreamTruncationRecord fromBytes(final byte[] data) {
        return SERIALIZER.deserialize(data);
    }

    @SneakyThrows(IOException.class)
    public byte[] toBytes() {
        return SERIALIZER.serialize(this).getCopy();
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
            truncationRecordBuilder
                    .streamCut(revisionDataInput.readMap(DataInput::readLong, DataInput::readLong))
                    .span(revisionDataInput.readMap(StreamSegmentRecord.SERIALIZER::deserialize, DataInput::readInt))
                    .deletedSegments(new HashSet<>(revisionDataInput.readCollection(DataInput::readLong)))
                    .toDelete(new HashSet<>(revisionDataInput.readCollection(DataInput::readLong)))
                    .sizeTill(revisionDataInput.readLong())
                    .updating(revisionDataInput.readBoolean());
        }

        private void write00(StreamTruncationRecord streamTruncationRecord, RevisionDataOutput revisionDataOutput)
                throws IOException {
            revisionDataOutput.writeMap(streamTruncationRecord.getStreamCut(), DataOutput::writeLong, DataOutput::writeLong);
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
