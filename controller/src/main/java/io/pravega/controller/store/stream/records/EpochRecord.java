/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.store.stream.records;

import com.google.common.collect.ImmutableList;
import io.pravega.common.ObjectBuilder;
import io.pravega.common.io.serialization.RevisionDataInput;
import io.pravega.common.io.serialization.RevisionDataOutput;
import io.pravega.common.io.serialization.VersionedSerializer;
import io.pravega.controller.store.stream.ScaleMetadata;
import io.pravega.controller.store.stream.Segment;
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
import java.util.List;
import java.util.ArrayList;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Serializable class that captures an epoch record.
 */
@Data
public class EpochRecord {
    public static final EpochRecordSerializer SERIALIZER = new EpochRecordSerializer();

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
    private ScaleMetadata scaleMetadata;
    
    @Builder
    public EpochRecord(int epoch, int referenceEpoch, @NonNull ImmutableList<StreamSegmentRecord> segments,
                       long creationTime, Optional<EpochRecord> previous) {
        this.epoch = epoch;
        this.referenceEpoch = referenceEpoch;
        this.segments = segments;
        this.creationTime = creationTime;
        this.segmentMap = segments.stream().collect(Collectors.toMap(StreamSegmentRecord::segmentId, x -> x));
        long splits = 0;
        long merges = 0;

        List<StreamSegmentRecord> segmentsList = convertToList(this.segments);
        if (previous.isPresent()) {
            List<StreamSegmentRecord> previousSegmentsList = convertToList(previous.get().segments);
            splits = findSegmentSplitsMerges(previousSegmentsList, segmentsList);
            merges = findSegmentSplitsMerges(segmentsList, previousSegmentsList);
            this.scaleMetadata = new ScaleMetadata(this.creationTime, transform(segmentsList), splits, merges);
        } else {
            // we're at the first epoch, so previousEpoch is empty
            this.scaleMetadata = new ScaleMetadata(this.creationTime, transform(segmentsList), 0L, 0L);
        }
    }

    private List<StreamSegmentRecord> convertToList(ImmutableList<StreamSegmentRecord> segmentList) {
        List<StreamSegmentRecord> segList = new ArrayList<>(segmentList.size());
        for (StreamSegmentRecord seg: segmentList) {
            segList.add(seg);
        }
        return segList;
    }

    private Segment transform(StreamSegmentRecord segmentRecord) {
        return new Segment(segmentRecord.segmentId(), segmentRecord.getCreationTime(),
                segmentRecord.getKeyStart(), segmentRecord.getKeyEnd());
    }

    private List<Segment> transform(List<StreamSegmentRecord> segmentRecords) {
        return segmentRecords.stream().map(this::transform).collect(Collectors.toList());
    }

    /**
     * Method to calculate number of splits and merges.
     *
     * Principle to calculate the number of splits and merges:
     * 1- An event has occurred if a reference range is present (overlaps) in at least two consecutive target ranges.
     * 2- If the direction of the check in 1 is forward, then it is a split, otherwise it is a merge.
     *
     * @param referenceSegmentsList Reference segment list.
     * @param targetSegmentsList Target segment list.
     * @return Number of splits/merges.
     */
    private long findSegmentSplitsMerges(List<StreamSegmentRecord> referenceSegmentsList, List<StreamSegmentRecord> targetSegmentsList) {
        return referenceSegmentsList.stream().filter(
                segment -> targetSegmentsList.stream().filter(target -> target.overlaps(segment)).count() > 1 ).count();
    }
    
    @SneakyThrows(IOException.class)
    public byte[] toBytes() {
        return SERIALIZER.serialize(this).getCopy();
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

    }
    
    private static class EpochRecordSerializer extends VersionedSerializer.WithBuilder<EpochRecord, EpochRecord.EpochRecordBuilder> {
        @Override
        protected byte getWriteVersion() {
            return 0;
        }

        @Override
        protected void declareVersions() {
            version(0).revision(0, this::write00, this::read00);
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

        @Override
        protected EpochRecord.EpochRecordBuilder newBuilder() {
            return EpochRecord.builder();
        }
    }
}
