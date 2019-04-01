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

import com.google.common.collect.ImmutableList;
import io.pravega.common.ObjectBuilder;
import io.pravega.common.io.serialization.RevisionDataInput;
import io.pravega.common.io.serialization.RevisionDataOutput;
import io.pravega.common.io.serialization.VersionedSerializer;
import lombok.Builder;
import lombok.Data;
import lombok.SneakyThrows;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
    private final List<StreamSegmentRecord> segments;
    private final long creationTime;
    private final Map<Long, StreamSegmentRecord> segmentMap = new HashMap<>();
    
    @Builder
    /**
     * This is a private constructor that is only directly used by the builder during the deserialization. 
     * The deserialization passes @param copyCollections as false so that we do not make an immutable copy of the collection
     * for the collection passed to the constructor via deserialization. 
     *
     * The all other constructors, the value of copyCollections flag is true and we make an immutable collection copy of 
     * the supplied collection. 
     * All getters of this class that return a collection always wrap them under Collections.unmodifiableCollection so that
     * no one can change the data object from outside.  
     */
    private EpochRecord(int epoch, int referenceEpoch, List<StreamSegmentRecord> segments, long creationTime, boolean copyCollections) {
        this.epoch = epoch;
        this.referenceEpoch = referenceEpoch;
        this.segments = copyCollections ? ImmutableList.copyOf(segments) : segments;
        this.creationTime = creationTime;
        this.segmentMap.putAll(segments.stream().collect(Collectors.toMap(StreamSegmentRecord::segmentId, x -> x)));
    }

    public EpochRecord(int epoch, int referenceEpoch, List<StreamSegmentRecord> segments, long creationTime) {
        this(epoch, referenceEpoch, segments, creationTime, true);
    }
    
    public List<StreamSegmentRecord> getSegments() {
        return Collections.unmodifiableList(segments);
    }

    private Map<Long, StreamSegmentRecord> getSegmentMap() {
        return Collections.unmodifiableMap(segmentMap);
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
                   .referenceEpoch(revisionDataInput.readInt())
                   .segments(revisionDataInput.readCollection(StreamSegmentRecord.SERIALIZER::deserialize, ArrayList::new))
                   .creationTime(revisionDataInput.readLong())
                   .copyCollections(false);
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
