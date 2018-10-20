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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import io.pravega.common.ObjectBuilder;
import io.pravega.common.io.serialization.RevisionDataInput;
import io.pravega.common.io.serialization.RevisionDataOutput;
import io.pravega.common.io.serialization.VersionedSerializer;
import io.pravega.common.util.CollectionHelpers;
import lombok.Builder;
import lombok.Data;
import lombok.Getter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

@Slf4j
@Builder
@Data
/**
 * Data class to capture a retention set. This contains a sorted (by recording time) list of retention set records.
 */
public class RetentionSet {
    public static final RetentionSetSerializer SERIALIZER = new RetentionSetSerializer();

    @Getter
    private final List<StreamCutReferenceRecord> retentionRecords;

    RetentionSet(List<StreamCutReferenceRecord> streamCutReferenceRecords) {
        this.retentionRecords = ImmutableList.copyOf(streamCutReferenceRecords);
    }

    /**
     * This method adds a reference to retentionStreamCutRecord in the retentionSet.  
     * @param retentionSet record
     * @param cut stream cut for which the reference has to be added. 
     * @return updated retentionSet
     */
    public static RetentionSet addReferenceToStreamCutIfLatest(RetentionSet retentionSet, RetentionStreamCutRecord cut) {
        // add only if cut.recordingTime is newer than any previous cut
        List<StreamCutReferenceRecord> retentionRecords = retentionSet.retentionRecords;
        if (retentionRecords.isEmpty() || retentionRecords.get(retentionRecords.size() - 1).getRecordingTime() < cut.getRecordingTime()) {
            List<StreamCutReferenceRecord> list = Lists.newArrayList(retentionRecords);

            list.add(new StreamCutReferenceRecord(cut.getRecordingTime(), cut.getRecordingSize()));
            return new RetentionSet(list);
        }
        return retentionSet;
    }

    /**
     * Find retention record on or before the given time.
     * @param time time
     * @return reference record which is greatest lower bound for given time. It returns null if no such record exists in the set.
     */
    public StreamCutReferenceRecord findStreamCutReferenceForTime(long time) {
        int beforeIndex = getGreatestLowerBound(this, time, StreamCutReferenceRecord::getRecordingTime);
        if (beforeIndex < 0) {
            return null;
        }

        return retentionRecords.get(beforeIndex);
    }

    /**
     * Find retention record on or before the given size.
     * @param size size
     * @return reference record which is greatest lower bound for given size. It returns null if no such record exists in the set.
     */
    public StreamCutReferenceRecord findStreamCutReferenceForSize(long size) {
        int beforeIndex = getGreatestLowerBound(this, size, StreamCutReferenceRecord::getRecordingSize);
        if (beforeIndex < 0) {
            return null;
        }

        return retentionRecords.get(beforeIndex);
    }

    /**
     * Get a list of all retention reference stream cut records on or before (inclusive) the given record.
     * @param record reference record
     * @return list of reference records before given reference record.
     */
    public List<StreamCutReferenceRecord> retentionRecordsBefore(StreamCutReferenceRecord record) {
        Preconditions.checkNotNull(record);
        int beforeIndex = getGreatestLowerBound(this, record.getRecordingTime(), StreamCutReferenceRecord::getRecordingTime);
        
        return retentionRecords.subList(0, beforeIndex + 1);
    }

    /**
     * Creates a new retention set object by removing all records on or before given record. 
     * @param set retention set to update
     * @param record reference record
     * @return updated retention set record after removing all elements before given reference record.
     */
    public static RetentionSet removeStreamCutBefore(RetentionSet set, StreamCutReferenceRecord record) {
        Preconditions.checkNotNull(record);
        // remove all stream cuts with recordingTime before supplied cut
        int beforeIndex = getGreatestLowerBound(set, record.getRecordingTime(), StreamCutReferenceRecord::getRecordingTime);
        if (beforeIndex < 0) {
            return set;            
        }
        
        if (beforeIndex + 1 == set.retentionRecords.size()) {
            return new RetentionSet(Collections.emptyList());
        }
        
        return new RetentionSet(set.retentionRecords.subList(beforeIndex + 1, set.retentionRecords.size()));
    }

    private static int getGreatestLowerBound(RetentionSet set, long value, Function<StreamCutReferenceRecord, Long> func) {
        return CollectionHelpers.findGreatestLowerBound(set.retentionRecords, x -> Long.compare(value, func.apply(x)));
    }

    public StreamCutReferenceRecord getLatest() {
        if (retentionRecords.isEmpty()) {
            return null;
        }
        return retentionRecords.get(retentionRecords.size() - 1);
    }

    public static class RetentionSetBuilder implements ObjectBuilder<RetentionSet> {
    }

    @SneakyThrows(IOException.class)
    public static RetentionSet fromBytes(final byte[] data) {
        return SERIALIZER.deserialize(data);
    }

    @SneakyThrows(IOException.class)
    public byte[] toBytes() {
        return SERIALIZER.serialize(this).getCopy();
    }
    
    private static class RetentionSetSerializer
            extends VersionedSerializer.WithBuilder<RetentionSet, RetentionSet.RetentionSetBuilder> {
        @Override
        protected byte getWriteVersion() {
            return 0;
        }

        @Override
        protected void declareVersions() {
            version(0).revision(0, this::write00, this::read00);
        }

        private void read00(RevisionDataInput revisionDataInput, RetentionSet.RetentionSetBuilder retentionRecordBuilder)
                throws IOException {
            retentionRecordBuilder.retentionRecords(revisionDataInput.readCollection(StreamCutReferenceRecord.SERIALIZER::deserialize,
                    ArrayList::new));
        }

        private void write00(RetentionSet retentionRecord, RevisionDataOutput revisionDataOutput) throws IOException {
            revisionDataOutput.writeCollection(retentionRecord.getRetentionRecords(), StreamCutReferenceRecord.SERIALIZER::serialize);
        }

        @Override
        protected RetentionSet.RetentionSetBuilder newBuilder() {
            return RetentionSet.builder();
        }
    }
}
