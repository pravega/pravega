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
import io.pravega.common.Exceptions;
import io.pravega.common.ObjectBuilder;
import io.pravega.common.io.serialization.RevisionDataInput;
import io.pravega.common.io.serialization.RevisionDataOutput;
import io.pravega.common.io.serialization.VersionedSerializer;
import lombok.Builder;
import lombok.Data;
import lombok.Getter;
import lombok.SneakyThrows;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

@Data
/**
 * Each HistoryTimeSeriesRecord captures delta between two consecutive epoch records.
 * To compute an epoch record from this time series, we need at least one complete epoch record and then we can
 * apply deltas on it iteratively until we reach the desired epoch record.
 */
public class HistoryTimeSeriesRecord {
    public static final HistoryTimeSeriesRecordSerializer SERIALIZER = new HistoryTimeSeriesRecordSerializer();

    @Getter
    private final int epoch;
    @Getter
    private final int referenceEpoch;
    @Getter
    private final List<StreamSegmentRecord> segmentsSealed;
    @Getter
    private final List<StreamSegmentRecord> segmentsCreated;
    @Getter
    private final long scaleTime;

    @Builder
    HistoryTimeSeriesRecord(int epoch, int referenceEpoch, List<StreamSegmentRecord> segmentsSealed, List<StreamSegmentRecord> segmentsCreated,
                            long creationTime) {
        if (epoch == referenceEpoch) {
            Exceptions.checkNotNullOrEmpty(segmentsSealed, "segments sealed");
            Exceptions.checkNotNullOrEmpty(segmentsCreated, "segments created");
        } else {
            Exceptions.checkArgument(segmentsSealed == null || segmentsSealed.isEmpty(), "sealed segments", "should be null for duplicate epoch");
            Exceptions.checkArgument(segmentsCreated == null || segmentsCreated.isEmpty(), "created segments", "should be null for duplicate epoch");
        }
        this.epoch = epoch;
        this.referenceEpoch = referenceEpoch;
        this.segmentsSealed = segmentsSealed == null ? null : ImmutableList.copyOf(segmentsSealed);
        this.segmentsCreated = segmentsCreated == null ? null : ImmutableList.copyOf(segmentsCreated);
        this.scaleTime = creationTime;
    }

    @Builder
    HistoryTimeSeriesRecord(int epoch, List<StreamSegmentRecord> segmentsSealed, List<StreamSegmentRecord> segmentsCreated, long creationTime) {
        this(epoch, epoch, segmentsSealed, segmentsCreated, creationTime);
    }

    public boolean isDuplicate() {
        return epoch != referenceEpoch;
    }

    @SneakyThrows(IOException.class)
    public byte[] toBytes() {
        return SERIALIZER.serialize(this).getCopy();
    }
    
    @SneakyThrows(IOException.class)
    public static HistoryTimeSeriesRecord fromBytes(final byte[] record) {
        InputStream inputStream = new ByteArrayInputStream(record, 0, record.length);
        return SERIALIZER.deserialize(inputStream);
    }

    public static class HistoryTimeSeriesRecordBuilder implements ObjectBuilder<HistoryTimeSeriesRecord> {

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
                   .referenceEpoch(revisionDataInput.readInt())
                   .segmentsSealed(revisionDataInput.readCollection(StreamSegmentRecord.SERIALIZER::deserialize, ArrayList::new))
                   .segmentsCreated(revisionDataInput.readCollection(StreamSegmentRecord.SERIALIZER::deserialize, ArrayList::new))
                   .creationTime(revisionDataInput.readLong());
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
