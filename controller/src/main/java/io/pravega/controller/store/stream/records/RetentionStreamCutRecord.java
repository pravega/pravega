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

import com.google.common.collect.ImmutableMap;
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
import java.util.Map;

/**
 * This is data class for storing stream cut with time when the cut was computed.
 * And the size of data being cut.
 * This record is indexed in retentionSet using the recording time and recording size.
 */
@Data
@Builder
@Slf4j
public class RetentionStreamCutRecord {
    public static final RetentionStreamCutRecordSerializer SERIALIZER = new RetentionStreamCutRecordSerializer();

    /**
     * Time when this stream cut was recorded.
     */
    final long recordingTime;
    /**
     * Amount of data in the stream preceeding this cut.
     */
    final long recordingSize;
    /**
     * Actual Stream cut.
     */
    final Map<StreamSegmentRecord, Long> streamCut;

    public RetentionStreamCutRecord(long recordingTime, long recordingSize, Map<StreamSegmentRecord, Long> streamCut) {
        this.recordingTime = recordingTime;
        this.recordingSize = recordingSize;
        this.streamCut = ImmutableMap.copyOf(streamCut);
    }

    public RetentionSetRecord getRetentionRecord() {
        return new RetentionSetRecord(recordingTime, recordingSize);
    }

    public static class RetentionStreamCutRecordBuilder implements ObjectBuilder<RetentionStreamCutRecord> {

    }

    @SneakyThrows(IOException.class)
    public static RetentionStreamCutRecord fromBytes(final byte[] data) {
        return SERIALIZER.deserialize(data);
    }

    @SneakyThrows(IOException.class)
    public byte[] toBytes() {
        return SERIALIZER.serialize(this).getCopy();
    }

    private static class RetentionStreamCutRecordSerializer
            extends VersionedSerializer.WithBuilder<RetentionStreamCutRecord, RetentionStreamCutRecord.RetentionStreamCutRecordBuilder> {
        @Override
        protected byte getWriteVersion() {
            return 0;
        }

        @Override
        protected void declareVersions() {
            version(0).revision(0, this::write00, this::read00);
        }

        private void read00(RevisionDataInput revisionDataInput, RetentionStreamCutRecord.RetentionStreamCutRecordBuilder streamCutRecordBuilder)
                throws IOException {
            streamCutRecordBuilder.recordingTime(revisionDataInput.readLong())
                                  .recordingSize(revisionDataInput.readLong())
                                  .streamCut(revisionDataInput.readMap(StreamSegmentRecord.SERIALIZER::deserialize, DataInput::readLong));
        }

        private void write00(RetentionStreamCutRecord streamCutRecord, RevisionDataOutput revisionDataOutput) throws IOException {
            revisionDataOutput.writeLong(streamCutRecord.getRecordingTime());
            revisionDataOutput.writeLong(streamCutRecord.getRecordingSize());
            revisionDataOutput.writeMap(streamCutRecord.getStreamCut(), StreamSegmentRecord.SERIALIZER::serialize, DataOutput::writeLong);
        }

        @Override
        protected RetentionStreamCutRecord.RetentionStreamCutRecordBuilder newBuilder() {
            return RetentionStreamCutRecord.builder();
        }
    }

}
