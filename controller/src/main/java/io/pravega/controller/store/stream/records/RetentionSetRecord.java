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

import io.pravega.common.ObjectBuilder;
import io.pravega.common.io.serialization.RevisionDataInput;
import io.pravega.common.io.serialization.RevisionDataOutput;
import io.pravega.common.io.serialization.VersionedSerializer;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;

/**
 * This is data class for storing stream cut with time when the cut was computed.
 * And the size of data being cut.
 */
@Data
@Builder
@AllArgsConstructor
@Slf4j
/**
 * Data Class representing individual retention set records where recording time and recording sizes are stored.
 */
public class RetentionSetRecord {
    public static final RetentionSetRecordSerializer SERIALIZER = new RetentionSetRecordSerializer();

    /**
     * Time when this stream cut was recorded.
     */
    final long recordingTime;
    /**
     * Amount of data in the stream preceding this cut.
     */
    final long recordingSize;

    public static class RetentionSetRecordBuilder implements ObjectBuilder<RetentionSetRecord> {

    }

    @SneakyThrows(IOException.class)
    public static RetentionSetRecord fromBytes(final byte[] data) {
        return SERIALIZER.deserialize(data);
    }

    @SneakyThrows(IOException.class)
    public byte[] toBytes() {
        return SERIALIZER.serialize(this).getCopy();
    }
    
    static class RetentionSetRecordSerializer
            extends VersionedSerializer.WithBuilder<RetentionSetRecord, RetentionSetRecord.RetentionSetRecordBuilder> {
        @Override
        protected byte getWriteVersion() {
            return 0;
        }

        @Override
        protected void declareVersions() {
            version(0).revision(0, this::write00, this::read00);
        }

        private void read00(RevisionDataInput revisionDataInput, RetentionSetRecord.RetentionSetRecordBuilder retentionRecordBuilder)
                throws IOException {
            retentionRecordBuilder.recordingSize(revisionDataInput.readLong());
            retentionRecordBuilder.recordingTime(revisionDataInput.readLong());
        }

        private void write00(RetentionSetRecord retentionRecord, RevisionDataOutput revisionDataOutput) throws IOException {
            revisionDataOutput.writeLong(retentionRecord.getRecordingSize());
            revisionDataOutput.writeLong(retentionRecord.getRecordingTime());
        }

        @Override
        protected RetentionSetRecord.RetentionSetRecordBuilder newBuilder() {
            return RetentionSetRecord.builder();
        }
    }
}
