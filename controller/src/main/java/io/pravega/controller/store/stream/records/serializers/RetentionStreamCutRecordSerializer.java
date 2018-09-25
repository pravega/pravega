/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.store.stream.records.serializers;

import io.pravega.common.io.serialization.RevisionDataInput;
import io.pravega.common.io.serialization.RevisionDataOutput;
import io.pravega.common.io.serialization.VersionedSerializer;
import io.pravega.controller.store.stream.records.RetentionStreamCutRecord;
import io.pravega.controller.store.stream.records.StreamSegmentRecord;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class RetentionStreamCutRecordSerializer
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
