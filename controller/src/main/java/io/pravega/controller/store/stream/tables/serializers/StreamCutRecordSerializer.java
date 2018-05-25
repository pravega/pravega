/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.store.stream.tables.serializers;

import io.pravega.common.io.serialization.RevisionDataInput;
import io.pravega.common.io.serialization.RevisionDataOutput;
import io.pravega.common.io.serialization.VersionedSerializer;
import io.pravega.controller.store.stream.tables.StreamCutRecord;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class StreamCutRecordSerializer
        extends VersionedSerializer.WithBuilder<StreamCutRecord, StreamCutRecord.StreamCutRecordBuilder> {
    @Override
    protected byte getWriteVersion() {
        return 0;
    }

    @Override
    protected void declareVersions() {
        version(0).revision(0, this::write00, this::read00);
    }

    private void read00(RevisionDataInput revisionDataInput, StreamCutRecord.StreamCutRecordBuilder streamCutRecordBuilder)
            throws IOException {
        streamCutRecordBuilder.recordingTime(revisionDataInput.readLong())
                .recordingSize(revisionDataInput.readLong())
                .streamCut(revisionDataInput.readMap(DataInput::readInt, DataInput::readLong));
    }

    private void write00(StreamCutRecord streamCutRecord, RevisionDataOutput revisionDataOutput) throws IOException {
        revisionDataOutput.writeLong(streamCutRecord.getRecordingTime());
        revisionDataOutput.writeLong(streamCutRecord.getRecordingSize());
        revisionDataOutput.writeMap(streamCutRecord.getStreamCut(), DataOutput::writeInt, DataOutput::writeLong);
    }

    @Override
    protected StreamCutRecord.StreamCutRecordBuilder newBuilder() {
        return StreamCutRecord.builder();
    }
}
