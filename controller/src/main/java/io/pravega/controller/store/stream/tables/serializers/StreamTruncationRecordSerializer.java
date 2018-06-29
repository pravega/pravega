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

import com.google.common.collect.ImmutableSet;
import io.pravega.common.io.serialization.RevisionDataInput;
import io.pravega.common.io.serialization.RevisionDataOutput;
import io.pravega.common.io.serialization.VersionedSerializer;
import io.pravega.controller.store.stream.tables.StreamTruncationRecord;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class StreamTruncationRecordSerializer
        extends VersionedSerializer.WithBuilder<StreamTruncationRecord, StreamTruncationRecord.StreamTruncationRecordBuilder> {
    @Override
    protected byte getWriteVersion() {
        return 0;
    }

    @Override
    protected void declareVersions() {
        version(0).revision(0, this::write00, this::read00);
    }

    private void read00(RevisionDataInput revisionDataInput,
                        StreamTruncationRecord.StreamTruncationRecordBuilder streamTruncationRecordBuilder)
            throws IOException {
        streamTruncationRecordBuilder
                .streamCut(revisionDataInput.readMap(DataInput::readLong, DataInput::readLong))
                .cutEpochMap(revisionDataInput.readMap(DataInput::readLong, DataInput::readInt))
                .deletedSegments(ImmutableSet.copyOf(revisionDataInput.readCollection(DataInput::readLong)))
                .toDelete(ImmutableSet.copyOf(revisionDataInput.readCollection(DataInput::readLong)))
                .updating(revisionDataInput.readBoolean());
    }

    private void write00(StreamTruncationRecord streamTruncationRecord, RevisionDataOutput revisionDataOutput)
            throws IOException {
        revisionDataOutput.writeMap(streamTruncationRecord.getStreamCut(), DataOutput::writeLong, DataOutput::writeLong);
        revisionDataOutput.writeMap(streamTruncationRecord.getCutEpochMap(), DataOutput::writeLong, DataOutput::writeInt);
        revisionDataOutput.writeCollection(streamTruncationRecord.getDeletedSegments(), DataOutput::writeLong);
        revisionDataOutput.writeCollection(streamTruncationRecord.getToDelete(), DataOutput::writeLong);
        revisionDataOutput.writeBoolean(streamTruncationRecord.isUpdating());
    }

    @Override
    protected StreamTruncationRecord.StreamTruncationRecordBuilder newBuilder() {
        return StreamTruncationRecord.builder();
    }
}
