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
import io.pravega.controller.store.stream.records.StreamSegmentRecord;

import java.io.IOException;

public class StreamSegmentRecordSerializer extends VersionedSerializer.WithBuilder<StreamSegmentRecord, StreamSegmentRecord.StreamSegmentRecordBuilder> {
    @Override
    protected byte getWriteVersion() {
        return 0;
    }

    @Override
    protected void declareVersions() {
        version(0).revision(0, this::write00, this::read00);
    }

    private void read00(RevisionDataInput revisionDataInput, StreamSegmentRecord.StreamSegmentRecordBuilder builder) throws IOException {
        builder.segmentNumber(revisionDataInput.readInt())
                .creationTime(revisionDataInput.readLong())
                .creationEpoch(revisionDataInput.readInt())
                .keyStart(Double.longBitsToDouble(revisionDataInput.readLong()))
                .keyEnd(Double.longBitsToDouble(revisionDataInput.readLong()));
    }

    private void write00(StreamSegmentRecord segment, RevisionDataOutput revisionDataOutput) throws IOException {
        revisionDataOutput.writeInt(segment.getSegmentNumber());
        revisionDataOutput.writeLong(segment.getCreationTime());
        revisionDataOutput.writeInt(segment.getCreationEpoch());
        revisionDataOutput.writeLong(Double.doubleToRawLongBits(segment.getKeyStart()));
        revisionDataOutput.writeLong(Double.doubleToRawLongBits(segment.getKeyEnd()));
    }

    @Override
    protected StreamSegmentRecord.StreamSegmentRecordBuilder newBuilder() {
        return StreamSegmentRecord.builder();
    }
}
