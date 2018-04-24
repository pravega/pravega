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
import io.pravega.controller.store.stream.tables.SegmentRecord;

import java.io.IOException;

public class SegmentRecordSerializer extends VersionedSerializer.WithBuilder<SegmentRecord, SegmentRecord.SegmentRecordBuilder> {
    @Override
    protected byte getWriteVersion() {
        return 0;
    }

    @Override
    protected void declareVersions() {
        version(0).revision(0, this::write00, this::read00);
    }

    private void read00(RevisionDataInput revisionDataInput, SegmentRecord.SegmentRecordBuilder builder) throws IOException {
        builder.segmentNumber(revisionDataInput.readInt())
                .startTime(revisionDataInput.readLong())
                .creationEpoch(revisionDataInput.readInt())
                .routingKeyStart(Double.longBitsToDouble(revisionDataInput.readLong()))
                .routingKeyEnd(Double.longBitsToDouble(revisionDataInput.readLong()));
    }

    private void write00(SegmentRecord segment, RevisionDataOutput revisionDataOutput) throws IOException {
        revisionDataOutput.writeInt(segment.getSegmentNumber());
        revisionDataOutput.writeLong(segment.getStartTime());
        revisionDataOutput.writeInt(segment.getCreationEpoch());
        revisionDataOutput.writeLong(Double.doubleToRawLongBits(segment.getRoutingKeyStart()));
        revisionDataOutput.writeLong(Double.doubleToRawLongBits(segment.getRoutingKeyEnd()));
    }

    @Override
    protected SegmentRecord.SegmentRecordBuilder newBuilder() {
        return SegmentRecord.builder();
    }
}
