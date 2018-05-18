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
import io.pravega.controller.store.stream.tables.SealedSegmentsRecord;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class SealedSegmentsRecordSerializer
        extends VersionedSerializer.WithBuilder<SealedSegmentsRecord, SealedSegmentsRecord.SealedSegmentsRecordBuilder> {
    @Override
    protected byte getWriteVersion() {
        return 0;
    }

    @Override
    protected void declareVersions() {
        version(0).revision(0, this::write00, this::read00);
    }

    private void read00(RevisionDataInput revisionDataInput,
                        SealedSegmentsRecord.SealedSegmentsRecordBuilder sealedSegmentsRecordBuilder) throws IOException {
        sealedSegmentsRecordBuilder.sealedSegmentsSizeMap(revisionDataInput.readMap(DataInput::readInt, DataInput::readLong));
    }

    private void write00(SealedSegmentsRecord sealedSegmentsRecord, RevisionDataOutput revisionDataOutput) throws IOException {
        revisionDataOutput.writeMap(sealedSegmentsRecord.getSealedSegmentsSizeMap(), DataOutput::writeInt, DataOutput::writeLong);
    }

    @Override
    protected SealedSegmentsRecord.SealedSegmentsRecordBuilder newBuilder() {
        return SealedSegmentsRecord.builder();
    }
}
