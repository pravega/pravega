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
import io.pravega.controller.store.stream.records.EpochRecord;
import io.pravega.controller.store.stream.records.StreamSegmentRecord;

import java.io.IOException;
import java.util.ArrayList;

public class EpochRecordSerializer extends VersionedSerializer.WithBuilder<EpochRecord, EpochRecord.EpochRecordBuilder> {
    @Override
    protected byte getWriteVersion() {
        return 0;
    }

    @Override
    protected void declareVersions() {
        version(0).revision(0, this::write00, this::read00);
    }

    private void read00(RevisionDataInput revisionDataInput, EpochRecord.EpochRecordBuilder builder) throws IOException {
        builder.epoch(revisionDataInput.readInt())
                .referenceEpoch(revisionDataInput.readInt())
                .segments(revisionDataInput.readCollection(StreamSegmentRecord.SERIALIZER::deserialize, ArrayList::new))
                .creationTime(revisionDataInput.readLong());
    }

    private void write00(EpochRecord history, RevisionDataOutput revisionDataOutput) throws IOException {
        revisionDataOutput.writeInt(history.getEpoch());
        revisionDataOutput.writeInt(history.getReferenceEpoch());
        revisionDataOutput.writeCollection(history.getSegments(), StreamSegmentRecord.SERIALIZER::serialize);
        revisionDataOutput.writeLong(history.getCreationTime());
    }

    @Override
    protected EpochRecord.EpochRecordBuilder newBuilder() {
        return EpochRecord.builder();
    }
}