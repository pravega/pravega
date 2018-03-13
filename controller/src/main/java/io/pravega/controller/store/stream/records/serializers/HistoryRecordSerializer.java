/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.store.stream.records.serializers;

import io.pravega.common.io.serialization.RevisionDataInput;
import io.pravega.common.io.serialization.RevisionDataOutput;
import io.pravega.common.io.serialization.VersionedSerializer;
import io.pravega.controller.store.stream.records.HistoryRecord;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

public class HistoryRecordSerializer extends VersionedSerializer.WithBuilder<HistoryRecord, HistoryRecord.HistoryRecordBuilder> {
    @Override
    protected byte writeVersion() {
        return 0;
    }

    @Override
    protected void declareVersions() {
        version(0).revision(0, this::write00, this::read00);
    }

    private void read00(RevisionDataInput revisionDataInput, HistoryRecord.HistoryRecordBuilder builder) throws IOException {
        builder.epoch(revisionDataInput.readInt())
                .segments(revisionDataInput.readCollection(DataInput::readInt, ArrayList::new))
                .scaleTime(revisionDataInput.readLong());
    }

    private void write00(HistoryRecord history, RevisionDataOutput revisionDataOutput) throws IOException {
        revisionDataOutput.writeInt(history.getEpoch());
        revisionDataOutput.writeCollection(history.getSegments(), DataOutput::writeInt);
        revisionDataOutput.writeLong(history.getScaleTime());
    }

    @Override
    protected HistoryRecord.HistoryRecordBuilder newBuilder() {
        return HistoryRecord.builder();
    }
}