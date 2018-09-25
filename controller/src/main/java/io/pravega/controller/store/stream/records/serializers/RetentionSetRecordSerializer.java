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
import io.pravega.controller.store.stream.records.RetentionSetRecord;

import java.io.IOException;

public class RetentionSetRecordSerializer
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
