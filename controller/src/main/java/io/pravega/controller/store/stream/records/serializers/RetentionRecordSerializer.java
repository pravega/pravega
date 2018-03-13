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
import io.pravega.controller.store.stream.records.StreamCutRecord;
import io.pravega.controller.store.stream.records.RetentionRecord;

import java.io.IOException;
import java.util.ArrayList;

public class RetentionRecordSerializer
        extends VersionedSerializer.WithBuilder<RetentionRecord, RetentionRecord.RetentionRecordBuilder> {
    @Override
    protected byte writeVersion() {
        return 0;
    }

    @Override
    protected void declareVersions() {
        version(0).revision(0, this::write00, this::read00);
    }

    private void read00(RevisionDataInput revisionDataInput, RetentionRecord.RetentionRecordBuilder retentionRecordBuilder)
            throws IOException {
        retentionRecordBuilder.streamCuts(revisionDataInput.readCollection(StreamCutRecord.SERIALIZER::deserialize,
                ArrayList::new));
    }

    private void write00(RetentionRecord retentionRecord, RevisionDataOutput revisionDataOutput) throws IOException {
        revisionDataOutput.writeCollection(retentionRecord.getStreamCuts(), StreamCutRecord.SERIALIZER::serialize);
    }

    @Override
    protected RetentionRecord.RetentionRecordBuilder newBuilder() {
        return RetentionRecord.builder();
    }
}
