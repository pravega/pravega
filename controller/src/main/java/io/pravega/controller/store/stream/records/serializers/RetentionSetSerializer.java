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
import io.pravega.controller.store.stream.records.RetentionSet;
import io.pravega.controller.store.stream.records.RetentionSetRecord;

import java.io.IOException;
import java.util.ArrayList;

public class RetentionSetSerializer
        extends VersionedSerializer.WithBuilder<RetentionSet, RetentionSet.RetentionSetBuilder> {
    @Override
    protected byte getWriteVersion() {
        return 0;
    }

    @Override
    protected void declareVersions() {
        version(0).revision(0, this::write00, this::read00);
    }

    private void read00(RevisionDataInput revisionDataInput, RetentionSet.RetentionSetBuilder retentionRecordBuilder)
            throws IOException {
        retentionRecordBuilder.retentionRecords(revisionDataInput.readCollection(RetentionSetRecord.SERIALIZER::deserialize,
                ArrayList::new));
    }

    private void write00(RetentionSet retentionRecord, RevisionDataOutput revisionDataOutput) throws IOException {
        revisionDataOutput.writeCollection(retentionRecord.getRetentionRecords(), RetentionSetRecord.SERIALIZER::serialize);
    }

    @Override
    protected RetentionSet.RetentionSetBuilder newBuilder() {
        return RetentionSet.builder();
    }
}
