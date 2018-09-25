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
import io.pravega.controller.store.stream.records.CommitTransactionsRecord;

import java.io.IOException;
import java.util.ArrayList;

public class CommitTransactionsRecordSerializer
        extends VersionedSerializer.WithBuilder<CommitTransactionsRecord, CommitTransactionsRecord.CommitTransactionsRecordBuilder> {
    @Override
    protected byte getWriteVersion() {
        return 0;
    }

    @Override
    protected void declareVersions() {
        version(0).revision(0, this::write00, this::read00);
    }

    private void read00(RevisionDataInput revisionDataInput, CommitTransactionsRecord.CommitTransactionsRecordBuilder builder)
            throws IOException {
        builder.epoch(revisionDataInput.readInt())
                .transactionsToCommit(revisionDataInput.readCollection(RevisionDataInput::readUUID, ArrayList::new))
                .activeEpoch(revisionDataInput.readInt());
    }

    private void write00(CommitTransactionsRecord record, RevisionDataOutput revisionDataOutput) throws IOException {
        revisionDataOutput.writeInt(record.getEpoch());
        revisionDataOutput.writeCollection(record.getTransactionsToCommit(), RevisionDataOutput::writeUUID);
        revisionDataOutput.writeInt(record.getActiveEpoch());
    }

    @Override
    protected CommitTransactionsRecord.CommitTransactionsRecordBuilder newBuilder() {
        return CommitTransactionsRecord.builder();
    }
}
