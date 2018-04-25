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
import io.pravega.controller.store.stream.TxnStatus;
import io.pravega.controller.store.stream.tables.CompletedTxnRecord;

import java.io.IOException;

public class CompletedTxnRecordSerializer
        extends VersionedSerializer.WithBuilder<CompletedTxnRecord, CompletedTxnRecord.CompletedTxnRecordBuilder> {
    @Override
    protected byte getWriteVersion() {
        return 0;
    }

    @Override
    protected void declareVersions() {
        version(0).revision(0, this::write00, this::read00);
    }

    private void read00(RevisionDataInput revisionDataInput,
                        CompletedTxnRecord.CompletedTxnRecordBuilder completedTxnRecordBuilder)
            throws IOException {
        completedTxnRecordBuilder.completeTime(revisionDataInput.readLong())
                .completionStatus(TxnStatus.values()[revisionDataInput.readInt()]);
    }

    private void write00(CompletedTxnRecord completedTxnRecord, RevisionDataOutput revisionDataOutput) throws IOException {
        revisionDataOutput.writeLong(completedTxnRecord.getCompleteTime());
        revisionDataOutput.writeInt(completedTxnRecord.getCompletionStatus().ordinal());
    }

    @Override
    protected CompletedTxnRecord.CompletedTxnRecordBuilder newBuilder() {
        return CompletedTxnRecord.builder();
    }
}
