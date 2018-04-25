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
import io.pravega.controller.store.stream.tables.ActiveTxnRecord;

import java.io.IOException;

public class ActiveTxnRecordSerializer
        extends VersionedSerializer.WithBuilder<ActiveTxnRecord, ActiveTxnRecord.ActiveTxnRecordBuilder> {
    @Override
    protected byte getWriteVersion() {
        return 0;
    }

    @Override
    protected void declareVersions() {
        version(0).revision(0, this::write00, this::read00);
    }

    private void read00(RevisionDataInput revisionDataInput, ActiveTxnRecord.ActiveTxnRecordBuilder activeTxnRecordBuilder)
            throws IOException {
        activeTxnRecordBuilder.txCreationTimestamp(revisionDataInput.readLong())
                .leaseExpiryTime(revisionDataInput.readLong())
                .maxExecutionExpiryTime(revisionDataInput.readLong())
                .scaleGracePeriod(revisionDataInput.readLong())
                .txnStatus(TxnStatus.values()[revisionDataInput.readInt()]);
    }

    private void write00(ActiveTxnRecord activeTxnRecord, RevisionDataOutput revisionDataOutput) throws IOException {
        revisionDataOutput.writeLong(activeTxnRecord.getTxCreationTimestamp());
        revisionDataOutput.writeLong(activeTxnRecord.getLeaseExpiryTime());
        revisionDataOutput.writeLong(activeTxnRecord.getMaxExecutionExpiryTime());
        revisionDataOutput.writeLong(activeTxnRecord.getScaleGracePeriod());
        revisionDataOutput.writeInt(activeTxnRecord.getTxnStatus().ordinal());
    }

    @Override
    protected ActiveTxnRecord.ActiveTxnRecordBuilder newBuilder() {
        return ActiveTxnRecord.builder();
    }
}
