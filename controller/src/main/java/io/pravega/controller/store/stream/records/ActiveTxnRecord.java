/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.store.stream.records;

import io.pravega.common.ObjectBuilder;
import io.pravega.common.io.serialization.RevisionDataInput;
import io.pravega.common.io.serialization.RevisionDataOutput;
import io.pravega.common.io.serialization.VersionedSerializer;
import io.pravega.controller.store.stream.TxnStatus;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;

@Data
@Builder
@AllArgsConstructor
@Slf4j
public class ActiveTxnRecord {
    public static final ActiveTxnRecordSerializer SERIALIZER = new ActiveTxnRecordSerializer();
    
    private final long txCreationTimestamp;
    private final long leaseExpiryTime;
    private final long maxExecutionExpiryTime;
    private final TxnStatus txnStatus;
    private final String writerId;
    private final long commitTime;

    public ActiveTxnRecord(long txCreationTimestamp, long leaseExpiryTime, long maxExecutionExpiryTime, TxnStatus txnStatus) {
        this.txCreationTimestamp = txCreationTimestamp;
        this.leaseExpiryTime = leaseExpiryTime;
        this.maxExecutionExpiryTime = maxExecutionExpiryTime;
        this.txnStatus = txnStatus;
        this.writerId = "";
        this.commitTime = Long.MIN_VALUE;
    }

    public static class ActiveTxnRecordBuilder implements ObjectBuilder<ActiveTxnRecord> {

    }

    @SneakyThrows(IOException.class)
    public static ActiveTxnRecord fromBytes(final byte[] data) {
        return SERIALIZER.deserialize(data);
    }

    @SneakyThrows(IOException.class)
    public byte[] toBytes() {
        return SERIALIZER.serialize(this).getCopy();
    }

    private static class ActiveTxnRecordSerializer
            extends VersionedSerializer.WithBuilder<ActiveTxnRecord, ActiveTxnRecord.ActiveTxnRecordBuilder> {
        @Override
        protected byte getWriteVersion() {
            return 0;
        }

        @Override
        protected void declareVersions() {
            version(0).revision(0, this::write00, this::read00).revision(1, this::write01, this::read01);
        }

        private void read00(RevisionDataInput revisionDataInput, ActiveTxnRecord.ActiveTxnRecordBuilder activeTxnRecordBuilder)
                throws IOException {
            activeTxnRecordBuilder.txCreationTimestamp(revisionDataInput.readLong())
                                  .leaseExpiryTime(revisionDataInput.readLong())
                                  .maxExecutionExpiryTime(revisionDataInput.readLong())
                                  .txnStatus(TxnStatus.values()[revisionDataInput.readCompactInt()])
                                  .writerId(null)
                                  .commitTime(Long.MIN_VALUE);
        }

        private void write00(ActiveTxnRecord activeTxnRecord, RevisionDataOutput revisionDataOutput) throws IOException {
            revisionDataOutput.writeLong(activeTxnRecord.getTxCreationTimestamp());
            revisionDataOutput.writeLong(activeTxnRecord.getLeaseExpiryTime());
            revisionDataOutput.writeLong(activeTxnRecord.getMaxExecutionExpiryTime());
            revisionDataOutput.writeCompactInt(activeTxnRecord.getTxnStatus().ordinal());
        }

        private void read01(RevisionDataInput revisionDataInput, ActiveTxnRecord.ActiveTxnRecordBuilder activeTxnRecordBuilder)
                throws IOException {
            activeTxnRecordBuilder.txCreationTimestamp(revisionDataInput.readLong())
                                  .leaseExpiryTime(revisionDataInput.readLong())
                                  .maxExecutionExpiryTime(revisionDataInput.readLong())
                                  .txnStatus(TxnStatus.values()[revisionDataInput.readCompactInt()])
                                  .writerId(revisionDataInput.readUTF())
                                  .commitTime(revisionDataInput.readCompactLong());
        }

        private void write01(ActiveTxnRecord activeTxnRecord, RevisionDataOutput revisionDataOutput) throws IOException {
            revisionDataOutput.writeLong(activeTxnRecord.getTxCreationTimestamp());
            revisionDataOutput.writeLong(activeTxnRecord.getLeaseExpiryTime());
            revisionDataOutput.writeLong(activeTxnRecord.getMaxExecutionExpiryTime());
            revisionDataOutput.writeCompactInt(activeTxnRecord.getTxnStatus().ordinal());
            revisionDataOutput.writeUTF(activeTxnRecord.writerId);
            revisionDataOutput.writeCompactLong(activeTxnRecord.commitTime);
        }

        @Override
        protected ActiveTxnRecord.ActiveTxnRecordBuilder newBuilder() {
            return ActiveTxnRecord.builder();
        }
    }
}
