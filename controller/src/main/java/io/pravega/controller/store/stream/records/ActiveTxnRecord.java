/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
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
import java.util.Optional;

@Data
@Builder
@AllArgsConstructor
@Slf4j
public class ActiveTxnRecord {
    public static final ActiveTxnRecord EMPTY = ActiveTxnRecord.builder().txCreationTimestamp(Long.MIN_VALUE)
            .leaseExpiryTime(Long.MIN_VALUE).maxExecutionExpiryTime(Long.MIN_VALUE).txnStatus(TxnStatus.UNKNOWN)
            .writerId(Optional.empty()).commitTime(Optional.empty()).build();
    
    public static final ActiveTxnRecordSerializer SERIALIZER = new ActiveTxnRecordSerializer();

    private final long txCreationTimestamp;
    private final long leaseExpiryTime;
    private final long maxExecutionExpiryTime;
    private final TxnStatus txnStatus;
    private final Optional<String> writerId;
    private final Optional<Long> commitTime;
    private final Optional<Long> commitOrder;

    public ActiveTxnRecord(long txCreationTimestamp, long leaseExpiryTime, long maxExecutionExpiryTime, TxnStatus txnStatus) {
        this.txCreationTimestamp = txCreationTimestamp;
        this.leaseExpiryTime = leaseExpiryTime;
        this.maxExecutionExpiryTime = maxExecutionExpiryTime;
        this.txnStatus = txnStatus;
        this.writerId = Optional.empty();
        this.commitTime = Optional.empty();
        this.commitOrder = Optional.empty();
    }

    public ActiveTxnRecord(long txCreationTimestamp, long leaseExpiryTime, long maxExecutionExpiryTime, TxnStatus txnStatus, 
                           String writerId, long commitTime, long commitOrder) {
        this.txCreationTimestamp = txCreationTimestamp;
        this.leaseExpiryTime = leaseExpiryTime;
        this.maxExecutionExpiryTime = maxExecutionExpiryTime;
        this.txnStatus = txnStatus;
        this.writerId = Optional.ofNullable(writerId);
        this.commitTime = Optional.of(commitTime);
        this.commitOrder = Optional.of(commitOrder);
    }

    public String getWriterId() {
        return writerId.orElse("");
    }

    public long getCommitTime() {
        return commitTime.orElse(Long.MIN_VALUE);
    }
    
    public long getCommitOrder() {
        return commitOrder.orElse(Long.MIN_VALUE);
    }

    public static class ActiveTxnRecordBuilder implements ObjectBuilder<ActiveTxnRecord> {
        private Optional<String> writerId = Optional.empty();
        private Optional<Long> commitTime = Optional.empty();

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
                                  .txnStatus(TxnStatus.values()[revisionDataInput.readCompactInt()]);
        }

        private void write00(ActiveTxnRecord activeTxnRecord, RevisionDataOutput revisionDataOutput) throws IOException {
            revisionDataOutput.writeLong(activeTxnRecord.getTxCreationTimestamp());
            revisionDataOutput.writeLong(activeTxnRecord.getLeaseExpiryTime());
            revisionDataOutput.writeLong(activeTxnRecord.getMaxExecutionExpiryTime());
            revisionDataOutput.writeCompactInt(activeTxnRecord.getTxnStatus().ordinal());
        }

        private void read01(RevisionDataInput revisionDataInput, ActiveTxnRecord.ActiveTxnRecordBuilder activeTxnRecordBuilder)
                throws IOException {
            activeTxnRecordBuilder.writerId(Optional.of(revisionDataInput.readUTF()))
                                  .commitTime(Optional.of(revisionDataInput.readLong()))
                                  .commitOrder(Optional.of(revisionDataInput.readLong()));
        }

        private void write01(ActiveTxnRecord activeTxnRecord, RevisionDataOutput revisionDataOutput) throws IOException {
            revisionDataOutput.writeUTF(activeTxnRecord.getWriterId());
            revisionDataOutput.writeLong(activeTxnRecord.getCommitTime());
            revisionDataOutput.writeLong(activeTxnRecord.getCommitOrder());
        }

        @Override
        protected ActiveTxnRecord.ActiveTxnRecordBuilder newBuilder() {
            return ActiveTxnRecord.builder();
        }
    }
}
