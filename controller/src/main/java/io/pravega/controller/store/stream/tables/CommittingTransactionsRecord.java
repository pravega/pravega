/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.store.stream.tables;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import io.pravega.common.ObjectBuilder;
import io.pravega.controller.store.stream.tables.serializers.CommittingTransactionsRecordSerializer;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.SneakyThrows;

import java.io.IOException;
import java.util.List;
import java.util.UUID;

@Data
@Builder
@AllArgsConstructor
/**
 * This class is the metadata to capture the currently processing transaction commit work. This captures the list of
 * transcations that current round of processing will attempt to commit. If the processing fails and retries, it will
 * find the list of transcations and reattempt to process them in exact same order.
 */
public class CommittingTransactionsRecord {
    public static final CommittingTransactionsRecordSerializer SERIALIZER = new CommittingTransactionsRecordSerializer();

    public static final CommittingTransactionsRecord EMPTY = CommittingTransactionsRecord.builder().epoch(Integer.MIN_VALUE)
            .transactionsToCommit(ImmutableList.of()).activeEpoch(Integer.MIN_VALUE).build();

    /**
     * Epoch from which transactions are committed.
     */
    final int epoch;
    /**
     * Transactions to be be committed.
     */
    final List<UUID> transactionsToCommit;

    /**
     * Active Epoch during start of rolling transaction.
     */
    int activeEpoch;

    public CommittingTransactionsRecord(int epoch, List<UUID> transactionsToCommit) {
        this(epoch, transactionsToCommit, Integer.MIN_VALUE);
    }

    public boolean isRollingTransactions() {
        return activeEpoch != Integer.MIN_VALUE;
    }

    public static class CommittingTransactionsRecordBuilder implements ObjectBuilder<CommittingTransactionsRecord> {
        private int activeEpoch = Integer.MIN_VALUE;
    }

    @SneakyThrows(IOException.class)
    public static CommittingTransactionsRecord parse(final byte[] data) {
        return SERIALIZER.deserialize(data);
    }

    @SneakyThrows(IOException.class)
    public byte[] toByteArray() {
        return SERIALIZER.serialize(this).getCopy();
    }

    public CommittingTransactionsRecord getRollingTxnRecord(int activeEpoch) {
        Preconditions.checkState(this.activeEpoch == Integer.MIN_VALUE);
        return new CommittingTransactionsRecord(this.epoch, this.transactionsToCommit, activeEpoch);
    }
}