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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import io.pravega.common.ObjectBuilder;
import io.pravega.controller.store.stream.records.serializers.CommitTransactionsRecordSerializer;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.SneakyThrows;

import java.io.IOException;
import java.util.List;
import java.util.UUID;

@Data
@Builder
/**
 * This class is the metadata to capture the currently processing transaction commit work. This captures the list of
 * transcations that current round of processing will attempt to commit. If the processing fails and retries, it will
 * find the list of transcations and reattempt to process them in exact same order.
 */
public class CommitTransactionsRecord {
    public static final CommitTransactionsRecordSerializer SERIALIZER = new CommitTransactionsRecordSerializer();
    public static final CommitTransactionsRecord EMPTY = CommitTransactionsRecord.builder().epoch(Integer.MIN_VALUE)
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
     * Epoch from which transactions are committed.
     */
    int activeEpoch;

    CommitTransactionsRecord(int epoch, List<UUID> transactionsToCommit) {
        this(epoch, transactionsToCommit, Integer.MIN_VALUE);
    }

    CommitTransactionsRecord(int epoch, List<UUID> transactionsToCommit, int activeEpoch) {
        this.epoch = epoch;
        this.transactionsToCommit = transactionsToCommit;
        this.activeEpoch = activeEpoch;
    }

    public static class CommitTransactionsRecordBuilder implements ObjectBuilder<CommitTransactionsRecord> {
        private int activeEpoch = Integer.MIN_VALUE;
    }

    @SneakyThrows(IOException.class)
    public static CommitTransactionsRecord parse(final byte[] data) {
        return SERIALIZER.deserialize(data);
    }

    @SneakyThrows(IOException.class)
    public byte[] toByteArray() {
        return SERIALIZER.serialize(this).getCopy();
    }

    public CommitTransactionsRecord getRollingTxnRecord(int activeEpoch) {
        Preconditions.checkState(this.activeEpoch == Integer.MIN_VALUE);
        return new CommitTransactionsRecord(this.epoch, this.transactionsToCommit, activeEpoch);
    }
}