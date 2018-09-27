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
import lombok.Builder;
import lombok.Data;
import lombok.SneakyThrows;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Data
@Builder
/**
 * This class is the metadata to capture the currently processing transaction commit work. This captures the list of
 * transcations that current round of processing will attempt to commit. If the processing fails and retries, it will
 * find the list of transcations and reattempt to process them in exact same order.
 * This also includes optional "active epoch" field which is set if the commits have to be rolled over because they are
 * over an older epoch.
 */
public class CommitTransactionsRecord {
    public static final CommitTransactionsRecordSerializer SERIALIZER = new CommitTransactionsRecordSerializer();
    public static final CommitTransactionsRecord EMPTY = CommitTransactionsRecord.builder().epoch(Integer.MIN_VALUE)
            .transactionsToCommit(ImmutableList.of()).activeEpoch(Optional.empty()).build();
    /**
     * Epoch from which transactions are committed.
     */
    final int epoch;
    /**
     * Transactions to be be committed.
     */
    final List<UUID> transactionsToCommit;

    /**
     * Set only for rolling transactions and identify the active epoch that is being rolled over.
     */
    Optional<Integer> activeEpoch;

    CommitTransactionsRecord(int epoch, List<UUID> transactionsToCommit) {
        this(epoch, transactionsToCommit, Optional.empty());
    }

    CommitTransactionsRecord(int epoch, List<UUID> transactionsToCommit, int activeEpoch) {
        this(epoch, transactionsToCommit, Optional.of(activeEpoch));
    }

    private CommitTransactionsRecord(int epoch, List<UUID> transactionsToCommit, Optional<Integer> activeEpoch) {
        this.epoch = epoch;
        this.transactionsToCommit = transactionsToCommit;
        this.activeEpoch = activeEpoch;
    }

    public static class CommitTransactionsRecordBuilder implements ObjectBuilder<CommitTransactionsRecord> {
        private Optional<Integer> activeEpoch = Optional.empty();
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
        Preconditions.checkState(!this.activeEpoch.isPresent());
        return new CommitTransactionsRecord(this.epoch, this.transactionsToCommit, activeEpoch);
    }

    public boolean isRollingTxRecord() {
        return activeEpoch.isPresent();
    }
}