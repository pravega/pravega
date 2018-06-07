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
public class CommittingTransactionsRecord {
    public static final CommittingTransactionsRecordSerializer SERIALIZER = new CommittingTransactionsRecordSerializer();

    /**
     * Epoch from which transactions are committed.
     */
    final int epoch;
    /**
     * Transactions to be be committed.
     */
    final List<UUID> transactionsToCommit;

    public static class CommittingTransactionsRecordBuilder implements ObjectBuilder<CommittingTransactionsRecord> {

    }

    @SneakyThrows(IOException.class)
    public static CommittingTransactionsRecord parse(final byte[] data) {
        return SERIALIZER.deserialize(data);
    }

    @SneakyThrows(IOException.class)
    public byte[] toByteArray() {
        return SERIALIZER.serialize(this).getCopy();
    }
}