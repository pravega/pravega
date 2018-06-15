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
import io.pravega.controller.store.stream.TxnStatus;
import io.pravega.controller.store.stream.tables.serializers.ActiveTxnRecordSerializer;
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

    public static class ActiveTxnRecordBuilder implements ObjectBuilder<ActiveTxnRecord> {

    }

    @SneakyThrows(IOException.class)
    public static ActiveTxnRecord parse(final byte[] data) {
        return SERIALIZER.deserialize(data);
    }

    @SneakyThrows(IOException.class)
    public byte[] toByteArray() {
        return SERIALIZER.serialize(this).getCopy();
    }
}
