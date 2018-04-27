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
import io.pravega.controller.store.stream.tables.serializers.CompletedTxnRecordSerializer;
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
public class CompletedTxnRecord {
    public static final CompletedTxnRecordSerializer SERIALIZER = new CompletedTxnRecordSerializer();

    private final long completeTime;
    private final TxnStatus completionStatus;

    public static class CompletedTxnRecordBuilder implements ObjectBuilder<CompletedTxnRecord> {

    }

    @SneakyThrows(IOException.class)
    public static CompletedTxnRecord parse(final byte[] data) {
        return SERIALIZER.deserialize(data);
    }

    @SneakyThrows(IOException.class)
    public byte[] toByteArray() {
        return SERIALIZER.serialize(this).getCopy();
    }
}
