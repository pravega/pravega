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
import io.pravega.common.io.serialization.VersionedSerializer;
import io.pravega.controller.store.stream.TxnStatus;
import io.pravega.controller.store.stream.records.serializers.CompletedTxnRecordSerializer;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.Lombok;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;

@Data
@Builder
@AllArgsConstructor
@Slf4j
public class CompletedTxnRecord {
    public static final VersionedSerializer.WithBuilder<CompletedTxnRecord, CompletedTxnRecord.CompletedTxnRecordBuilder> SERIALIZER
            = new CompletedTxnRecordSerializer();

    private final long completeTime;
    private final TxnStatus completionStatus;

    public static class CompletedTxnRecordBuilder implements ObjectBuilder<CompletedTxnRecord> {

    }

    public static CompletedTxnRecord parse(byte[] array) {
        CompletedTxnRecord completedRecord;
        try {
            completedRecord = SERIALIZER.deserialize(array);
        } catch (IOException e) {
            log.error("deserialization error for completed txn record {}", e);
            throw Lombok.sneakyThrow(e);
        }
        return completedRecord;
    }

    public byte[] toByteArray() {
        byte[] array;
        try {
            array = SERIALIZER.serialize(this).array();
        } catch (IOException e) {
            log.error("error serializing completed txn record {}", e);
            throw Lombok.sneakyThrow(e);
        }
        return array;
    }

}
