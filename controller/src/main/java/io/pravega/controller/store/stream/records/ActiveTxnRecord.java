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
import io.pravega.controller.store.stream.records.serializers.ActiveTxnRecordSerializer;
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
public class ActiveTxnRecord {
    public static final VersionedSerializer.WithBuilder<ActiveTxnRecord, ActiveTxnRecord.ActiveTxnRecordBuilder> SERIALIZER
            = new ActiveTxnRecordSerializer();

    private final long txCreationTimestamp;
    private final long leaseExpiryTime;
    private final long maxExecutionExpiryTime;
    private final long scaleGracePeriod;
    private final TxnStatus txnStatus;

    public static class ActiveTxnRecordBuilder implements ObjectBuilder<ActiveTxnRecord> {

    }

    public static ActiveTxnRecord parse(byte[] data) {
        ActiveTxnRecord activeTxnRecord;
        try {
            activeTxnRecord = SERIALIZER.deserialize(data);
        } catch (IOException e) {
            log.error("deserialization error for active txn record {}", e);
            throw Lombok.sneakyThrow(e);
        }
        return activeTxnRecord;
    }

    public byte[] toByteArray() {
        byte[] array;
        try {
            array = SERIALIZER.serialize(this).array();
        } catch (IOException e) {
            log.error("error serializing active txn record {}", e);
            throw Lombok.sneakyThrow(e);
        }
        return array;
    }
}
