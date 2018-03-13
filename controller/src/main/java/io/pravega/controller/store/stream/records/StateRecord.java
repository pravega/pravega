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
import io.pravega.controller.store.stream.State;
import io.pravega.controller.store.stream.records.serializers.StateRecordSerializer;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.Lombok;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;

@Data
@Builder
@Slf4j
@AllArgsConstructor
public class StateRecord {
    public static final VersionedSerializer.WithBuilder<StateRecord, StateRecord.StateRecordBuilder> SERIALIZER
            = new StateRecordSerializer();

    private final State state;

    public static class StateRecordBuilder implements ObjectBuilder<StateRecord> {

    }

    public static StateRecord parse(byte[] data) {
        StateRecord stateRecord;
        try {
            stateRecord = SERIALIZER.deserialize(data);
        } catch (IOException e) {
            log.error("deserialization error for state record {}", e);
            throw Lombok.sneakyThrow(e);
        }
        return stateRecord;
    }

    public byte[] toByteArray() {
        byte[] array;
        try {
            array = SERIALIZER.serialize(this).array();
        } catch (IOException e) {
            log.error("error serializing state record {}", e);
            throw Lombok.sneakyThrow(e);
        }
        return array;
    }

}
