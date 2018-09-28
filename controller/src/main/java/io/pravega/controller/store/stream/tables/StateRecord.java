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
import io.pravega.controller.store.stream.tables.serializers.StateRecordSerializer;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;

@Data
@Builder
@Slf4j
@AllArgsConstructor
public class StateRecord {
    public static final StateRecordSerializer SERIALIZER = new StateRecordSerializer();

    private final State state;

    public static StateRecord create(State state) {
        return StateRecord.builder().state(state).build();
    }

    public static class StateRecordBuilder implements ObjectBuilder<StateRecord> {

    }

    @SneakyThrows(IOException.class)
    public static StateRecord parse(final byte[] data) {
        return SERIALIZER.deserialize(data);
    }

    @SneakyThrows(IOException.class)
    public byte[] toByteArray() {
        return SERIALIZER.serialize(this).getCopy();
    }
}
