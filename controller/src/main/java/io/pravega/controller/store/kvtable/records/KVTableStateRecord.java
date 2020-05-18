/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.store.kvtable.records;

import io.pravega.common.ObjectBuilder;
import io.pravega.common.io.serialization.RevisionDataInput;
import io.pravega.common.io.serialization.RevisionDataOutput;
import io.pravega.common.io.serialization.VersionedSerializer;
import io.pravega.controller.store.kvtable.KVTableState;
import io.pravega.controller.store.stream.State;
import io.pravega.controller.store.stream.records.VersionMismatchException;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;

/**
 * A serializable class that encapsulates a Stream's State.
 */
@Data
@Builder
@Slf4j
@AllArgsConstructor
public class KVTableStateRecord {
    public static final KVTStateRecordSerializer SERIALIZER = new KVTStateRecordSerializer();

    private final KVTableState state;

    public static class KVTableStateRecordBuilder implements ObjectBuilder<KVTableStateRecord> {

    }

    @SneakyThrows(IOException.class)
    public static KVTableStateRecord fromBytes(final byte[] data) {
        return SERIALIZER.deserialize(data);
    }

    @SneakyThrows(IOException.class)
    public byte[] toBytes() {
        return SERIALIZER.serialize(this).getCopy();
    }

    private static class KVTStateRecordSerializer extends VersionedSerializer.WithBuilder<KVTableStateRecord, KVTableStateRecord.KVTableStateRecordBuilder> {
        @Override
        protected byte getWriteVersion() {
            return 0;
        }

        @Override
        protected void declareVersions() {
            version(0).revision(0, this::write00, this::read00);
        }

        private void read00(RevisionDataInput revisionDataInput, KVTableStateRecordBuilder builder) throws IOException {
            int ordinal = revisionDataInput.readCompactInt();
            if (ordinal < State.values().length) {
                builder.state(KVTableState.values()[ordinal]);
            } else {
                throw new VersionMismatchException(KVTableStateRecord.class.getName());
            }
        }

        private void write00(KVTableStateRecord state, RevisionDataOutput revisionDataOutput) throws IOException {
            revisionDataOutput.writeCompactInt(state.getState().ordinal());
        }

        @Override
        protected KVTableStateRecordBuilder newBuilder() {
            return KVTableStateRecord.builder();
        }
    }
}
