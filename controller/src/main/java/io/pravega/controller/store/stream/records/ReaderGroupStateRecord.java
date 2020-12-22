/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.store.stream.records;

import io.pravega.common.ObjectBuilder;
import io.pravega.common.io.serialization.RevisionDataInput;
import io.pravega.common.io.serialization.RevisionDataOutput;
import io.pravega.common.io.serialization.VersionedSerializer;
import io.pravega.controller.store.stream.ReaderGroupState;
import io.pravega.controller.store.stream.State;
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
public class ReaderGroupStateRecord {
    public static final ReaderGroupStateRecordSerializer SERIALIZER = new ReaderGroupStateRecordSerializer();

    private final ReaderGroupState state;

    public static class ReaderGroupStateRecordBuilder implements ObjectBuilder<ReaderGroupStateRecord> {

    }

    @SneakyThrows(IOException.class)
    public static ReaderGroupStateRecord fromBytes(final byte[] data) {
        return SERIALIZER.deserialize(data);
    }

    @SneakyThrows(IOException.class)
    public byte[] toBytes() {
        return SERIALIZER.serialize(this).getCopy();
    }

    private static class ReaderGroupStateRecordSerializer extends VersionedSerializer.WithBuilder<ReaderGroupStateRecord, ReaderGroupStateRecord.ReaderGroupStateRecordBuilder> {
        @Override
        protected byte getWriteVersion() {
            return 0;
        }

        @Override
        protected void declareVersions() {
            version(0).revision(0, this::write00, this::read00);
        }

        private void read00(RevisionDataInput revisionDataInput, ReaderGroupStateRecord.ReaderGroupStateRecordBuilder builder) throws IOException {
            int ordinal = revisionDataInput.readCompactInt();
            if (ordinal < State.values().length) {
                builder.state(ReaderGroupState.values()[ordinal]);
            } else {
                throw new VersionMismatchException(ReaderGroupStateRecord.class.getName());
            }
        }

        private void write00(ReaderGroupStateRecord state, RevisionDataOutput revisionDataOutput) throws IOException {
            revisionDataOutput.writeCompactInt(state.getState().ordinal());
        }

        @Override
        protected ReaderGroupStateRecord.ReaderGroupStateRecordBuilder newBuilder() {
            return ReaderGroupStateRecord.builder();
        }
    }
}
