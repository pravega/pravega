/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.store.stream;

import io.pravega.common.ObjectBuilder;
import io.pravega.common.io.serialization.RevisionDataInput;
import io.pravega.common.io.serialization.RevisionDataOutput;
import io.pravega.common.io.serialization.VersionedSerializer;
import lombok.Builder;
import lombok.Data;
import lombok.SneakyThrows;

import java.io.IOException;

/**
 * Interface to capture version of a metadata object. 
 * This version is exposed to processors over the stream metadata store interface 
 * and they should use it if they want to perform compare and swap ability over metadata record updates. 
 */
public interface Version {
    IntVersion asIntVersion();

    byte[] toBytes();

    @Data
    @Builder
    /**
     * A version implementation that uses integer values. 
     */
    class IntVersion implements Version {
        public static final IntVersion EMPTY = IntVersion.builder().intValue(Integer.MIN_VALUE).build();
        static final IntVersionSerializer SERIALIZER = new IntVersionSerializer();
        private final Integer intValue;

        public static class IntVersionBuilder implements ObjectBuilder<IntVersion> {

        }

        @Override
        public IntVersion asIntVersion() {
            return this;
        }

        @Override
        @SneakyThrows(IOException.class)
        public byte[] toBytes() {
            return SERIALIZER.serialize(this).getCopy();
        }

        @SneakyThrows(IOException.class)
        public static IntVersion fromBytes(final byte[] data) {
            return SERIALIZER.deserialize(data);
        }
    }

    class IntVersionSerializer
            extends VersionedSerializer.WithBuilder<IntVersion, IntVersion.IntVersionBuilder> {
        @Override
        protected byte getWriteVersion() {
            return 0;
        }

        @Override
        protected void declareVersions() {
            version(0).revision(0, this::write00, this::read00);
        }

        private void read00(RevisionDataInput revisionDataInput, IntVersion.IntVersionBuilder builder)
                throws IOException {
            builder.intValue(revisionDataInput.readInt());
        }

        private void write00(IntVersion record, RevisionDataOutput revisionDataOutput) throws IOException {
            revisionDataOutput.writeInt(record.getIntValue());
        }

        @Override
        protected IntVersion.IntVersionBuilder newBuilder() {
            return IntVersion.builder();
        }
    }
}
