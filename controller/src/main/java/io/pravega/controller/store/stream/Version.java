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

import io.pravega.client.tables.impl.KeyVersionImpl;
import io.pravega.common.ObjectBuilder;
import io.pravega.common.io.serialization.RevisionDataInput;
import io.pravega.common.io.serialization.RevisionDataOutput;
import io.pravega.common.io.serialization.VersionedSerializer;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.SneakyThrows;
import java.io.IOException;

/**
 * Interface to capture version of a metadata object. 
 * This version is exposed to processors over the stream metadata store interface 
 * and they should use it if they want to perform compare and swap ability over metadata record updates. 
 */
public interface Version {
    IntVersion asIntVersion();
    
    LongVersion asLongVersion();

    byte[] toBytes();

    abstract class UnsupportedVersion implements Version {
        @Override
        public IntVersion asIntVersion() {
            throw new UnsupportedOperationException();
        }
        
        @Override
        public LongVersion asLongVersion() {
            throw new UnsupportedOperationException();
        }

        public byte[] toBytes() {
            throw new UnsupportedOperationException();
        }

    }
    
    @Data
    @Builder
    @EqualsAndHashCode(callSuper = false)
    /**
     * A version implementation that uses integer values. 
     */
    class IntVersion extends UnsupportedVersion {
        public static final IntVersion EMPTY = IntVersion.builder().intValue(Integer.MIN_VALUE).build();
        static final IntVersionSerializer SERIALIZER = new IntVersionSerializer();
        private final int intValue;
        
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
    
    @Data
    @Builder
    @EqualsAndHashCode(callSuper = false)
    /**
     * A version implementation that uses integer values. 
     */
    class LongVersion extends UnsupportedVersion {
        public static final LongVersion EMPTY = LongVersion.builder().longValue(KeyVersionImpl.NOT_EXISTS.getSegmentVersion()).build();
        static final LongVersionSerializer SERIALIZER = new LongVersionSerializer();
        private final long longValue;
        
        public static class LongVersionBuilder implements ObjectBuilder<LongVersion> {

        }

        @Override
        public LongVersion asLongVersion() {
            return this;
        }

        @Override
        @SneakyThrows(IOException.class)
        public byte[] toBytes() {
            return SERIALIZER.serialize(this).getCopy();
        }

        @SneakyThrows(IOException.class)
        public static LongVersion fromBytes(final byte[] data) {
            return SERIALIZER.deserialize(data);
        }
    }

    class LongVersionSerializer
            extends VersionedSerializer.WithBuilder<LongVersion, LongVersion.LongVersionBuilder> {
        @Override
        protected byte getWriteVersion() {
            return 0;
        }

        @Override
        protected void declareVersions() {
            version(0).revision(0, this::write00, this::read00);
        }

        private void read00(RevisionDataInput revisionDataInput, LongVersion.LongVersionBuilder builder)
                throws IOException {
            builder.longValue(revisionDataInput.readLong());
        }

        private void write00(LongVersion record, RevisionDataOutput revisionDataOutput) throws IOException {
            revisionDataOutput.writeLong(record.getLongValue());
        }

        @Override
        protected LongVersion.LongVersionBuilder newBuilder() {
            return LongVersion.builder();
        }
    }
}
