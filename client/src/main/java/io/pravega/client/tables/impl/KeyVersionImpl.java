/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.tables.impl;

import io.pravega.common.ObjectBuilder;
import io.pravega.common.io.serialization.RevisionDataInput;
import io.pravega.common.io.serialization.RevisionDataOutput;
import io.pravega.common.io.serialization.VersionedSerializer;
import io.pravega.common.util.ByteArraySegment;
import java.io.IOException;
import java.io.ObjectStreamException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import lombok.Builder;
import lombok.Data;
import lombok.SneakyThrows;

@Data
public class KeyVersionImpl implements KeyVersion {

    private static final KeyVersionImplSerializer SERIALIZER = new KeyVersionImpl.KeyVersionImplSerializer();

    private final long segmentVersion;

    @Builder(builderClassName = "KeyVersionBuilder")
    public KeyVersionImpl(long segmentVersion) {
        this.segmentVersion = segmentVersion;
    }

    @Override
    @SneakyThrows(IOException.class)
    public ByteBuffer toBytes() {
        ByteArraySegment serialized = SERIALIZER.serialize(this);
        return ByteBuffer.wrap(serialized.array(), serialized.arrayOffset(), serialized.getLength());
    }

    @SneakyThrows(IOException.class)
    public static KeyVersionImpl fromBytes(ByteBuffer buff) {
        return SERIALIZER.deserialize(new ByteArraySegment(buff));
    }


    private static class KeyVersionBuilder implements ObjectBuilder<KeyVersionImpl> {
    }

    public static class KeyVersionImplSerializer extends VersionedSerializer.WithBuilder<KeyVersionImpl, KeyVersionBuilder> {
        @Override
        protected KeyVersionBuilder newBuilder() {
            return builder();
        }

        @Override
        protected byte getWriteVersion() {
            return 0;
        }

        @Override
        protected void declareVersions() {
            version(0).revision(0, this::write00, this::read00);
        }

        private void read00(RevisionDataInput revisionDataInput, KeyVersionBuilder builder) throws IOException {
            builder.segmentVersion(revisionDataInput.readCompactLong());
        }

        private void write00(KeyVersionImpl version, RevisionDataOutput revisionDataOutput) throws IOException {
            revisionDataOutput.writeCompactLong(version.getSegmentVersion());
        }
    }

    @SneakyThrows(IOException.class)
    private Object writeReplace() throws ObjectStreamException {
        return new KeyVersionImpl.SerializedForm(SERIALIZER.serialize(this).getCopy());
    }

    @Data
    private static class SerializedForm implements Serializable {
        private static final long serialVersionUID = 1L;
        private final byte[] value;
        @SneakyThrows(IOException.class)
        Object readResolve() throws ObjectStreamException {
            return SERIALIZER.deserialize(new ByteArraySegment(value));
        }
    }
}

