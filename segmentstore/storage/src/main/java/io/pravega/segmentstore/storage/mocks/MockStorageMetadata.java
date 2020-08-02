/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.storage.mocks;

import io.pravega.common.ObjectBuilder;
import io.pravega.common.io.serialization.RevisionDataInput;
import io.pravega.common.io.serialization.RevisionDataOutput;
import io.pravega.common.io.serialization.VersionedSerializer;
import io.pravega.segmentstore.storage.metadata.StorageMetadata;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.io.IOException;

/**
 * Mock Test data.
 */
@Builder(toBuilder = true)
@Data
@EqualsAndHashCode(callSuper = true)
public class MockStorageMetadata extends StorageMetadata {

    final private String key;

    final private String value;

    /**
     * Constructor.
     *
     * @param key   Key.
     * @param value Value.
     */
    public MockStorageMetadata(String key, String value) {
        this.key = key;
        this.value = value;
    }

    @Override
    public StorageMetadata deepCopy() {
        return toBuilder().build();
    }

    /**
     * Builder that implements {@link ObjectBuilder}.
     */
    public static class MockStorageMetadataBuilder implements ObjectBuilder<MockStorageMetadata> {
    }

    /**
     * Serializer that implements {@link VersionedSerializer}.
     */
    public static class Serializer extends VersionedSerializer.WithBuilder<MockStorageMetadata, MockStorageMetadataBuilder> {
        @Override
        protected MockStorageMetadataBuilder newBuilder() {
            return MockStorageMetadata.builder();
        }

        @Override
        protected byte getWriteVersion() {
            return 0;
        }

        @Override
        protected void declareVersions() {
            version(0).revision(0, this::write00, this::read00);
        }

        private void write00(MockStorageMetadata object, RevisionDataOutput target) throws IOException {
            target.writeUTF(object.key);
            target.writeUTF(object.value);
        }

        private void read00(RevisionDataInput source, MockStorageMetadataBuilder b) throws IOException {
            b.key(source.readUTF());
            b.value(source.readUTF());
        }
    }
}
