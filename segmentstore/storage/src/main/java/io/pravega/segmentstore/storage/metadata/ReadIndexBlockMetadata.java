/**
 * Copyright Pravega Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.pravega.segmentstore.storage.metadata;

import io.pravega.common.ObjectBuilder;
import io.pravega.common.io.serialization.RevisionDataInput;
import io.pravega.common.io.serialization.RevisionDataOutput;
import io.pravega.common.io.serialization.VersionedSerializer;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.io.IOException;

/**
 * Represents read index block .
 * Following metadata is stored:
 * <ul>
 * <li>Name of entry.</li>     
 * <li>Name of the chunk containing first byte of this block.</li>
 * <li>Start offset of the chunk.</li>
 * </ul>
 */
@Builder(toBuilder = true)
@Data
@EqualsAndHashCode(callSuper = true)
public class ReadIndexBlockMetadata extends StorageMetadata {
    /**
     * Name of this index node.
     */
    private final String name;

    /**
     * Name of chunk.
     */
    private final String chunkName;

    /**
     * Length of the chunk.
     */
    private final long startOffset;

    /**
     * Status bit flags.
     */
    private volatile int status;

    /**
     * Retrieves the key associated with the metadata, which is the name of the chunk.
     *
     * @return Name of the chunk.
     */
    @Override
    public String getKey() {
        return name;
    }

    /**
     * Creates a deep copy of this instance.
     *
     * @return Deep copy of this instance.
     */
    @Override
    public StorageMetadata deepCopy() {
        return toBuilder().build();
    }

    /**
     * Sets the given bit for given mask.
     */
    private ReadIndexBlockMetadata setFlag(int mask, boolean value) {
        status = value ? (status | mask) : (status & (~mask));
        return this;
    }

    /**
     * Gets the status of the bit for given mask.
     */
    private boolean getFlag(int mask) {
        return (status & mask) != 0;
    }

    /**
     * Sets active status.
     * @param value Value to set.
     * @return This instance so that these calls can be chained.
     */
    public ReadIndexBlockMetadata setActive(boolean value) {
        return setFlag(StatusFlags.ACTIVE, value);
    }

    /**
     * Gets active status.
     * @return True if active, false otherwise.
     */
    public boolean isActive() {
        return getFlag(StatusFlags.ACTIVE);
    }

    /**
     * Builder that implements {@link ObjectBuilder}.
     */
    public static class ReadIndexBlockMetadataBuilder implements ObjectBuilder<ReadIndexBlockMetadata> {
    }

    /**
     * Serializer that implements {@link VersionedSerializer}.
     */
    public static class Serializer extends VersionedSerializer.WithBuilder<ReadIndexBlockMetadata, ReadIndexBlockMetadataBuilder> {
        @Override
        protected ReadIndexBlockMetadataBuilder newBuilder() {
            return ReadIndexBlockMetadata.builder();
        }

        @Override
        protected byte getWriteVersion() {
            return 0;
        }

        @Override
        protected void declareVersions() {
            version(0).revision(0, this::write00, this::read00);
        }

        private void write00(ReadIndexBlockMetadata object, RevisionDataOutput output) throws IOException {
            output.writeUTF(object.name);
            output.writeUTF(object.chunkName);
            output.writeCompactLong(object.startOffset);
            output.writeCompactInt(object.status);
        }

        private void read00(RevisionDataInput input, ReadIndexBlockMetadataBuilder b) throws IOException {
            b.name(input.readUTF());
            b.chunkName(input.readUTF());
            b.startOffset(input.readCompactLong());
            b.status(input.readCompactInt());
        }
    }
}
