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

import javax.annotation.concurrent.ThreadSafe;
import java.io.IOException;

import static com.google.common.base.Strings.emptyToNull;
import static com.google.common.base.Strings.nullToEmpty;

/**
 * Represents chunk metadata.
 * Following metadata is stored.
 * <ul>
 * <li>Name of the chunk.</li>
 * <li>Length of the chunk.</li>
 * <li>Name of the next chunk in list.</li>
 * <li>Status flags.</li>
 * </ul>
 */
@Builder(toBuilder = true)
@Data
@EqualsAndHashCode(callSuper = true)
@ThreadSafe
public class ChunkMetadata extends StorageMetadata {
    /**
     * Name of this chunk.
     */
    private final String name;

    /**
     * Length of the chunk.
     */
    private volatile long length;

    /**
     * Name of the next chunk.
     */
    private volatile String nextChunk;

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
    private ChunkMetadata setFlag(int mask, boolean value) {
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
    public ChunkMetadata setActive(boolean value) {
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
    public static class ChunkMetadataBuilder implements ObjectBuilder<ChunkMetadata> {
    }

    /**
     * Serializer that implements {@link VersionedSerializer}.
     */
    public static class Serializer extends VersionedSerializer.WithBuilder<ChunkMetadata, ChunkMetadataBuilder> {
        @Override
        protected ChunkMetadataBuilder newBuilder() {
            return ChunkMetadata.builder();
        }

        @Override
        protected byte getWriteVersion() {
            return 0;
        }

        @Override
        protected void declareVersions() {
            version(0).revision(0, this::write00, this::read00);
        }

        private void write00(ChunkMetadata object, RevisionDataOutput output) throws IOException {
            output.writeUTF(object.name);
            output.writeCompactLong(object.length);
            output.writeUTF(nullToEmpty(object.nextChunk));
            output.writeCompactInt(object.status);
        }

        private void read00(RevisionDataInput input, ChunkMetadataBuilder b) throws IOException {
            b.name(input.readUTF());
            b.length(input.readCompactLong());
            b.nextChunk(emptyToNull(input.readUTF()));
            b.status(input.readCompactInt());
        }
    }
}
