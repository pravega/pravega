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
package io.pravega.client.tables.impl;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.pravega.common.ObjectBuilder;
import io.pravega.common.io.serialization.RevisionDataInput;
import io.pravega.common.io.serialization.RevisionDataOutput;
import io.pravega.common.io.serialization.VersionedSerializer;
import io.pravega.shared.protocol.netty.ByteBufWrapper;
import java.io.IOException;
import java.nio.ByteBuffer;
import lombok.Builder;
import lombok.Data;
import lombok.NonNull;
import lombok.SneakyThrows;

/**
 * Key-Value Table Key/Entry Iterator state.
 */
@Data
@Builder
class KeyValueTableIteratorState {
    private static final KeyValueTableIteratorStateSerializer SERIALIZER = new KeyValueTableIteratorStateSerializer();
    /**
     * Name of the Key-Value Table the iterator belongs to.
     */
    @NonNull
    private final String keyValueTableName;
    /**
     * Id of the Segment the Iterator is currently on.
     */
    private final long segmentId;
    /**
     * Iterator State for the Segment Iterator (this is an opaque buffer that is passed along to the {@link TableSegment}).
     */
    private final ByteBuf segmentIteratorState;

    //region Serialization

    @SneakyThrows(IOException.class)
    ByteBuffer toBytes() {
        return SERIALIZER.serialize(this).asByteBuffer();
    }

    @SneakyThrows(IOException.class)
    static KeyValueTableIteratorState fromBytes(ByteBuffer serialization) {
        return KeyValueTableIteratorState.SERIALIZER.deserialize(new ByteBufWrapper(Unpooled.wrappedBuffer(serialization)).getReader());
    }

    private static class KeyValueTableIteratorStateBuilder implements ObjectBuilder<KeyValueTableIteratorState> {
    }

    static class KeyValueTableIteratorStateSerializer extends
            VersionedSerializer.WithBuilder<KeyValueTableIteratorState, KeyValueTableIteratorState.KeyValueTableIteratorStateBuilder> {
        @Override
        protected KeyValueTableIteratorState.KeyValueTableIteratorStateBuilder newBuilder() {
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

        private void read00(RevisionDataInput revisionDataInput, KeyValueTableIteratorState.KeyValueTableIteratorStateBuilder builder) throws IOException {
            builder.keyValueTableName(revisionDataInput.readUTF());
            builder.segmentId(revisionDataInput.readLong());
            byte[] array = revisionDataInput.readArray();
            if (array != null && array.length > 0) {
                builder.segmentIteratorState(Unpooled.wrappedBuffer(array));
            }
        }

        private void write00(KeyValueTableIteratorState checkpoint, RevisionDataOutput revisionDataOutput) throws IOException {
            revisionDataOutput.writeUTF(checkpoint.keyValueTableName);
            revisionDataOutput.writeLong(checkpoint.segmentId);
            revisionDataOutput.writeBuffer(checkpoint.segmentIteratorState == null ? null : new ByteBufWrapper(checkpoint.segmentIteratorState));
        }
    }

    //endregion
}
