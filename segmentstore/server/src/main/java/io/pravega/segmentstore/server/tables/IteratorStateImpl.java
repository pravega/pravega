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
package io.pravega.segmentstore.server.tables;

import com.google.common.base.Preconditions;
import io.pravega.common.ObjectBuilder;
import io.pravega.common.io.serialization.RevisionDataInput;
import io.pravega.common.io.serialization.RevisionDataOutput;
import io.pravega.common.io.serialization.VersionedSerializer;
import io.pravega.common.util.ArrayView;
import io.pravega.common.util.BufferView;
import io.pravega.segmentstore.contracts.tables.IteratorState;
import java.io.IOException;
import java.util.UUID;
import lombok.Getter;
import lombok.NonNull;
import lombok.SneakyThrows;

/**
 * Represents the state of a resumable iterator.
 */
class IteratorStateImpl implements IteratorState {
    private static final Serializer SERIALIZER = new Serializer();

    /**
     * Gets the Key Hash of the last TableBucket contained in the iteration so far. When sorted lexicographically, all
     * TableBuckets with Hashes smaller than or equal to this one have been included.
     */
    @Getter
    @NonNull
    private final UUID keyHash;

    /**
     * Creates a new instance of the IteratorState class.
     *
     * @param keyHash The Key Hash to use.
     */
    IteratorStateImpl(@NonNull UUID keyHash) {
        Preconditions.checkArgument(KeyHasher.isValid(keyHash), "keyHash must be at least IteratorState.MIN_HASH and at most IteratorState.MAX_HASH.");
        this.keyHash = keyHash;
    }

    @Override
    public String toString() {
        return String.format("Hash = %s", this.keyHash);
    }

    //region Serialization

    /**
     * Creates a new instance of the IteratorState class from the given array.
     *
     * @param data The serialization of an IteratorState. This must have been generated using {@link #serialize()}.
     * @return As new instance of the IteratorState class.
     * @throws IOException If unable to deserialize.
     */
    static IteratorStateImpl deserialize(BufferView data) throws IOException {
        return SERIALIZER.deserialize(data);
    }

    /**
     * Serializes this IteratorState instance into an {@link ArrayView}.
     *
     * @return The {@link ArrayView} that was used for serialization.
     */
    @SneakyThrows(IOException.class)
    public ArrayView serialize() {
        return SERIALIZER.serialize(this);
    }

    private static class IteratorStateBuilder implements ObjectBuilder<IteratorStateImpl> {
        private UUID keyHash;

        @Override
        public IteratorStateImpl build() {
            return new IteratorStateImpl(keyHash);
        }
    }

    private static class Serializer extends VersionedSerializer.WithBuilder<IteratorStateImpl, IteratorStateBuilder> {
        @Override
        protected IteratorStateBuilder newBuilder() {
            return new IteratorStateBuilder();
        }

        @Override
        protected byte getWriteVersion() {
            return 0;
        }

        @Override
        protected void declareVersions() {
            version(0).revision(0, this::write00, this::read00);
        }

        private void read00(RevisionDataInput revisionDataInput, IteratorStateBuilder builder) throws IOException {
            builder.keyHash = revisionDataInput.readUUID();
        }

        private void write00(IteratorStateImpl state, RevisionDataOutput revisionDataOutput) throws IOException {
            revisionDataOutput.length(RevisionDataOutput.UUID_BYTES);
            revisionDataOutput.writeUUID(state.keyHash);
        }
    }

    //endregion
}
