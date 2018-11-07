/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.server.tables;

import io.pravega.common.ObjectBuilder;
import io.pravega.common.io.serialization.RevisionDataInput;
import io.pravega.common.io.serialization.RevisionDataOutput;
import io.pravega.common.io.serialization.VersionedSerializer;
import io.pravega.common.util.ArrayView;
import java.io.IOException;
import lombok.Builder;
import lombok.Getter;
import lombok.SneakyThrows;

/**
 * Represents the state of a resumable iterator.
 */
@Builder
class IteratorState {
    /**
     * An IteratorState that indicates there are no more items left in the iteration.
     */
    public static final IteratorState END = new IteratorState(null);
    private static final Serializer SERIALIZER = new Serializer();

    /**
     * Gets the KeyHash of the last TableBucket contained in the iteration so far. When sorted lexicographically, all
     * TableBuckets with KeyHashes smaller than or equal to this one have been included.
     * If null, indicates there are no more items left in the iteration.
     */
    @Getter
    private final byte[] lastBucketHash;

    static IteratorState deserialize(byte[] data) throws IOException {
        return SERIALIZER.deserialize(data);
    }

    /**
     * Gets a value indicating whether the iteration for this IteratorState has reached an end.
     *
     * @return True if the iteration has reached an end, false otherwise.
     */
    public boolean isEnd() {
        return this.lastBucketHash == null;
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

    //region Serializer

    public static class IteratorStateBuilder implements ObjectBuilder<IteratorState> {
    }

    private static class Serializer extends VersionedSerializer.WithBuilder<IteratorState, IteratorStateBuilder> {
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
            byte[] hash = revisionDataInput.readArray();
            builder.lastBucketHash(hash.length == 0 ? null : hash);
        }

        private void write00(IteratorState state, RevisionDataOutput revisionDataOutput) throws IOException {
            revisionDataOutput.length(revisionDataOutput.getCollectionLength(state.lastBucketHash == null ? 0 : state.lastBucketHash.length, 1));
            revisionDataOutput.writeArray(state.lastBucketHash);
        }
    }

    //endregion
}
