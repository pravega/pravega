/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.server.tables;

import com.google.common.base.Preconditions;
import io.pravega.common.ObjectBuilder;
import io.pravega.common.io.serialization.RevisionDataInput;
import io.pravega.common.io.serialization.RevisionDataOutput;
import io.pravega.common.io.serialization.VersionedSerializer;
import io.pravega.common.util.ArrayView;
import lombok.Getter;
import lombok.SneakyThrows;

import java.io.IOException;

/**
 * Represents the state of a resumable iterator.
 */
public class EntryIteratorState {
    private static final Serializer SERIALIZER = new Serializer();

    private static final int BOOLEAN_BYTES = 1;

    @Getter
    private final long position;

    @Getter
    private final boolean reachedEnd;

    @Getter
    private final boolean shouldClear;

    @Getter
    private final boolean deletionRecord;

    /**
     * Creates a new instance of the EntryIteratorState class.
     *
     * @param position The position of the TableEntry
     * @param reachedEnd If the Entry is at the end of the segment (more recently appended).
     * @param shouldClear Marks if the client should clear their state (provided start position has been truncated).
     * @param deletionRecord The Entry read is marked for deletion.
     */
    EntryIteratorState(long position, boolean reachedEnd, boolean shouldClear, boolean deletionRecord) {
        Preconditions.checkArgument(isValid(position), "Position must be at least 0 (a non-negative integer).");
        this.position = position;
        this.reachedEnd = reachedEnd;
        this.shouldClear = shouldClear;
        this.deletionRecord = deletionRecord;
    }

    boolean isValid(long position) {
        return position >= 0;
    }

    @Override
    public String toString() {
        return String.format("Position = %s Reached End = %s Should Clear = %s", this.position, this.reachedEnd, this.shouldClear);
    }

    //region Serialization

    /**
     * Creates a new instance of the IteratorState class from the given array.
     *
     * @param data A byte array containing the serialization of an IteratorState. This must have been generated using
     *             {@link #serialize()}.
     * @return As new instance of the IteratorState class.
     */
    @SneakyThrows(IOException.class)
    public static EntryIteratorState deserialize(byte[] data) {
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

    private static class IteratorStateBuilder implements ObjectBuilder<EntryIteratorState> {
        private long position;
        private boolean reachedEnd;
        private boolean shouldClear;
        private boolean deletionRecord;

        @Override
        public EntryIteratorState build() {
            return new EntryIteratorState(position, reachedEnd, shouldClear, deletionRecord);
        }
    }

    private static class Serializer extends VersionedSerializer.WithBuilder<EntryIteratorState, IteratorStateBuilder> {
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
            builder.position = revisionDataInput.readCompactLong();
            builder.reachedEnd = revisionDataInput.readBoolean();
            builder.shouldClear = revisionDataInput.readBoolean();
            builder.deletionRecord = revisionDataInput.readBoolean();
        }

        private void write00(EntryIteratorState state, RevisionDataOutput revisionDataOutput) throws IOException {
            revisionDataOutput.length(revisionDataOutput.getCompactLongLength(state.position) + 3 * BOOLEAN_BYTES);
            revisionDataOutput.writeCompactLong(state.position);
            revisionDataOutput.writeBoolean(state.reachedEnd);
            revisionDataOutput.writeBoolean(state.shouldClear);
            revisionDataOutput.writeBoolean(state.deletionRecord);
        }
    }

    //endregion
}
