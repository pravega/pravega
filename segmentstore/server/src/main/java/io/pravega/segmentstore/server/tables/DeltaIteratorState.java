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
import lombok.Builder;
import lombok.Getter;
import lombok.SneakyThrows;
import lombok.ToString;

import java.io.IOException;

/**
 * Represents the state of a resumable delta iterator.
 *
 * The DeltaIteratorState class is used in conjunction with {@link io.pravega.segmentstore.contracts.tables.TableStore#entryDeltaIterator}.
 * An 'EntryDeltaIterator' iterates over a TableSegment directly via a {@link io.pravega.segmentstore.server.DirectSegmentAccess} view,
 * instead of indirectly through index structures ({@link TableBucket}), as is the case with {@link io.pravega.segmentstore.contracts.tables.TableStore#entryIterator}.
 * This 'raw' iteration provides meaning to fields such as {@link DeltaIteratorState#reachedEnd} and {@link DeltaIteratorState#fromPosition}.
 */
@ToString
@Builder
public class DeltaIteratorState implements IteratorState {
    private static final Serializer SERIALIZER = new Serializer();
    private static final int BOOLEAN_BYTES = 1;
    @Getter
    private final long fromPosition;
    @Getter
    private final boolean reachedEnd;
    @Getter
    private final boolean shouldClear;
    @Getter
    private final boolean deletionRecord;

    /**
     * Creates a new instance of the DeltaIteratorState class.
     *
     * @param fromPosition The next position to start iteration from.
     * @param reachedEnd If the Entry is at the end of the segment (more recently appended).
     * @param shouldClear Marks if the client should clear their state (provided start position has been truncated).
     * @param deletionRecord The Entry most recently read has been marked for deletion.
     */
    DeltaIteratorState(long fromPosition, boolean reachedEnd, boolean shouldClear, boolean deletionRecord) {
        Preconditions.checkArgument(isValid(fromPosition), "Position must be at least 0 (a non-negative integer).");
        this.fromPosition = fromPosition;
        this.reachedEnd = reachedEnd;
        this.shouldClear = shouldClear;
        this.deletionRecord = deletionRecord;
    }

    /**
     * Creates a new instance of the DeltaIteratorState that reflects an empty TableSegment.
     */
    public DeltaIteratorState() {
        this.fromPosition = 0;
        this.reachedEnd = true;
        this.shouldClear = false;
        this.deletionRecord = false;
    }

    static boolean isValid(long position) {
        return position >= 0;
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
    public static DeltaIteratorState deserialize(BufferView data) {
        return SERIALIZER.deserialize(data);
    }

    /**
     * Serializes this IteratorState instance into an {@link ArrayView}.
     *
     * @return The {@link ArrayView} that was used for serialization.
     */
    @Override
    @SneakyThrows(IOException.class)
    public ArrayView serialize() {
        return SERIALIZER.serialize(this);
    }

    private static class DeltaIteratorStateBuilder implements ObjectBuilder<DeltaIteratorState> {
    }

    private static class Serializer extends VersionedSerializer.WithBuilder<DeltaIteratorState, DeltaIteratorStateBuilder> {
        @Override
        protected DeltaIteratorStateBuilder newBuilder() {
            return new DeltaIteratorStateBuilder();
        }

        @Override
        protected byte getWriteVersion() {
            return 0;
        }

        @Override
        protected void declareVersions() {
            version(0).revision(0, this::write00, this::read00);
        }

        private void read00(RevisionDataInput revisionDataInput, DeltaIteratorStateBuilder builder) throws IOException {
            builder.fromPosition = revisionDataInput.readCompactLong();
            builder.reachedEnd = revisionDataInput.readBoolean();
            builder.shouldClear = revisionDataInput.readBoolean();
            builder.deletionRecord = revisionDataInput.readBoolean();
        }

        private void write00(DeltaIteratorState state, RevisionDataOutput revisionDataOutput) throws IOException {
            revisionDataOutput.length(revisionDataOutput.getCompactLongLength(state.fromPosition) + 3 * BOOLEAN_BYTES);
            revisionDataOutput.writeCompactLong(state.fromPosition);
            revisionDataOutput.writeBoolean(state.reachedEnd);
            revisionDataOutput.writeBoolean(state.shouldClear);
            revisionDataOutput.writeBoolean(state.deletionRecord);
        }
    }

    //endregion
}
