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
package io.pravega.controller.store.stream.records;

import io.pravega.common.ObjectBuilder;
import io.pravega.common.io.serialization.RevisionDataInput;
import io.pravega.common.io.serialization.RevisionDataOutput;
import io.pravega.common.io.serialization.VersionedSerializer;
import io.pravega.controller.store.stream.State;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;

/**
 * A serializable class that encapsulates a Stream's State.
 */
@Data
@Builder
@Slf4j
@AllArgsConstructor
public class StateRecord {
    public static final StateRecordSerializer SERIALIZER = new StateRecordSerializer();

    private final State state;

    public static class StateRecordBuilder implements ObjectBuilder<StateRecord> {

    }

    @SneakyThrows(IOException.class)
    public static StateRecord fromBytes(final byte[] data) {
        return SERIALIZER.deserialize(data);
    }

    @SneakyThrows(IOException.class)
    public byte[] toBytes() {
        return SERIALIZER.serialize(this).getCopy();
    }

    @Override
    public String toString() {
        return String.format("%s = %s", "state", state);
    }

    private static class StateRecordSerializer extends VersionedSerializer.WithBuilder<StateRecord, StateRecord.StateRecordBuilder> {
        @Override
        protected byte getWriteVersion() {
            return 0;
        }

        @Override
        protected void declareVersions() {
            version(0).revision(0, this::write00, this::read00);
        }

        private void read00(RevisionDataInput revisionDataInput, StateRecord.StateRecordBuilder builder) throws IOException {
            int ordinal = revisionDataInput.readCompactInt();
            if (ordinal < State.values().length) {
                builder.state(State.values()[ordinal]);
            } else {
                throw new VersionMismatchException(StateRecord.class.getName());
            }
        }

        private void write00(StateRecord state, RevisionDataOutput revisionDataOutput) throws IOException {
            revisionDataOutput.writeCompactInt(state.getState().ordinal());
        }

        @Override
        protected StateRecord.StateRecordBuilder newBuilder() {
            return StateRecord.builder();
        }
    }
}
