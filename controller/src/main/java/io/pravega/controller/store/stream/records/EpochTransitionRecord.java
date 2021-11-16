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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.pravega.common.ObjectBuilder;
import io.pravega.common.io.serialization.RevisionDataInput;
import io.pravega.common.io.serialization.RevisionDataOutput;
import io.pravega.common.io.serialization.VersionedSerializer;
import lombok.Builder;
import lombok.Data;
import lombok.NonNull;
import lombok.SneakyThrows;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.AbstractMap;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Transient record that is created while epoch transition takes place and captures the transition. This record is deleted
 * once transition completes.
 */
@Data
public class EpochTransitionRecord {
    public static final EpochTransitionRecordSerializer SERIALIZER = new EpochTransitionRecordSerializer();
    public static final EpochTransitionRecord EMPTY = new EpochTransitionRecord(Integer.MIN_VALUE, Long.MIN_VALUE, ImmutableSet.of(), ImmutableMap.of());

    /**
     * Active epoch at the time of requested transition.
     */
    final int activeEpoch;
    /**
     * Time when this epoch creation request was started.
     */
    final long time;
    /**
     * Segments to be sealed.
     */
    final ImmutableSet<Long> segmentsToSeal;
    /**
     * Key ranges for new segments to be created.
     */
    final ImmutableMap<Long, Map.Entry<Double, Double>> newSegmentsWithRange;

    private static class EpochTransitionRecordBuilder implements ObjectBuilder<EpochTransitionRecord> {

    }

    @Builder
    public EpochTransitionRecord(int activeEpoch, long time, @NonNull ImmutableSet<Long> segmentsToSeal,
                                 @NonNull ImmutableMap<Long, Map.Entry<Double, Double>> newSegmentsWithRange) {
        this.activeEpoch = activeEpoch;
        this.time = time;
        this.segmentsToSeal = segmentsToSeal;
        this.newSegmentsWithRange = newSegmentsWithRange;
    }

    public int getNewEpoch() {
        return activeEpoch + 1;
    }
    
    @SneakyThrows(IOException.class)
    public static EpochTransitionRecord fromBytes(final byte[] data) {
        return SERIALIZER.deserialize(data);
    }

    @SneakyThrows(IOException.class)
    public byte[] toBytes() {
        return SERIALIZER.serialize(this).getCopy();
    }

    @Override
    public String toString() {
        return String.format("%s = %s", "activeEpoch", activeEpoch) + "\n" +
                String.format("%s = %s", "time", time) + "\n" +
                String.format("%s = %s", "segmentsToSeal", segmentsToSeal) + "\n" +
                String.format("%s = %s", "newSegmentsWithRange", newSegmentsWithRange.keySet().stream()
                        .map(key -> key + " : (" + newSegmentsWithRange.get(key).getKey() + ", " + newSegmentsWithRange.get(key).getValue() + ")")
                        .collect(Collectors.joining(", ", "{", "}")));
    }
    
    private static class EpochTransitionRecordSerializer
            extends VersionedSerializer.WithBuilder<EpochTransitionRecord, EpochTransitionRecord.EpochTransitionRecordBuilder> {
        @Override
        protected byte getWriteVersion() {
            return 0;
        }

        @Override
        protected void declareVersions() {
            version(0).revision(0, this::write00, this::read00);
        }

        private void read00(RevisionDataInput revisionDataInput,
                            EpochTransitionRecord.EpochTransitionRecordBuilder epochTransitionRecordBuilder) throws IOException {
            epochTransitionRecordBuilder.activeEpoch(revisionDataInput.readInt())
                                        .time(revisionDataInput.readLong());

            ImmutableSet.Builder<Long> segmentsToSealBuilder = ImmutableSet.builder();
            revisionDataInput.readCollection(DataInput::readLong, segmentsToSealBuilder);
            epochTransitionRecordBuilder
                    .segmentsToSeal(segmentsToSealBuilder.build());

            ImmutableMap.Builder<Long, Map.Entry<Double, Double>> builder = ImmutableMap.builder();
            revisionDataInput.readMap(DataInput::readLong, this::readValue, builder);
            epochTransitionRecordBuilder.newSegmentsWithRange(builder.build()).build();
        }

        private Map.Entry<Double, Double> readValue(RevisionDataInput revisionDataInput) throws IOException {
            Map<Double, Double> map = revisionDataInput.readMap(DataInput::readDouble, DataInput::readDouble);
            Optional<Double> keyOpt = map.keySet().stream().findFirst();
            Optional<Double> value = map.values().stream().findFirst();
            return keyOpt.map(key -> new AbstractMap.SimpleEntry<>(key, value.orElse(null))).orElse(null);
        }

        private void write00(EpochTransitionRecord epochTransitionRecord, RevisionDataOutput revisionDataOutput) throws IOException {
            revisionDataOutput.writeInt(epochTransitionRecord.getActiveEpoch());
            revisionDataOutput.writeLong(epochTransitionRecord.getTime());
            revisionDataOutput.writeCollection(epochTransitionRecord.getSegmentsToSeal(), DataOutput::writeLong);
            revisionDataOutput.writeMap(epochTransitionRecord.getNewSegmentsWithRange(), DataOutput::writeLong, this::writeValue);
        }

        private void writeValue(RevisionDataOutput revisionDataOutput, Map.Entry<Double, Double> value) throws IOException {
            Map<Double, Double> map = new HashMap<>();
            map.put(value.getKey(), value.getValue());
            revisionDataOutput.writeMap(map, DataOutput::writeDouble, DataOutput::writeDouble);
        }

        @Override
        protected EpochTransitionRecord.EpochTransitionRecordBuilder newBuilder() {
            return EpochTransitionRecord.builder();
        }
    }
}
