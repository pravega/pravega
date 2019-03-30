/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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
import lombok.SneakyThrows;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

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
    final Set<Long> segmentsToSeal;
    /**
     * Key ranges for new segments to be created.
     */
    final Map<Long, Map.Entry<Double, Double>> newSegmentsWithRange;

    private static class EpochTransitionRecordBuilder implements ObjectBuilder<EpochTransitionRecord> {

    }

    @Builder
    private EpochTransitionRecord(int activeEpoch, long time, Set<Long> segmentsToSeal, Map<Long, Map.Entry<Double, Double>> newSegmentsWithRange, 
                                 boolean copyCollection) {
        this.activeEpoch = activeEpoch;
        this.time = time;
        this.segmentsToSeal = copyCollection ? ImmutableSet.copyOf(segmentsToSeal) : segmentsToSeal;
        this.newSegmentsWithRange = copyCollection ? ImmutableMap.copyOf(newSegmentsWithRange) : newSegmentsWithRange;
    }
    
    public EpochTransitionRecord(int activeEpoch, long time, Set<Long> segmentsToSeal, Map<Long, Map.Entry<Double, Double>> newSegmentsWithRange) {
        this(activeEpoch, time, segmentsToSeal, newSegmentsWithRange, true);
    }

    public Set<Long> getSegmentsToSeal() {
        return Collections.unmodifiableSet(segmentsToSeal);
    }

    public Map<Long, Map.Entry<Double, Double>> getNewSegmentsWithRange() {
        return Collections.unmodifiableMap(newSegmentsWithRange);
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

            ArrayList<Long> ts = revisionDataInput.readCollection(DataInput::readLong, ArrayList::new);
            Map<Long, Map.Entry<Double, Double>> kvMap = revisionDataInput.readMap(DataInput::readLong, this::readValue);

            epochTransitionRecordBuilder
                    .segmentsToSeal(ImmutableSet.copyOf(ts))
                    .newSegmentsWithRange(ImmutableMap.copyOf(kvMap))
                    .copyCollection(false)
                    .build();
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
