/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.store.stream.records.serializers;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.pravega.common.io.serialization.RevisionDataInput;
import io.pravega.common.io.serialization.RevisionDataOutput;
import io.pravega.common.io.serialization.VersionedSerializer;
import io.pravega.controller.store.stream.records.EpochTransitionRecord;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class EpochTransitionRecordSerializer
        extends VersionedSerializer.WithBuilder<EpochTransitionRecord, EpochTransitionRecord.EpochTransitionRecordBuilder> {
    @Override
    protected byte writeVersion() {
        return 0;
    }

    @Override
    protected void declareVersions() {
        version(0).revision(0, this::write00, this::read00);
    }

    private void read00(RevisionDataInput revisionDataInput,
                        EpochTransitionRecord.EpochTransitionRecordBuilder epochTransitionRecordBuilder) throws IOException {
        epochTransitionRecordBuilder.activeEpoch(revisionDataInput.readInt())
                .newEpoch(revisionDataInput.readInt())
                .time(revisionDataInput.readLong());

        ArrayList<Integer> ts = revisionDataInput.readCollection(DataInput::readInt, ArrayList::new);
        Map<Integer, AbstractMap.SimpleEntry<Double, Double>> kvMap = revisionDataInput.readMap(DataInput::readInt, this::readValue);

        epochTransitionRecordBuilder
                .segmentsToSeal(ImmutableSet.copyOf(ts))
                .newSegmentsWithRange(ImmutableMap.copyOf(kvMap))
                .build();
    }

    private AbstractMap.SimpleEntry<Double, Double> readValue(RevisionDataInput revisionDataInput) throws IOException {
        Map<Double, Double> map = revisionDataInput.readMap(DataInput::readDouble, DataInput::readDouble);
        Optional<Double> keyOpt = map.keySet().stream().findFirst();
        Optional<Double> value = map.values().stream().findFirst();
        return keyOpt.map(key -> new AbstractMap.SimpleEntry<>(key, value.orElse(null))).orElse(null);
    }

    private void write00(EpochTransitionRecord epochTransitionRecord, RevisionDataOutput revisionDataOutput) throws IOException {
        revisionDataOutput.writeInt(epochTransitionRecord.getActiveEpoch());
        revisionDataOutput.writeInt(epochTransitionRecord.getNewEpoch());
        revisionDataOutput.writeLong(epochTransitionRecord.getTime());
        revisionDataOutput.writeCollection(epochTransitionRecord.getSegmentsToSeal(), DataOutput::writeInt);
        revisionDataOutput.writeMap(epochTransitionRecord.getNewSegmentsWithRange(), DataOutput::writeInt, this::writeValue);
    }

    private void writeValue(RevisionDataOutput revisionDataOutput, AbstractMap.SimpleEntry<Double, Double> value) throws IOException {
        Map<Double, Double> map = new HashMap<>();
        map.put(value.getKey(), value.getValue());
        revisionDataOutput.writeMap(map, DataOutput::writeDouble, DataOutput::writeDouble);
    }

    @Override
    protected EpochTransitionRecord.EpochTransitionRecordBuilder newBuilder() {
        return EpochTransitionRecord.builder();
    }
}
