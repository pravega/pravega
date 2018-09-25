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

import io.pravega.common.io.serialization.RevisionDataInput;
import io.pravega.common.io.serialization.RevisionDataOutput;
import io.pravega.common.io.serialization.VersionedSerializer;
import io.pravega.controller.store.stream.records.HistoryTimeSeriesRecord;
import io.pravega.controller.store.stream.records.StreamSegmentRecord;

import java.io.IOException;
import java.util.ArrayList;

public class HistoryTimeSeriesRecordSerializer extends
        VersionedSerializer.WithBuilder<HistoryTimeSeriesRecord, HistoryTimeSeriesRecord.HistoryTimeSeriesRecordBuilder> {
    @Override
    protected byte getWriteVersion() {
        return 0;
    }

    @Override
    protected void declareVersions() {
        version(0).revision(0, this::write00, this::read00);
    }

    private void read00(RevisionDataInput revisionDataInput, HistoryTimeSeriesRecord.HistoryTimeSeriesRecordBuilder builder) throws IOException {
        builder.epoch(revisionDataInput.readInt())
                .referenceEpoch(revisionDataInput.readInt())
                .segmentsSealed(revisionDataInput.readCollection(StreamSegmentRecord.SERIALIZER::deserialize, ArrayList::new))
                .segmentsCreated(revisionDataInput.readCollection(StreamSegmentRecord.SERIALIZER::deserialize, ArrayList::new))
                .creationTime(revisionDataInput.readLong());
    }

    private void write00(HistoryTimeSeriesRecord history, RevisionDataOutput revisionDataOutput) throws IOException {
        revisionDataOutput.writeInt(history.getEpoch());
        revisionDataOutput.writeInt(history.getReferenceEpoch());
        revisionDataOutput.writeCollection(history.getSegmentsSealed(), StreamSegmentRecord.SERIALIZER::serialize);
        revisionDataOutput.writeCollection(history.getSegmentsCreated(), StreamSegmentRecord.SERIALIZER::serialize);
        revisionDataOutput.writeLong(history.getScaleTime());
    }

    @Override
    protected HistoryTimeSeriesRecord.HistoryTimeSeriesRecordBuilder newBuilder() {
        return HistoryTimeSeriesRecord.builder();
    }
}