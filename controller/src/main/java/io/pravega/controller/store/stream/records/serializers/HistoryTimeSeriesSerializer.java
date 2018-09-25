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
import io.pravega.controller.store.stream.records.HistoryTimeSeries;
import io.pravega.controller.store.stream.records.HistoryTimeSeriesRecord;

import java.io.IOException;
import java.util.ArrayList;

public class HistoryTimeSeriesSerializer extends
        VersionedSerializer.WithBuilder<HistoryTimeSeries, HistoryTimeSeries.HistoryTimeSeriesBuilder> {
    @Override
    protected byte getWriteVersion() {
        return 0;
    }

    @Override
    protected void declareVersions() {
        version(0).revision(0, this::write00, this::read00);
    }

    private void read00(RevisionDataInput revisionDataInput, HistoryTimeSeries.HistoryTimeSeriesBuilder builder) throws IOException {
        builder.historyRecords(revisionDataInput.readCollection(HistoryTimeSeriesRecord.SERIALIZER::deserialize,
                ArrayList::new));
    }

    private void write00(HistoryTimeSeries history, RevisionDataOutput revisionDataOutput) throws IOException {
        revisionDataOutput.writeCollection(history.getHistoryRecords(), HistoryTimeSeriesRecord.SERIALIZER::serialize);
    }

    @Override
    protected HistoryTimeSeries.HistoryTimeSeriesBuilder newBuilder() {
        return HistoryTimeSeries.builder();
    }
}