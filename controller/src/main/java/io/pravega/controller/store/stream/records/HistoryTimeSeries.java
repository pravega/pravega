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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import io.pravega.common.ObjectBuilder;
import io.pravega.controller.store.stream.records.serializers.HistoryTimeSeriesSerializer;
import lombok.Builder;
import lombok.Data;
import lombok.SneakyThrows;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.LinkedList;
import java.util.List;

@Data
/**
 * This class stores chunks of the history time series.
 * Each chunk is of fixed size and contains list of epochs in form of HistoryTimeSeriesRecord.
 */
public class HistoryTimeSeries {
    public static final HistoryTimeSeriesSerializer SERIALIZER = new HistoryTimeSeriesSerializer();

    private final List<HistoryTimeSeriesRecord> historyRecords;

    @Builder
    HistoryTimeSeries(List<HistoryTimeSeriesRecord> historyRecords) {
        this.historyRecords = new LinkedList<>();
        this.historyRecords.addAll(historyRecords);
    }

    @SneakyThrows(IOException.class)
    public byte[] toByteArray() {
        return SERIALIZER.serialize(this).getCopy();
    }

    @SneakyThrows(IOException.class)
    public static HistoryTimeSeries parse(final byte[] record) {
        InputStream inputStream = new ByteArrayInputStream(record, 0, record.length);
        return SERIALIZER.deserialize(inputStream);
    }

    public static class HistoryTimeSeriesBuilder implements ObjectBuilder<HistoryTimeSeries> {

    }

    public ImmutableList<HistoryTimeSeriesRecord> getHistoryRecords() {
        return ImmutableList.copyOf(historyRecords);
    }

    public HistoryTimeSeriesRecord getLatestRecord() {
        return historyRecords.get(historyRecords.size() - 1);
    }

    public static HistoryTimeSeries addHistoryRecord(HistoryTimeSeries series, HistoryTimeSeriesRecord record) {
        List<HistoryTimeSeriesRecord> list = Lists.newArrayList(series.historyRecords);

        // add only if cut.recordingTime is newer than any previous record
        if (list.stream().noneMatch(x -> x.getScaleTime() >= record.getScaleTime())) {
            list.add(record);
        }

        return new HistoryTimeSeries(list);
    }
}
