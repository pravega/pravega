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

import com.google.common.collect.ImmutableList;
import io.pravega.common.ObjectBuilder;
import io.pravega.common.io.serialization.RevisionDataInput;
import io.pravega.common.io.serialization.RevisionDataOutput;
import io.pravega.common.io.serialization.VersionedSerializer;
import lombok.Builder;
import lombok.Data;
import lombok.NonNull;
import lombok.SneakyThrows;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.stream.Collectors;

/**
 * This class stores chunks of the history time series.
 * Each chunk is of fixed size and contains list of epochs in form of HistoryTimeSeriesRecord.
 */
@Data
public class HistoryTimeSeries {
    public static final HistoryTimeSeriesSerializer SERIALIZER = new HistoryTimeSeriesSerializer();
    public static final int HISTORY_CHUNK_SIZE = 1000;

    private final ImmutableList<HistoryTimeSeriesRecord> historyRecords;

    @Builder
    public HistoryTimeSeries(@NonNull ImmutableList<HistoryTimeSeriesRecord> historyRecords) {
        this.historyRecords = historyRecords;
    }

    @SneakyThrows(IOException.class)
    public byte[] toBytes() {
        return SERIALIZER.serialize(this).getCopy();
    }

    @Override
    public String toString() {
        return String.format("%s = [%n    %s%n]", "historyRecords", historyRecords.stream()
                .map(historyTimeSeriesRecord -> historyTimeSeriesRecord.toString().replace("\n", "\n    "))
                .collect(Collectors.joining("\n,\n    ")));
    }

    @SneakyThrows(IOException.class)
    public static HistoryTimeSeries fromBytes(final byte[] record) {
        InputStream inputStream = new ByteArrayInputStream(record, 0, record.length);
        return SERIALIZER.deserialize(inputStream);
    }

    private static class HistoryTimeSeriesBuilder implements ObjectBuilder<HistoryTimeSeries> {

    }

    public HistoryTimeSeriesRecord getLatestRecord() {
        return historyRecords.get(historyRecords.size() - 1);
    }

    public static HistoryTimeSeries addHistoryRecord(HistoryTimeSeries series, HistoryTimeSeriesRecord record) {
        ImmutableList.Builder<HistoryTimeSeriesRecord> listBuilder = ImmutableList.builder();
        ImmutableList<HistoryTimeSeriesRecord> list = series.historyRecords;
        listBuilder.addAll(list);

        // add only if epoch is immediate epoch following the highest epoch in the series
        if (list.get(list.size() - 1).getEpoch() == record.getEpoch() - 1) {
            listBuilder.add(record);
        } else if (list.get(list.size() - 1).getEpoch() != record.getEpoch()) {
            throw new IllegalArgumentException("new epoch record is not continuous");
        }

        return new HistoryTimeSeries(listBuilder.build());
    }

    private static class HistoryTimeSeriesSerializer extends
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
            ImmutableList.Builder<HistoryTimeSeriesRecord> historyBuilder = ImmutableList.builder();
            revisionDataInput.readCollection(HistoryTimeSeriesRecord.SERIALIZER::deserialize, historyBuilder);
            builder.historyRecords(historyBuilder.build());
        }

        private void write00(HistoryTimeSeries history, RevisionDataOutput revisionDataOutput) throws IOException {
            revisionDataOutput.writeCollection(history.getHistoryRecords(), HistoryTimeSeriesRecord.SERIALIZER::serialize);
        }

        @Override
        protected HistoryTimeSeries.HistoryTimeSeriesBuilder newBuilder() {
            return HistoryTimeSeries.builder();
        }
    }
}
