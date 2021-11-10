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
package io.pravega.cli.admin.serializers.controller;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.pravega.cli.admin.serializers.AbstractSerializer;
import io.pravega.common.util.ByteArraySegment;
import io.pravega.controller.store.stream.records.HistoryTimeSeries;
import io.pravega.controller.store.stream.records.HistoryTimeSeriesRecord;
import io.pravega.controller.store.stream.records.StreamSegmentRecord;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

public class HistoryTimeSeriesSerializer extends AbstractSerializer {

    static final String HISTORY_TIME_SERIES_HISTORY_RECORDS = "historyRecords";

    static final String HISTORY_TIME_SERIES_RECORD_EPOCH = "epoch";
    static final String HISTORY_TIME_SERIES_RECORD_REFERENCE_EPOCH = "referenceEpoch";
    static final String HISTORY_TIME_SERIES_RECORD_SEGMENTS_SEALED = "segmentsSealed";
    static final String HISTORY_TIME_SERIES_RECORD_SEGMENTS_CREATED = "segmentsCreated";
    static final String HISTORY_TIME_SERIES_RECORD_SCALE_TIME = "scaleTime";

    static final String HISTORY_TIME_SERIES_RECORD_LIST_ENTRY_DELIMITER = "\n";

    static final String HISTORY_TIME_SERIES_RECORD_PAIR_DELIMITER = "[]";
    static final String HISTORY_TIME_SERIES_RECORD_VALUE_DELIMITER = "->";
    private static final String HISTORY_TIME_SERIES_RECORD_PAIR_DELIMITER_REGEX = "\\[]";

    private static final Map<String, Function<HistoryTimeSeriesRecord, String>> HISTORY_TIME_SERIES_RECORD_FIELD_MAP =
            ImmutableMap.<String, Function<HistoryTimeSeriesRecord, String>>builder()
                    .put(HISTORY_TIME_SERIES_RECORD_EPOCH, r -> String.valueOf(r.getEpoch()))
                    .put(HISTORY_TIME_SERIES_RECORD_REFERENCE_EPOCH, r -> String.valueOf(r.getReferenceEpoch()))
                    .put(HISTORY_TIME_SERIES_RECORD_SCALE_TIME, r -> String.valueOf(r.getScaleTime()))
                    .put(HISTORY_TIME_SERIES_RECORD_SEGMENTS_SEALED, r -> convertCollectionToString(r.getSegmentsSealed(), AbstractSerializer::convertStreamSegmentRecordToString))
                    .put(HISTORY_TIME_SERIES_RECORD_SEGMENTS_CREATED, r -> convertCollectionToString(r.getSegmentsCreated(), AbstractSerializer::convertStreamSegmentRecordToString))
                    .build();

    private static final Map<String, Function<HistoryTimeSeries, String>> HISTORY_TIME_SERIES_FIELD_MAP =
            ImmutableMap.<String, Function<HistoryTimeSeries, String>>builder()
                    .put(HISTORY_TIME_SERIES_HISTORY_RECORDS, r -> convertCollectionToStringWithCustomDelimiter(r.getHistoryRecords(),
                            HistoryTimeSeriesSerializer::convertHistoryTimeSeriesRecordToString, HISTORY_TIME_SERIES_RECORD_LIST_ENTRY_DELIMITER))
                    .build();

    @Override
    public String getName() {
        return "HistoryTimeSeries";
    }

    @Override
    public ByteBuffer serialize(String value) {
        Map<String, String> data = parseStringData(value);
        List<HistoryTimeSeriesRecord> historyRecords = new ArrayList<>(
                convertStringToCollectionWithCustomDelimiter(getAndRemoveIfExists(data, HISTORY_TIME_SERIES_HISTORY_RECORDS),
                HistoryTimeSeriesSerializer::convertStringToHistoryTimeSeriesRecord, HISTORY_TIME_SERIES_RECORD_LIST_ENTRY_DELIMITER));

        HistoryTimeSeries record = new HistoryTimeSeries(ImmutableList.copyOf(historyRecords));
        return new ByteArraySegment(record.toBytes()).asByteBuffer();
    }

    @Override
    public String deserialize(ByteBuffer serializedValue) {
        return applyDeserializer(serializedValue, HistoryTimeSeries::fromBytes, HISTORY_TIME_SERIES_FIELD_MAP);
    }

    private static String convertHistoryTimeSeriesRecordToString(HistoryTimeSeriesRecord record) {
        StringBuilder stringValueBuilder = new StringBuilder();
        HISTORY_TIME_SERIES_RECORD_FIELD_MAP.forEach((name, f) ->
                appendFieldWithCustomDelimiters(stringValueBuilder, name, f.apply(record),
                        HISTORY_TIME_SERIES_RECORD_PAIR_DELIMITER, HISTORY_TIME_SERIES_RECORD_VALUE_DELIMITER));
        return stringValueBuilder.toString();
    }

    private static HistoryTimeSeriesRecord convertStringToHistoryTimeSeriesRecord(String recordString) {
        Map<String, String> data = parseStringDataWithCustomDelimiters(recordString, HISTORY_TIME_SERIES_RECORD_PAIR_DELIMITER_REGEX, 
                HISTORY_TIME_SERIES_RECORD_VALUE_DELIMITER);
        List<StreamSegmentRecord> segmentsSealed = new ArrayList<>(convertStringToCollection(getAndRemoveIfExists(data, HISTORY_TIME_SERIES_RECORD_SEGMENTS_SEALED),
                AbstractSerializer::convertStringToStreamSegmentRecord));
        List<StreamSegmentRecord> segmentsCreated = new ArrayList<>(convertStringToCollection(getAndRemoveIfExists(data, HISTORY_TIME_SERIES_RECORD_SEGMENTS_CREATED),
                AbstractSerializer::convertStringToStreamSegmentRecord));

        return new HistoryTimeSeriesRecord(Integer.parseInt(getAndRemoveIfExists(data, HISTORY_TIME_SERIES_RECORD_EPOCH)),
                Integer.parseInt(getAndRemoveIfExists(data, HISTORY_TIME_SERIES_RECORD_REFERENCE_EPOCH)),
                ImmutableList.copyOf(segmentsSealed),
                ImmutableList.copyOf(segmentsCreated),
                Long.parseLong(getAndRemoveIfExists(data, HISTORY_TIME_SERIES_RECORD_SCALE_TIME)));
    }
}
