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
import io.pravega.controller.store.stream.records.HistoryTimeSeriesRecord;
import io.pravega.controller.store.stream.records.StreamSegmentRecord;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

public class HistoryTimeSeriesRecordSerializer extends AbstractSerializer {

    public static final String HISTORY_TIME_SERIES_RECORD_EPOCH = "epoch";
    public static final String HISTORY_TIME_SERIES_RECORD_REFERENCE_EPOCH = "referenceEpoch";
    public static final String HISTORY_TIME_SERIES_RECORD_SEGMENTS_SEALED = "segmentsSealed";
    public static final String HISTORY_TIME_SERIES_RECORD_SEGMENTS_CREATED = "segmentsCreated";
    public static final String HISTORY_TIME_SERIES_RECORD_SCALE_TIME = "scaleTime";

    private static final Map<String, Function<HistoryTimeSeriesRecord, String>> HISTORY_TIME_SERIES_RECORD_FIELD_MAP =
            ImmutableMap.<String, Function<HistoryTimeSeriesRecord, String>>builder()
                    .put(HISTORY_TIME_SERIES_RECORD_EPOCH, r -> String.valueOf(r.getEpoch()))
                    .put(HISTORY_TIME_SERIES_RECORD_REFERENCE_EPOCH, r -> String.valueOf(r.getReferenceEpoch()))
                    .put(HISTORY_TIME_SERIES_RECORD_SCALE_TIME, r -> String.valueOf(r.getScaleTime()))
                    .put(HISTORY_TIME_SERIES_RECORD_SEGMENTS_SEALED, r -> convertCollectionToString(r.getSegmentsSealed(), AbstractSerializer::convertStreamSegmentRecordToString))
                    .put(HISTORY_TIME_SERIES_RECORD_SEGMENTS_CREATED, r -> convertCollectionToString(r.getSegmentsCreated(), AbstractSerializer::convertStreamSegmentRecordToString))
                    .build();

    @Override
    public String getName() {
        return "HistoryTimeSeriesRecord";
    }

    @Override
    public ByteBuffer serialize(String value) {
        Map<String, String> data = parseStringData(value);
        List<StreamSegmentRecord> segmentsSealed = new ArrayList<>(convertStringToCollection(getAndRemoveIfExists(data, HISTORY_TIME_SERIES_RECORD_SEGMENTS_SEALED),
                AbstractSerializer::convertStringToStreamSegmentRecord));
        List<StreamSegmentRecord> segmentsCreated = new ArrayList<>(convertStringToCollection(getAndRemoveIfExists(data, HISTORY_TIME_SERIES_RECORD_SEGMENTS_CREATED),
                AbstractSerializer::convertStringToStreamSegmentRecord));

        HistoryTimeSeriesRecord record = new HistoryTimeSeriesRecord(
                Integer.parseInt(getAndRemoveIfExists(data, HISTORY_TIME_SERIES_RECORD_EPOCH)),
                Integer.parseInt(getAndRemoveIfExists(data, HISTORY_TIME_SERIES_RECORD_REFERENCE_EPOCH)),
                ImmutableList.copyOf(segmentsSealed),
                ImmutableList.copyOf(segmentsCreated),
                Long.parseLong(getAndRemoveIfExists(data, HISTORY_TIME_SERIES_RECORD_SCALE_TIME)));
        return new ByteArraySegment(record.toBytes()).asByteBuffer();
    }

    @Override
    public String deserialize(ByteBuffer serializedValue) {
        StringBuilder stringValueBuilder;
        HistoryTimeSeriesRecord data = HistoryTimeSeriesRecord.fromBytes(new ByteArraySegment(serializedValue).getCopy());
        stringValueBuilder = new StringBuilder();
        HISTORY_TIME_SERIES_RECORD_FIELD_MAP.forEach((name, f) -> appendField(stringValueBuilder, name, f.apply(data)));
        return stringValueBuilder.toString();
    }
}
