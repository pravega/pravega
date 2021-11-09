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
import io.pravega.controller.store.stream.records.RetentionSet;
import io.pravega.controller.store.stream.records.StreamCutReferenceRecord;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

public class RetentionSetSerializer extends AbstractSerializer {

    static final String RETENTION_SET_RETENTION_RECORDS = "retentionRecords";

    static final String STREAM_CUT_REFERENCE_RECORD_RECORDING_TIME = "recordingTime";
    static final String STREAM_CUT_REFERENCE_RECORD_RECORDING_SIZE = "recordingSize";
    static final String STREAM_CUT_REFERENCE_RECORD_PAIR_DELIMITER = "|";
    static final String STREAM_CUT_REFERENCE_RECORD_PAIR_DELIMITER_REGEX = "\\|";
    static final String STREAM_CUT_REFERENCE_RECORD_VALUE_DELIMITER = "-";

    private static final Map<String, Function<StreamCutReferenceRecord, String>> STREAM_CUT_REFERENCE_RECORD_FIELD_MAP =
            ImmutableMap.<String, Function<StreamCutReferenceRecord, String>>builder()
                    .put(STREAM_CUT_REFERENCE_RECORD_RECORDING_TIME, r -> String.valueOf(r.getRecordingTime()))
                    .put(STREAM_CUT_REFERENCE_RECORD_RECORDING_SIZE, r -> String.valueOf(r.getRecordingSize()))
                    .build();

    private static final Map<String, Function<RetentionSet, String>> RETENTION_SET_FIELD_MAP =
            ImmutableMap.<String, Function<RetentionSet, String>>builder()
                    .put(RETENTION_SET_RETENTION_RECORDS, r -> convertCollectionToString(r.getRetentionRecords(), sr -> {
                        StringBuilder streamCutStringBuilder = new StringBuilder();
                        STREAM_CUT_REFERENCE_RECORD_FIELD_MAP.forEach((name, f) ->
                                appendFieldWithCustomDelimiters(streamCutStringBuilder, name, f.apply(sr),
                                        STREAM_CUT_REFERENCE_RECORD_PAIR_DELIMITER, STREAM_CUT_REFERENCE_RECORD_VALUE_DELIMITER));
                        return streamCutStringBuilder.toString();
                    }))
                    .build();

    @Override
    public String getName() {
        return "RetentionSet";
    }

    @Override
    public ByteBuffer serialize(String value) {
        Map<String, String> data = parseStringData(value);
        List<StreamCutReferenceRecord> retentionRecords = new ArrayList<>(convertStringToCollection(getAndRemoveIfExists(data, RETENTION_SET_RETENTION_RECORDS), s -> {
            Map<String, String> streamCutDataMap = parseStringDataWithCustomDelimiters(s, STREAM_CUT_REFERENCE_RECORD_PAIR_DELIMITER_REGEX, STREAM_CUT_REFERENCE_RECORD_VALUE_DELIMITER);
            return StreamCutReferenceRecord.builder()
                    .recordingTime(Long.parseLong(getAndRemoveIfExists(streamCutDataMap, STREAM_CUT_REFERENCE_RECORD_RECORDING_TIME)))
                    .recordingSize(Long.parseLong(getAndRemoveIfExists(streamCutDataMap, STREAM_CUT_REFERENCE_RECORD_RECORDING_SIZE)))
                    .build();
        }));

        RetentionSet record = new RetentionSet(ImmutableList.copyOf(retentionRecords));
        return new ByteArraySegment(record.toBytes()).asByteBuffer();
    }

    @Override
    public String deserialize(ByteBuffer serializedValue) {
        return applyDeserializer(serializedValue, RetentionSet::fromBytes, RETENTION_SET_FIELD_MAP);
    }
}
