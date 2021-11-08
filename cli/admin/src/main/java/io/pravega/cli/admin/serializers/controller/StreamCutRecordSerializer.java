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

import com.google.common.collect.ImmutableMap;
import io.pravega.cli.admin.serializers.AbstractSerializer;
import io.pravega.common.util.ByteArraySegment;
import io.pravega.controller.store.stream.records.StreamCutRecord;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.function.Function;

public class StreamCutRecordSerializer extends AbstractSerializer {

    public static final String STREAM_CUT_RECORD_RECORDING_TIME = "recordingTime";
    public static final String STREAM_CUT_RECORD_RECORDING_SIZE = "recordingSize";
    public static final String STREAM_CUT_RECORD_STREAM_CUT = "streamCut";

    private static final Map<String, Function<StreamCutRecord, String>> STREAM_CUT_RECORD_FIELD_MAP =
            ImmutableMap.<String, Function<StreamCutRecord, String>>builder()
                    .put(STREAM_CUT_RECORD_RECORDING_TIME, r -> String.valueOf(r.getRecordingTime()))
                    .put(STREAM_CUT_RECORD_RECORDING_SIZE, r -> String.valueOf(r.getRecordingSize()))
                    .put(STREAM_CUT_RECORD_STREAM_CUT, r -> convertMapToString(r.getStreamCut(), String::valueOf, String::valueOf))
                    .build();

    @Override
    public String getName() {
        return "StreamCutRecord";
    }

    @Override
    public ByteBuffer serialize(String value) {
        Map<String, String> data = parseStringData(value);
        Map<Long, Long> streamCut = convertStringToMap(getAndRemoveIfExists(data, STREAM_CUT_RECORD_STREAM_CUT),
                Long::parseLong, Long::parseLong, getName() + "." + STREAM_CUT_RECORD_STREAM_CUT);

        StreamCutRecord record =  new StreamCutRecord(Long.parseLong(getAndRemoveIfExists(data, STREAM_CUT_RECORD_RECORDING_TIME)),
                Long.parseLong(getAndRemoveIfExists(data, STREAM_CUT_RECORD_RECORDING_SIZE)),
                ImmutableMap.copyOf(streamCut));
        return new ByteArraySegment(record.toBytes()).asByteBuffer();
    }

    @Override
    public String deserialize(ByteBuffer serializedValue) {
        return applyDeserializer(serializedValue, StreamCutRecord::fromBytes, STREAM_CUT_RECORD_FIELD_MAP);
    }
}
