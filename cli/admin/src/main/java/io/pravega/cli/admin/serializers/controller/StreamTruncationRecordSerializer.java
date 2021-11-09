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
import com.google.common.collect.ImmutableSet;
import io.pravega.cli.admin.serializers.AbstractSerializer;
import io.pravega.common.util.ByteArraySegment;
import io.pravega.controller.store.stream.records.StreamSegmentRecord;
import io.pravega.controller.store.stream.records.StreamTruncationRecord;

import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

public class StreamTruncationRecordSerializer extends AbstractSerializer {

    static final String STREAM_TRUNCATION_RECORD_STREAM_CUT = "streamCut";
    static final String STREAM_TRUNCATION_RECORD_SPAN = "span";
    static final String STREAM_TRUNCATION_RECORD_DELETED_SEGMENTS = "deletedSegments";
    static final String STREAM_TRUNCATION_RECORD_TO_DELETE = "toDelete";
    static final String STREAM_TRUNCATION_RECORD_SIZE_TILL = "sizeTill";
    static final String STREAM_TRUNCATION_RECORD_UPDATING = "updating";

    private static final Map<String, Function<StreamTruncationRecord, String>> STREAM_TRUNCATION_RECORD_FIELD_MAP =
            ImmutableMap.<String, Function<StreamTruncationRecord, String>>builder()
                    .put(STREAM_TRUNCATION_RECORD_STREAM_CUT, r -> convertMapToString(r.getStreamCut(), String::valueOf, String::valueOf))
                    .put(STREAM_TRUNCATION_RECORD_SPAN, r -> convertMapToString(r.getSpan(), AbstractSerializer::convertStreamSegmentRecordToString, String::valueOf))
                    .put(STREAM_TRUNCATION_RECORD_DELETED_SEGMENTS, r -> convertCollectionToString(r.getDeletedSegments(), String::valueOf))
                    .put(STREAM_TRUNCATION_RECORD_TO_DELETE, r -> convertCollectionToString(r.getToDelete(), String::valueOf))
                    .put(STREAM_TRUNCATION_RECORD_SIZE_TILL, r -> String.valueOf(r.getSizeTill()))
                    .put(STREAM_TRUNCATION_RECORD_UPDATING, r -> String.valueOf(r.isUpdating()))
                    .build();

    @Override
    public String getName() {
        return "StreamTruncationRecord";
    }

    @Override
    public ByteBuffer serialize(String value) {
        Map<String, String> data = parseStringData(value);
        Map<Long, Long> streamCut = convertStringToMap(getAndRemoveIfExists(data, STREAM_TRUNCATION_RECORD_STREAM_CUT),
                Long::parseLong, Long::parseLong, getName() + STREAM_TRUNCATION_RECORD_STREAM_CUT);
        Map<StreamSegmentRecord, Integer> span = convertStringToMap(getAndRemoveIfExists(data, STREAM_TRUNCATION_RECORD_SPAN),
                AbstractSerializer::convertStringToStreamSegmentRecord, Integer::parseInt, getName() + "." + STREAM_TRUNCATION_RECORD_SPAN);
        Set<Long> deletedSegments = new HashSet<>(convertStringToCollection(getAndRemoveIfExists(data, STREAM_TRUNCATION_RECORD_DELETED_SEGMENTS), Long::parseLong));
        Set<Long> toDelete = new HashSet<>(convertStringToCollection(getAndRemoveIfExists(data, STREAM_TRUNCATION_RECORD_TO_DELETE), Long::parseLong));

        StreamTruncationRecord record = new StreamTruncationRecord(ImmutableMap.copyOf(streamCut), ImmutableMap.copyOf(span),
                ImmutableSet.copyOf(deletedSegments), ImmutableSet.copyOf(toDelete),
                Long.parseLong(getAndRemoveIfExists(data, STREAM_TRUNCATION_RECORD_SIZE_TILL)),
                Boolean.parseBoolean(getAndRemoveIfExists(data, STREAM_TRUNCATION_RECORD_UPDATING)));
        return new ByteArraySegment(record.toBytes()).asByteBuffer();
    }

    @Override
    public String deserialize(ByteBuffer serializedValue) {
        return applyDeserializer(serializedValue, StreamTruncationRecord::fromBytes, STREAM_TRUNCATION_RECORD_FIELD_MAP);
    }
}
