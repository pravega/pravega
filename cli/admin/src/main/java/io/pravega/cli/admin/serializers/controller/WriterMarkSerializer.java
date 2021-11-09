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
import io.pravega.controller.store.stream.records.WriterMark;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.function.Function;

public class WriterMarkSerializer extends AbstractSerializer {

    static final String WRITER_MARK_TIMESTAMP = "timestamp";
    static final String WRITER_MARK_POSITION = "position";
    static final String WRITER_MARK_IS_ALIVE = "isAlive";

    private static final Map<String, Function<WriterMark, String>> WRITER_MARK_FIELD_MAP =
            ImmutableMap.<String, Function<WriterMark, String>>builder()
                    .put(WRITER_MARK_TIMESTAMP, r -> String.valueOf(r.getTimestamp()))
                    .put(WRITER_MARK_POSITION, r -> convertMapToString(r.getPosition(), String::valueOf, String::valueOf))
                    .put(WRITER_MARK_IS_ALIVE, r -> String.valueOf(r.isAlive()))
                    .build();

    @Override
    public String getName() {
        return "WriterMark";
    }

    @Override
    public ByteBuffer serialize(String value) {
        Map<String, String> data = parseStringData(value);
        Map<Long, Long> position = convertStringToMap(getAndRemoveIfExists(data, WRITER_MARK_POSITION),
                Long::parseLong, Long::parseLong, getName() + "." + WRITER_MARK_POSITION);

        WriterMark record = new WriterMark(Long.parseLong(getAndRemoveIfExists(data, WRITER_MARK_TIMESTAMP)),
                ImmutableMap.copyOf(position), Boolean.parseBoolean(getAndRemoveIfExists(data, WRITER_MARK_IS_ALIVE)));
        return new ByteArraySegment(record.toBytes()).asByteBuffer();
    }

    @Override
    public String deserialize(ByteBuffer serializedValue) {
        return applyDeserializer(serializedValue, WriterMark::fromBytes, WRITER_MARK_FIELD_MAP);
    }
}
