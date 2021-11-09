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
import io.pravega.controller.store.stream.records.StreamSubscriber;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.function.Function;

public class StreamSubscriberSerializer extends AbstractSerializer {

    static final String STREAM_SUBSCRIBER_SUBSCRIBER = "subscriber";
    static final String STREAM_SUBSCRIBER_GENERATION = "generation";
    static final String STREAM_SUBSCRIBER_TRUNCATION_STREAM_CUT = "truncationStreamCut";
    static final String STREAM_SUBSCRIBER_UPDATE_TIME = "updateTime";

    private static final Map<String, Function<StreamSubscriber, String>> STREAM_SUBSCRIBER_FIELD_MAP =
            ImmutableMap.<String, Function<StreamSubscriber, String>>builder()
                    .put(STREAM_SUBSCRIBER_SUBSCRIBER, StreamSubscriber::getSubscriber)
                    .put(STREAM_SUBSCRIBER_GENERATION, r -> String.valueOf(r.getGeneration()))
                    .put(STREAM_SUBSCRIBER_TRUNCATION_STREAM_CUT, r -> convertMapToString(r.getTruncationStreamCut(), String::valueOf, String::valueOf))
                    .put(STREAM_SUBSCRIBER_UPDATE_TIME, r -> String.valueOf(r.getUpdateTime()))
                    .build();

    @Override
    public String getName() {
        return "StreamSubscriber";
    }

    @Override
    public ByteBuffer serialize(String value) {
        Map<String, String> data = parseStringData(value);
        Map<Long, Long> truncationStreamCut = convertStringToMap(getAndRemoveIfExists(data, STREAM_SUBSCRIBER_TRUNCATION_STREAM_CUT),
                Long::parseLong, Long::parseLong, getName() + "." + STREAM_SUBSCRIBER_TRUNCATION_STREAM_CUT);

        StreamSubscriber record =  new StreamSubscriber(getAndRemoveIfExists(data, STREAM_SUBSCRIBER_SUBSCRIBER),
                Long.parseLong(getAndRemoveIfExists(data, STREAM_SUBSCRIBER_GENERATION)), ImmutableMap.copyOf(truncationStreamCut),
                Long.parseLong(getAndRemoveIfExists(data, STREAM_SUBSCRIBER_UPDATE_TIME)));
        return new ByteArraySegment(record.toBytes()).asByteBuffer();
    }

    @Override
    public String deserialize(ByteBuffer serializedValue) {
        return applyDeserializer(serializedValue, StreamSubscriber::fromBytes, STREAM_SUBSCRIBER_FIELD_MAP);
    }
}
