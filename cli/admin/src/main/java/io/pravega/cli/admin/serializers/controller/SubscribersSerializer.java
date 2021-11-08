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
import io.pravega.controller.store.stream.records.Subscribers;

import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

public class SubscribersSerializer extends AbstractSerializer {

    public static final String SUBSCRIBERS_SUBSCRIBERS = "subscribers";

    private static final Map<String, Function<Subscribers, String>> SUBSCRIBERS_FIELD_MAP =
            ImmutableMap.<String, Function<Subscribers, String>>builder()
                    .put(SUBSCRIBERS_SUBSCRIBERS, r -> convertCollectionToString(r.getSubscribers(), s -> s))
                    .build();

    @Override
    public String getName() {
        return "Subscribers";
    }

    @Override
    public ByteBuffer serialize(String value) {
        Map<String, String> data = parseStringData(value);
        Set<String> subscribers = new HashSet<>(convertStringToCollection(getAndRemoveIfExists(data, SUBSCRIBERS_SUBSCRIBERS), s -> s));

        Subscribers record =  new Subscribers(ImmutableSet.copyOf(subscribers));
        return new ByteArraySegment(record.toBytes()).asByteBuffer();
    }

    @Override
    public String deserialize(ByteBuffer serializedValue) {
        return applyDeserializer(serializedValue, Subscribers::fromBytes, SUBSCRIBERS_FIELD_MAP);
    }
}
