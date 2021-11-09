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
import io.pravega.controller.store.stream.State;
import io.pravega.controller.store.stream.records.StateRecord;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.function.Function;

public class StateRecordSerializer extends AbstractSerializer {

    static final String STATE_RECORD_STATE = "state";

    private static final Map<String, Function<StateRecord, String>> STATE_RECORD_FIELD_MAP =
            ImmutableMap.<String, Function<StateRecord, String>>builder()
                    .put(STATE_RECORD_STATE, r -> r.getState().toString())
                    .build();

    @Override
    public String getName() {
        return "StateRecord";
    }

    @Override
    public ByteBuffer serialize(String value) {
        Map<String, String> data = parseStringData(value);
        StateRecord record = new StateRecord(State.valueOf(getAndRemoveIfExists(data, STATE_RECORD_STATE).toUpperCase()));
        return new ByteArraySegment(record.toBytes()).asByteBuffer();
    }

    @Override
    public String deserialize(ByteBuffer serializedValue) {
        return applyDeserializer(serializedValue, StateRecord::fromBytes, STATE_RECORD_FIELD_MAP);
    }
}
