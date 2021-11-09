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

import org.junit.Test;

import java.nio.ByteBuffer;

import static io.pravega.cli.admin.serializers.AbstractSerializer.appendField;
import static io.pravega.cli.admin.serializers.controller.StateRecordSerializer.STATE_RECORD_STATE;
import static org.junit.Assert.assertEquals;

public class StateRecordSerializerTest {

    @Test
    public void testStateRecordSerializer() {
        StringBuilder userGeneratedMetadataBuilder = new StringBuilder();
        appendField(userGeneratedMetadataBuilder, STATE_RECORD_STATE, "ACTIVE");

        String userString = userGeneratedMetadataBuilder.toString();
        StateRecordSerializer serializer = new StateRecordSerializer();
        ByteBuffer buf = serializer.serialize(userString);
        assertEquals(userString, serializer.deserialize(buf));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testStateRecordSerializerArgumentFailure() {
        String userString = "";
        StateRecordSerializer serializer = new StateRecordSerializer();
        serializer.serialize(userString);
    }
}
