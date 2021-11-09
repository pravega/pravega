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
import static io.pravega.cli.admin.serializers.controller.CompletedTxnRecordSerializer.COMPLETED_TXN_RECORD_COMPLETE_TIME;
import static io.pravega.cli.admin.serializers.controller.CompletedTxnRecordSerializer.COMPLETED_TXN_RECORD_COMPLETION_STATUS;
import static org.junit.Assert.assertEquals;

public class CompletedTxnRecordSerializerTest {

    @Test
    public void testCompletedTxnRecordSerializer() {
        StringBuilder userGeneratedMetadataBuilder = new StringBuilder();
        appendField(userGeneratedMetadataBuilder, COMPLETED_TXN_RECORD_COMPLETE_TIME, String.valueOf(10));
        appendField(userGeneratedMetadataBuilder, COMPLETED_TXN_RECORD_COMPLETION_STATUS, "COMMITTED");

        String userString = userGeneratedMetadataBuilder.toString();
        CompletedTxnRecordSerializer serializer = new CompletedTxnRecordSerializer();
        ByteBuffer buf = serializer.serialize(userString);
        assertEquals(userString, serializer.deserialize(buf));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCompletedTxnRecordSerializerArgumentFailure() {
        StringBuilder userGeneratedMetadataBuilder = new StringBuilder();
        appendField(userGeneratedMetadataBuilder, COMPLETED_TXN_RECORD_COMPLETION_STATUS, "COMMITTED");

        String userString = userGeneratedMetadataBuilder.toString();
        CompletedTxnRecordSerializer serializer = new CompletedTxnRecordSerializer();
        serializer.serialize(userString);
    }
}
