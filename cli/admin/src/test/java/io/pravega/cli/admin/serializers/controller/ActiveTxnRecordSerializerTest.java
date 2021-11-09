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
import org.junit.Test;

import java.nio.ByteBuffer;

import static io.pravega.cli.admin.serializers.AbstractSerializer.appendField;
import static io.pravega.cli.admin.serializers.controller.ActiveTxnRecordSerializer.ACTIVE_TXN_RECORD_COMMIT_OFFSETS;
import static io.pravega.cli.admin.serializers.controller.ActiveTxnRecordSerializer.ACTIVE_TXN_RECORD_COMMIT_ORDER;
import static io.pravega.cli.admin.serializers.controller.ActiveTxnRecordSerializer.ACTIVE_TXN_RECORD_COMMIT_TIME;
import static io.pravega.cli.admin.serializers.controller.ActiveTxnRecordSerializer.ACTIVE_TXN_RECORD_LEASE_EXPIRY_TIME;
import static io.pravega.cli.admin.serializers.controller.ActiveTxnRecordSerializer.ACTIVE_TXN_RECORD_MAX_EXECUTION_EXPIRY_TIME;
import static io.pravega.cli.admin.serializers.controller.ActiveTxnRecordSerializer.ACTIVE_TXN_RECORD_TXN_STATUS;
import static io.pravega.cli.admin.serializers.controller.ActiveTxnRecordSerializer.ACTIVE_TXN_RECORD_TX_CREATION_TIMESTAMP;
import static io.pravega.cli.admin.serializers.controller.ActiveTxnRecordSerializer.ACTIVE_TXN_RECORD_WRITER_ID;
import static io.pravega.cli.admin.serializers.controller.ActiveTxnRecordSerializer.convertMapToString;
import static org.junit.Assert.assertEquals;

public class ActiveTxnRecordSerializerTest {

    @Test
    public void testActiveTxnRecordSerializer() {
        StringBuilder userGeneratedMetadataBuilder = new StringBuilder();
        appendField(userGeneratedMetadataBuilder, ACTIVE_TXN_RECORD_TX_CREATION_TIMESTAMP, String.valueOf(10L));
        appendField(userGeneratedMetadataBuilder, ACTIVE_TXN_RECORD_LEASE_EXPIRY_TIME, String.valueOf(100L));
        appendField(userGeneratedMetadataBuilder, ACTIVE_TXN_RECORD_MAX_EXECUTION_EXPIRY_TIME, String.valueOf(30L));
        appendField(userGeneratedMetadataBuilder, ACTIVE_TXN_RECORD_TXN_STATUS, "OPEN");
        appendField(userGeneratedMetadataBuilder, ACTIVE_TXN_RECORD_WRITER_ID, "123456");
        appendField(userGeneratedMetadataBuilder, ACTIVE_TXN_RECORD_COMMIT_TIME, String.valueOf(200L));
        appendField(userGeneratedMetadataBuilder, ACTIVE_TXN_RECORD_COMMIT_ORDER, String.valueOf(200L));
        appendField(userGeneratedMetadataBuilder, ACTIVE_TXN_RECORD_COMMIT_OFFSETS,
                convertMapToString(ImmutableMap.of(10L, 10L), String::valueOf, String::valueOf));

        String userString = userGeneratedMetadataBuilder.toString();
        ActiveTxnRecordSerializer serializer = new ActiveTxnRecordSerializer();
        ByteBuffer buf = serializer.serialize(userString);
        assertEquals(userString, serializer.deserialize(buf));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testActiveTxnRecordSerializerArgumentFailure() {
        StringBuilder userGeneratedMetadataBuilder = new StringBuilder();
        appendField(userGeneratedMetadataBuilder, ACTIVE_TXN_RECORD_TX_CREATION_TIMESTAMP, String.valueOf(10L));
        appendField(userGeneratedMetadataBuilder, ACTIVE_TXN_RECORD_LEASE_EXPIRY_TIME, String.valueOf(100L));
        appendField(userGeneratedMetadataBuilder, ACTIVE_TXN_RECORD_MAX_EXECUTION_EXPIRY_TIME, String.valueOf(30L));
        appendField(userGeneratedMetadataBuilder, ACTIVE_TXN_RECORD_WRITER_ID, "123456");
        appendField(userGeneratedMetadataBuilder, ACTIVE_TXN_RECORD_COMMIT_TIME, String.valueOf(200L));
        appendField(userGeneratedMetadataBuilder, ACTIVE_TXN_RECORD_COMMIT_ORDER, String.valueOf(200L));
        appendField(userGeneratedMetadataBuilder, ACTIVE_TXN_RECORD_COMMIT_OFFSETS,
                convertMapToString(ImmutableMap.of(10L, 10L), String::valueOf, String::valueOf));

        String userString = userGeneratedMetadataBuilder.toString();
        ActiveTxnRecordSerializer serializer = new ActiveTxnRecordSerializer();
        serializer.serialize(userString);
    }
}
