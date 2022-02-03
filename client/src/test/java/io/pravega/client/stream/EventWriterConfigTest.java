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
package io.pravega.client.stream;

import io.pravega.common.util.ByteArraySegment;
import lombok.Cleanup;
import org.apache.commons.io.output.ByteArrayOutputStream;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;

import static io.pravega.test.common.AssertExtensions.assertThrows;
import static org.junit.Assert.assertEquals;

public class EventWriterConfigTest {


    @Test
    public void testValidValues() throws IOException, ClassNotFoundException {
        EventWriterConfig config = EventWriterConfig.builder()
                .automaticallyNoteTime(true)
                .backoffMultiple(2)
                .enableConnectionPooling(false)
                .initialBackoffMillis(100)
                .maxBackoffMillis(1000)
                .retryAttempts(3)
                .transactionTimeoutTime(100000)
                .build();

        EventWriterConfig.EventWriterConfigSerializer serializer = new EventWriterConfig.EventWriterConfigSerializer();
        ByteArraySegment buff = serializer.serialize(config);
        EventWriterConfig result1 = serializer.deserialize(buff);

        ByteBuffer buffer = config.toBytes();
        EventWriterConfig result2 = EventWriterConfig.fromBytes(buffer);

        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        @Cleanup
        ObjectOutputStream oout = new ObjectOutputStream(bout);
        oout.writeObject(config);
        byte[] byteArray = bout.toByteArray();
        ObjectInputStream oin = new ObjectInputStream(new ByteArrayInputStream(byteArray));
        Object revision = oin.readObject();
        assertEquals(config, revision);

        assertEquals(true, result1.isAutomaticallyNoteTime());
        assertEquals(2, result1.getBackoffMultiple());
        assertEquals(false, result1.isEnableConnectionPooling());
        assertEquals(100, result1.getInitialBackoffMillis());
        assertEquals(1000, result1.getMaxBackoffMillis());
        assertEquals(3, result1.getRetryAttempts());
        assertEquals(100000, result1.getTransactionTimeoutTime());

        assertEquals(true, result2.isAutomaticallyNoteTime());
        assertEquals(2, result2.getBackoffMultiple());
        assertEquals(false, result2.isEnableConnectionPooling());
        assertEquals(100, result2.getInitialBackoffMillis());
        assertEquals(1000, result2.getMaxBackoffMillis());
        assertEquals(3, result2.getRetryAttempts());
        assertEquals(100000, result2.getTransactionTimeoutTime());
    }

    @Test
    public void testInvalidValues() {
        assertThrows(IllegalArgumentException.class, () -> EventWriterConfig.builder().backoffMultiple(-2).build());
        assertThrows(IllegalArgumentException.class, () -> EventWriterConfig.builder().initialBackoffMillis(-2).build());
        assertThrows(IllegalArgumentException.class, () -> EventWriterConfig.builder().maxBackoffMillis(-2).build());
        assertThrows(IllegalArgumentException.class, () -> EventWriterConfig.builder().retryAttempts(-2).build());
        assertThrows(IllegalArgumentException.class, () -> EventWriterConfig.builder().transactionTimeoutTime(-2).build());
    }

}
