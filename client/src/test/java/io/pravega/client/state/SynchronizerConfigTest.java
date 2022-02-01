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
package io.pravega.client.state;

import io.pravega.common.util.ByteArraySegment;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import io.pravega.client.stream.EventWriterConfig;

import java.io.IOException;
import java.nio.ByteBuffer;

public class SynchronizerConfigTest {

    @Test
    public void testValidValues() throws IOException {
        EventWriterConfig eventConfig = EventWriterConfig.builder()
                .automaticallyNoteTime(true)
                .backoffMultiple(2)
                .enableConnectionPooling(false)
                .initialBackoffMillis(100)
                .maxBackoffMillis(1000)
                .retryAttempts(3)
                .transactionTimeoutTime(100000)
                .build();
        SynchronizerConfig synchConfig = SynchronizerConfig.builder()
                .readBufferSize(1024)
                .eventWriterConfig(eventConfig)
                .build();

        SynchronizerConfig.SynchronizerConfigSerializer serializer = new SynchronizerConfig.SynchronizerConfigSerializer();
        ByteArraySegment buff = serializer.serialize(synchConfig);
        SynchronizerConfig result1 = serializer.deserialize(buff);

        ByteBuffer buffer = synchConfig.toBytes();
        SynchronizerConfig result2 = SynchronizerConfig.fromBytes(buffer);

        assertEquals(true, result1.getEventWriterConfig().isAutomaticallyNoteTime());
        assertEquals(2, result1.getEventWriterConfig().getBackoffMultiple());
        assertEquals(false, result1.getEventWriterConfig().isEnableConnectionPooling());
        assertEquals(100, result1.getEventWriterConfig().getInitialBackoffMillis());
        assertEquals(1000, result1.getEventWriterConfig().getMaxBackoffMillis());
        assertEquals(3, result1.getEventWriterConfig().getRetryAttempts());
        assertEquals(100000, result1.getEventWriterConfig().getTransactionTimeoutTime());
        assertEquals(1024, result1.getReadBufferSize());

        assertEquals(true, result2.getEventWriterConfig().isAutomaticallyNoteTime());
        assertEquals(2, result2.getEventWriterConfig().getBackoffMultiple());
        assertEquals(false, result2.getEventWriterConfig().isEnableConnectionPooling());
        assertEquals(100, result2.getEventWriterConfig().getInitialBackoffMillis());
        assertEquals(1000, result2.getEventWriterConfig().getMaxBackoffMillis());
        assertEquals(3, result2.getEventWriterConfig().getRetryAttempts());
        assertEquals(100000, result2.getEventWriterConfig().getTransactionTimeoutTime());
        assertEquals(1024, result2.getReadBufferSize());

    }

}