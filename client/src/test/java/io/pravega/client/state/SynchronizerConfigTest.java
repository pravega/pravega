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
        SynchronizerConfig s = serializer.deserialize(buff);

        assertEquals(true, synchConfig.getEventWriterConfig().isAutomaticallyNoteTime());
        assertEquals(2, synchConfig.getEventWriterConfig().getBackoffMultiple());
        assertEquals(false, synchConfig.getEventWriterConfig().isEnableConnectionPooling());
        assertEquals(100, synchConfig.getEventWriterConfig().getInitialBackoffMillis());
        assertEquals(1000, synchConfig.getEventWriterConfig().getMaxBackoffMillis());
        assertEquals(3, synchConfig.getEventWriterConfig().getRetryAttempts());
        assertEquals(100000, synchConfig.getEventWriterConfig().getTransactionTimeoutTime());
        assertEquals(1024, s.getReadBufferSize());

    }

}