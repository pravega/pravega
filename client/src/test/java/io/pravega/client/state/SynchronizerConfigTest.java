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

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import io.pravega.client.stream.EventWriterConfig;

public class SynchronizerConfigTest {

    @Test
    public void testValidValues() {
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
                .eventWriterConfig(eventConfig)
                .build();

        assertEquals(true, synchConfig.eventWriterConfig.isAutomaticallyNoteTime());
        assertEquals(2, synchConfig.eventWriterConfig.getBackoffMultiple());
        assertEquals(false, synchConfig.eventWriterConfig.isEnableConnectionPooling());
        assertEquals(100, synchConfig.eventWriterConfig.getInitialBackoffMillis());
        assertEquals(1000, synchConfig.eventWriterConfig.getMaxBackoffMillis());
        assertEquals(3, synchConfig.eventWriterConfig.getRetryAttempts());
        assertEquals(100000, synchConfig.eventWriterConfig.getTransactionTimeoutTime());
    }

}