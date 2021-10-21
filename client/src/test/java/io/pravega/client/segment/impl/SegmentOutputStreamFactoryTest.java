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
package io.pravega.client.segment.impl;

import io.pravega.client.connection.impl.ConnectionPool;
import io.pravega.client.control.impl.Controller;
import io.pravega.client.security.auth.DelegationTokenProviderFactory;
import io.pravega.client.stream.EventWriterConfig;
import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static io.pravega.shared.NameUtils.isTransactionSegment;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class SegmentOutputStreamFactoryTest {

    @Mock
    private Controller controller;
    @Mock
    private ConnectionPool cp;
    @Mock
    private ScheduledExecutorService executor;

    private SegmentOutputStreamFactory factory;
    private final Segment segment = new Segment("scope", "stream", 0);
    private final UUID txId = UUID.randomUUID();

    @Before
    public void setup() {
        when(cp.getInternalExecutor()).thenReturn(executor);
        factory = new SegmentOutputStreamFactoryImpl(controller, cp);
    }

    @Test
    public void createOutputStreamForTransaction() {
        EventWriterConfig writerConfig = EventWriterConfig.builder().build();

        SegmentOutputStreamImpl segWriter = (SegmentOutputStreamImpl) factory.createOutputStreamForTransaction(segment, txId, writerConfig,
                DelegationTokenProviderFactory.createWithEmptyToken());
        assertTrue(isTransactionSegment(segWriter.getSegmentName()));
        assertEquals(writerConfig.isEnableConnectionPooling(), segWriter.isUseConnectionPooling());
    }

    @Test
    public void createOutputStreamForSegment() {
        EventWriterConfig writerConfig = EventWriterConfig.builder().enableConnectionPooling(false).build();
        SegmentOutputStreamImpl segWriter = (SegmentOutputStreamImpl) factory.createOutputStreamForSegment(segment, writerConfig,
                DelegationTokenProviderFactory.createWithEmptyToken());
        assertEquals(segment.getScopedName(), segWriter.getSegmentName());
        assertEquals(writerConfig.isEnableConnectionPooling(), segWriter.isUseConnectionPooling());
    }

    @Test
    public void createOutputStreamForSegmentAndconnect() {
        EventWriterConfig writerConfig = EventWriterConfig.builder().enableConnectionPooling(false).build();
        SegmentOutputStreamImpl segWriter = (SegmentOutputStreamImpl) factory.createOutputStreamForSegment(segment, s -> {
        }, writerConfig, DelegationTokenProviderFactory.createWithEmptyToken());
        assertEquals(segment.getScopedName(), segWriter.getSegmentName());
        assertEquals(writerConfig.isEnableConnectionPooling(), segWriter.isUseConnectionPooling());
    }
}