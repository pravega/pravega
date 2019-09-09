/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.segment.impl;

import io.pravega.client.netty.impl.ConnectionFactory;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.impl.Controller;
import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static io.pravega.shared.segment.StreamSegmentNameUtils.isTransactionSegment;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class SegmentOutputStreamFactoryTest {

    @Mock
    private Controller controller;
    @Mock
    private ConnectionFactory cf;
    @Mock
    private ScheduledExecutorService executor;

    private SegmentOutputStreamFactory factory;
    private final Segment segment = new Segment("scope", "stream", 0);
    private final UUID txId = UUID.randomUUID();

    @Before
    public void setup() {
        when(cf.getInternalExecutor()).thenReturn(executor);
        factory = new SegmentOutputStreamFactoryImpl(controller, cf);
    }

    @Test
    public void createOutputStreamForTransaction() {
        EventWriterConfig writerConfig = EventWriterConfig.builder().build();

        SegmentOutputStreamImpl segWriter = (SegmentOutputStreamImpl) factory.createOutputStreamForTransaction(segment, txId, writerConfig, "");
        assertTrue(isTransactionSegment(segWriter.getSegmentName()));
        assertEquals(writerConfig.isEnableConnectionPooling(), segWriter.isUseConnectionPooling());
    }

    @Test
    public void createOutputStreamForSegment() {
        EventWriterConfig writerConfig = EventWriterConfig.builder().enableConnectionPooling(false).build();
        SegmentOutputStreamImpl segWriter = (SegmentOutputStreamImpl) factory.createOutputStreamForSegment(segment, writerConfig, "");
        assertEquals(segment.getScopedName(), segWriter.getSegmentName());
        assertEquals(writerConfig.isEnableConnectionPooling(), segWriter.isUseConnectionPooling());
    }

    @Test
    public void createOutputStreamForSegmentAndconnect() {
        EventWriterConfig writerConfig = EventWriterConfig.builder().enableConnectionPooling(false).build();
        SegmentOutputStreamImpl segWriter = (SegmentOutputStreamImpl) factory.createOutputStreamForSegment(segment, s -> {
        }, writerConfig, "");
        assertEquals(segment.getScopedName(), segWriter.getSegmentName());
        assertEquals(writerConfig.isEnableConnectionPooling(), segWriter.isUseConnectionPooling());
    }
}