/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.test.integration;

import io.pravega.client.segment.impl.NoSuchSegmentException;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.impl.JavaSerializer;
import io.pravega.client.stream.mock.MockClientFactory;
import io.pravega.client.stream.mock.MockStreamManager;
import io.pravega.controller.util.Config;
import io.pravega.segmentstore.contracts.StreamSegmentStore;
import io.pravega.segmentstore.server.host.handler.PravegaConnectionListener;
import io.pravega.segmentstore.server.store.ServiceBuilder;
import io.pravega.segmentstore.server.store.ServiceBuilderConfig;
import io.pravega.test.common.AssertExtensions;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;

@Slf4j
public class ReadFromDeletedStreamTest {
    static final StreamConfiguration CONFIG = StreamConfiguration.builder()
                                                                 .scope("test")
                                                                 .streamName("test")
                                                                 .scalingPolicy(ScalingPolicy.fixed(1))
                                                                 .build();

    @Test(timeout = 30000)
    public void testDeletedAndRecreatedStream() throws Exception {
        @Cleanup
        MockStreamManager streamManager = new MockStreamManager("test", "localhost", Config.SERVICE_PORT);

        ServiceBuilder serviceBuilder = ServiceBuilder.newInMemoryBuilder(ServiceBuilderConfig.getDefaultConfig());
        serviceBuilder.initialize();
        StreamSegmentStore store = serviceBuilder.createStreamSegmentService();

        @Cleanup
        PravegaConnectionListener server = new PravegaConnectionListener(false, "localhost", 12345, store, null, null, null, null, true);
        server.startListening();

        streamManager.createScope("test");
        streamManager.createStream("test", "test", CONFIG);

        @Cleanup
        MockClientFactory clientFactory = streamManager.getClientFactory();
        // Mocking pravega service by putting scale up and scale down requests for the stream
        @Cleanup
        EventStreamWriter<String> test = clientFactory.createEventWriter("test", new JavaSerializer<>(),
                                                                         EventWriterConfig.builder().build());
        test.writeEvent("0", "foo").get();
        streamManager.deleteStream("test", "test");
        AssertExtensions.assertThrows(NoSuchSegmentException.class, () -> test.writeEvent("0", "foo").get());
    }
}