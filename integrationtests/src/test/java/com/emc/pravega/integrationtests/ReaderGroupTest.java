/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.integrationtests;

import com.emc.pravega.ClientFactory;
import com.emc.pravega.service.contracts.StreamSegmentStore;
import com.emc.pravega.service.server.host.handler.PravegaConnectionListener;
import com.emc.pravega.service.server.store.ServiceBuilder;
import com.emc.pravega.service.server.store.ServiceBuilderConfig;
import com.emc.pravega.stream.EventStreamReader;
import com.emc.pravega.stream.EventStreamWriter;
import com.emc.pravega.stream.EventWriterConfig;
import com.emc.pravega.stream.ReaderConfig;
import com.emc.pravega.stream.ReaderGroupConfig;
import com.emc.pravega.stream.ScalingPolicy;
import com.emc.pravega.stream.Sequence;
import com.emc.pravega.stream.StreamConfiguration;
import com.emc.pravega.stream.impl.JavaSerializer;
import com.emc.pravega.stream.mock.MockClientFactory;
import com.emc.pravega.stream.mock.MockStreamManager;
import com.emc.pravega.testcommon.TestUtils;

import java.util.Collections;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;

import lombok.Cleanup;
import lombok.Data;

import org.junit.Test;

public class ReaderGroupTest {

    private static final String SCOPE = "scope";
    private static final String STREAM_NAME = "streamName";
    private static final String READER_GROUP = "ExampleReaderGroup";

    @Data
    private static class ReaderThread implements Runnable {
        private static final int READ_TIMEOUT = 60000;
        private final int eventsToRead;
        private final String readerId;
        private final ClientFactory clientFactory;
        private final AtomicReference<Exception> exception = new AtomicReference<>(null);

        @Override
        public void run() {
            try {
                @Cleanup
                EventStreamReader<String> reader = clientFactory.createReader(readerId,
                                                                              READER_GROUP,
                                                                              new JavaSerializer<>(),
                                                                              ReaderConfig.builder().build());
                String event = null;
                for (int i = 0; i < eventsToRead; i++) {
                    event = reader.readNextEvent(READ_TIMEOUT).getEvent();
                    if (event == null) {
                        exception.set(new IllegalStateException("Read timedOut unexpectedly"));
                    }
                }
            } catch (Exception e) {
                exception.set(e);
            }
        }
    }

    @Test(timeout = 20000)
    public void testEventHandoff() throws Exception {
        String endpoint = "localhost";
        int port = TestUtils.randomPort();
        @Cleanup
        ServiceBuilder serviceBuilder = ServiceBuilder.newInMemoryBuilder(ServiceBuilderConfig.getDefaultConfig());
        serviceBuilder.initialize().get();
        StreamSegmentStore store = serviceBuilder.createStreamSegmentService();
        @Cleanup
        PravegaConnectionListener server = new PravegaConnectionListener(false, port, store);
        server.startListening();

        @Cleanup
        MockStreamManager streamManager = new MockStreamManager(SCOPE, endpoint, port);
        streamManager.createScope();
        streamManager.createStream(STREAM_NAME, StreamConfiguration.builder()
                                                                   .scope(SCOPE)
                                                                   .streamName(STREAM_NAME)
                                                                   .scalingPolicy(ScalingPolicy.fixed(2))
                                                                   .build());
        @Cleanup
        MockClientFactory clientFactory = streamManager.getClientFactory();

        ReaderGroupConfig groupConfig = ReaderGroupConfig.builder().startingPosition(Sequence.MIN_VALUE).build();
        streamManager.createReaderGroup(READER_GROUP, groupConfig, Collections.singleton(STREAM_NAME));

        writeEvents(100, clientFactory);
        ReaderThread r1 = new ReaderThread(20, "Reader1", clientFactory);
        ReaderThread r2 = new ReaderThread(80, "Reader2", clientFactory);
        Thread reader1Thread = new Thread(r1);
        Thread reader2Thread = new Thread(r2);
        reader1Thread.start();
        reader2Thread.start();
        reader1Thread.join();
        reader2Thread.join();
        if (r1.exception.get() != null) {
            throw r1.exception.get();
        }
        if (r2.exception.get() != null) {
            throw r2.exception.get();
        }
    }
    
    @Test
    public void testMultiSegmentsPerReader() throws InterruptedException, ExecutionException {
        String endpoint = "localhost";
        int port = TestUtils.randomPort();
        @Cleanup
        ServiceBuilder serviceBuilder = ServiceBuilder.newInMemoryBuilder(ServiceBuilderConfig.getDefaultConfig());
        serviceBuilder.initialize().get();
        StreamSegmentStore store = serviceBuilder.createStreamSegmentService();
        @Cleanup
        PravegaConnectionListener server = new PravegaConnectionListener(false, port, store);
        server.startListening();

        @Cleanup
        MockStreamManager streamManager = new MockStreamManager(SCOPE, endpoint, port);
        streamManager.createScope();
        streamManager.createStream(STREAM_NAME, StreamConfiguration.builder()
                                                                   .scope(SCOPE)
                                                                   .streamName(STREAM_NAME)
                                                                   .scalingPolicy(ScalingPolicy.fixed(2))
                                                                   .build());
        @Cleanup
        MockClientFactory clientFactory = streamManager.getClientFactory();

        ReaderGroupConfig groupConfig = ReaderGroupConfig.builder().startingPosition(Sequence.MIN_VALUE).build();
        streamManager.createReaderGroup(READER_GROUP, groupConfig, Collections.singleton(STREAM_NAME));

        writeEvents(100, clientFactory);
        new ReaderThread(100, "Reader", clientFactory).run();
    }
    
    public void writeEvents(int eventsToWrite, ClientFactory clientFactory) {
        @Cleanup
        EventStreamWriter<String> writer = clientFactory.createEventWriter(STREAM_NAME,
                                                                           new JavaSerializer<>(),
                                                                           EventWriterConfig.builder().build());
        for (int i = 0; i < eventsToWrite; i++) {
            writer.writeEvent(Integer.toString(i), " Event " + i);
        }
        writer.flush();
    }
}
