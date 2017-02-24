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
import com.emc.pravega.stream.ReinitializationRequiredException;
import com.emc.pravega.stream.ScalingPolicy;
import com.emc.pravega.stream.Sequence;
import com.emc.pravega.stream.StreamConfiguration;
import com.emc.pravega.stream.impl.JavaSerializer;
import com.emc.pravega.stream.mock.MockClientFactory;
import com.emc.pravega.stream.mock.MockStreamManager;
import com.emc.pravega.testcommon.TestUtils;
import com.google.common.collect.Lists;

import lombok.Cleanup;
import lombok.Data;

import org.junit.Test;

public class ReaderGroupTest {

    private static final String SCOPE = "scope";
    private static final String STREAM_NAME = "streamName";
    private static final String READER_GROUP = "ExampleReaderGroup";

    @Data
    private class WriterThread implements Runnable {
        private final int eventsToWrite;
        private final ClientFactory clientFactory;

        public void run() {
            @Cleanup
            EventStreamWriter<String> writer = clientFactory.createEventWriter(STREAM_NAME,
                                                                               new JavaSerializer<>(),
                                                                               EventWriterConfig.builder().build());
            for (int i = 0; i < eventsToWrite; i++) {
                System.err.println("Writing event: " + i);
                writer.writeEvent(Integer.toString(i), " Event " + i);
            }
            writer.flush();
        }
    }

    @Data
    private class ReaderThread implements Runnable {
        private static final int READ_TIMEOUT = 60000;
        private final int eventsToRead;
        private final String readerId;
        private final ClientFactory clientFactory;

        public void run() {
            try {
                @Cleanup
                EventStreamReader<String> reader = clientFactory.createReader(readerId,
                                                                              READER_GROUP,
                                                                              new JavaSerializer<>(),
                                                                              ReaderConfig.builder().build());
                String event = null;
                for (int i=0; i < eventsToRead; i++) {
                    event = reader.readNextEvent(READ_TIMEOUT).getEvent();
                    System.err.println("Reader " + readerId +" Iteration "+ i + " Read event: " + event);
                }
                System.err.println("Reader" + readerId + " Read Done, event: " + event);
            } catch (ReinitializationRequiredException e1) {
                // TODO Auto-generated catch block
                e1.printStackTrace();
            }
        }
    }

    @Test(timeout = 20000)
    public void testEventHandoff() throws Exception {
        String endpoint = "localhost";
        int port = TestUtils.randomPort();

        ServiceBuilder serviceBuilder = ServiceBuilder.newInMemoryBuilder(ServiceBuilderConfig.getDefaultConfig());
        serviceBuilder.initialize().get();
        StreamSegmentStore store = serviceBuilder.createStreamSegmentService();
        @Cleanup
        PravegaConnectionListener server = new PravegaConnectionListener(false, port, store);
        server.startListening();

        @Cleanup
        MockStreamManager streamManager = new MockStreamManager(SCOPE, endpoint, port);
        streamManager.createStream(STREAM_NAME, StreamConfiguration.builder()
                                                                   .scope(SCOPE)
                                                                   .streamName(STREAM_NAME)
                                                                   .scalingPolicy(ScalingPolicy.fixed(2))
                                                                   .build());
        MockClientFactory clientFactory = streamManager.getClientFactory();

        ReaderGroupConfig groupConfig = ReaderGroupConfig.builder().startingPosition(Sequence.MIN_VALUE).build();
        streamManager.createReaderGroup(READER_GROUP, groupConfig, Lists.newArrayList(STREAM_NAME));

        new WriterThread(800, clientFactory).run();
        Thread reader1Thread = new Thread(new ReaderThread(200, "Reader1", clientFactory));
        Thread reader2Thread = new Thread(new ReaderThread(600, "Reader2", clientFactory));
        reader1Thread.start();
        reader2Thread.start();
        reader1Thread.join();
        reader2Thread.join();

    }
}
