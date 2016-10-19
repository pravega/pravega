package com.emc.pravega.demo;

import com.emc.pravega.stream.Consumer;
import com.emc.pravega.stream.ConsumerConfig;
import com.emc.pravega.stream.Stream;
import com.emc.pravega.stream.impl.JavaSerializer;
import com.emc.pravega.stream.mock.MockStreamManager;

import lombok.Cleanup;

public class StartConsumer {

    public static void main(String[] args) throws Exception {
        @Cleanup
        MockStreamManager streamManager = new MockStreamManager(StartLocalService.SCOPE,
                "localhost",
                StartLocalService.PORT);
        Stream stream = streamManager.createStream(StartLocalService.STREAM_NAME, null);

        @Cleanup
        Consumer<String> consumer = stream
            .createConsumer(new JavaSerializer<>(),
                            new ConsumerConfig(),
                            streamManager.getInitialPosition(StartLocalService.STREAM_NAME),
                            null);
        for (int i = 0; i < 20; i++) {
            String event = consumer.getNextEvent(60000);
            System.err.println("Read event: " + event);
        }
        System.exit(0);
    }
}
