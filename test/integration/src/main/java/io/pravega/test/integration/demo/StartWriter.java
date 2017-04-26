/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package io.pravega.test.integration.demo;

import io.pravega.stream.EventStreamWriter;
import io.pravega.stream.EventWriterConfig;
import io.pravega.stream.Transaction;
import io.pravega.stream.impl.JavaSerializer;
import io.pravega.stream.mock.MockStreamManager;

import lombok.Cleanup;

public class StartWriter {

    public static void main(String[] args) throws Exception {
        @Cleanup
        MockStreamManager streamManager = new MockStreamManager(StartLocalService.SCOPE,
                                                                "localhost",
                                                                StartLocalService.PORT);
        streamManager.createScope(StartLocalService.SCOPE);
        streamManager.createStream(StartLocalService.SCOPE, StartLocalService.STREAM_NAME, null);
        @Cleanup
        EventStreamWriter<String> writer = streamManager.getClientFactory().createEventWriter(StartLocalService.STREAM_NAME,
                                                                new JavaSerializer<>(),
                                                                EventWriterConfig.builder().build());
        Transaction<String> transaction = writer.beginTxn(60000, 60000, 60000);

        for (int i = 0; i < 10; i++) {
            String event = "\n Transactional write \n";
            System.err.println("Writing event: " + event);
            transaction.writeEvent(event);
            transaction.flush();
            Thread.sleep(500);
        }
        for (int i = 0; i < 10; i++) {
            String event = "\n Non-transactional Publish \n";
            System.err.println("Writing event: " + event);
            writer.writeEvent(event);
            writer.flush();
            Thread.sleep(500);
        }
        transaction.commit();
        System.exit(0);
    }
}
