/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.demo;

import com.emc.pravega.service.contracts.StreamSegmentStore;
import com.emc.pravega.service.server.host.handler.PravegaConnectionListener;
import com.emc.pravega.service.server.store.ServiceBuilder;
import com.emc.pravega.service.server.store.ServiceBuilderConfig;
import com.emc.pravega.stream.EventStreamWriter;
import com.emc.pravega.stream.EventWriterConfig;
import com.emc.pravega.stream.Transaction;
import com.emc.pravega.stream.impl.JavaSerializer;
import com.emc.pravega.stream.mock.MockClientFactory;

import java.util.concurrent.CompletableFuture;

import org.apache.curator.test.TestingServer;

import lombok.Cleanup;

public class EndToEndTransactionTest {
    public static void main(String[] args) throws Exception {
        @Cleanup
        TestingServer zkTestServer = new TestingServer();
        ControllerWrapper controller = ControllerWrapper.getControllerWrapper(zkTestServer.getConnectString());

        ServiceBuilder serviceBuilder = ServiceBuilder.newInMemoryBuilder(ServiceBuilderConfig.getDefaultConfig());
        serviceBuilder.initialize().get();
        StreamSegmentStore store = serviceBuilder.createStreamSegmentService();
        @Cleanup
        PravegaConnectionListener server = new PravegaConnectionListener(false, StartLocalService.PORT, store);
        server.startListening();

        MockClientFactory clientFactory = new MockClientFactory(StartLocalService.SCOPE, controller);

        @Cleanup
        EventStreamWriter<String> producer = clientFactory.createEventWriter(StartLocalService.STREAM_NAME, new JavaSerializer<>(), EventWriterConfig.builder().build());
        Transaction<String> transaction = producer.beginTxn(60000);

        for (int i = 0; i < 1; i++) {
            String event = "\n Transactional Publish \n";
            System.err.println("Producing event: " + event);
            transaction.writeEvent("", event);
            transaction.flush();
            Thread.sleep(500);
        }

        Transaction<String> transaction2 = producer.beginTxn(60000);
        for (int i = 0; i < 1; i++) {
            String event = "\n Transactional Publish \n";
            System.err.println("Producing event: " + event);
            transaction2.writeEvent("", event);
            transaction2.flush();
            Thread.sleep(500);
        }

        CompletableFuture<Object> commit = CompletableFuture.supplyAsync(() -> {
            try {
                transaction.commit();
            } catch (Exception e) {
                e.printStackTrace();
            }
            return null;
        });

        CompletableFuture<Object> drop = CompletableFuture.supplyAsync(() -> {
            try {
                transaction2.abort();
            } catch (Exception e) {
                e.printStackTrace();
            }
            return null;
        });

        CompletableFuture.allOf(commit, drop).get();

        System.exit(0);
    }
}
