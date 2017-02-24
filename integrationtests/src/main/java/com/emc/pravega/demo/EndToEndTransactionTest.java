/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package com.emc.pravega.demo;

import com.emc.pravega.controller.stream.api.v1.CreateStreamStatus;
import com.emc.pravega.service.contracts.StreamSegmentStore;
import com.emc.pravega.service.server.host.handler.PravegaConnectionListener;
import com.emc.pravega.service.server.store.ServiceBuilder;
import com.emc.pravega.service.server.store.ServiceBuilderConfig;
import com.emc.pravega.stream.EventStreamWriter;
import com.emc.pravega.stream.EventWriterConfig;
import com.emc.pravega.stream.ScalingPolicy;
import com.emc.pravega.stream.StreamConfiguration;
import com.emc.pravega.stream.Transaction;
import com.emc.pravega.stream.impl.Controller;
import com.emc.pravega.stream.impl.JavaSerializer;
import com.emc.pravega.stream.mock.MockClientFactory;
import lombok.Cleanup;
import org.apache.curator.test.TestingServer;

import java.util.concurrent.CompletableFuture;

import static org.junit.Assert.assertTrue;

public class EndToEndTransactionTest {
    public static void main(String[] args) throws Exception {
        @Cleanup
        TestingServer zkTestServer = new TestingServer();

        ServiceBuilder serviceBuilder = ServiceBuilder.newInMemoryBuilder(ServiceBuilderConfig.getDefaultConfig());
        serviceBuilder.initialize().get();
        StreamSegmentStore store = serviceBuilder.createStreamSegmentService();
        @Cleanup
        PravegaConnectionListener server = new PravegaConnectionListener(false, 12345, store);
        server.startListening();

        ControllerWrapper controllerWrapper = new ControllerWrapper(zkTestServer.getConnectString(), true);
        Controller controller = controllerWrapper.getController();

        final String testScope = "testScope";
        final String testStream = "testStream";
        controllerWrapper.getControllerService().createScope("testScope").get();

        ScalingPolicy policy = new ScalingPolicy(ScalingPolicy.Type.FIXED_NUM_SEGMENTS, 0, 0, 5);
        StreamConfiguration streamConfig =
                StreamConfiguration.builder()
                        .scope(testScope)
                        .streamName(testStream)
                        .scalingPolicy(policy)
                        .build();

        CompletableFuture<CreateStreamStatus> futureStatus = controller.createStream(streamConfig);
        CreateStreamStatus status = futureStatus.join();

        if (status != CreateStreamStatus.SUCCESS) {
            System.err.println("FAILURE: Error creating test stream");
            return;
        }

        MockClientFactory clientFactory = new MockClientFactory(testScope, controller);

        @Cleanup
        EventStreamWriter<String> producer = clientFactory.createEventWriter(
                testStream,
                new JavaSerializer<>(),
                EventWriterConfig.builder().build());

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

        Transaction.Status txnStatus = transaction.checkStatus();
        assertTrue(txnStatus == Transaction.Status.COMMITTING || txnStatus == Transaction.Status.COMMITTED);
        System.err.println("SUCCESS: successful in committing transaction. Transaction status=" + txnStatus);

        Transaction.Status txn2Status = transaction2.checkStatus();
        assertTrue(txn2Status == Transaction.Status.ABORTING || txn2Status == Transaction.Status.ABORTED);
        System.err.println("SUCCESS: successful in dropping transaction. Transaction status=" + txn2Status);

        Thread.sleep(2000);

        txnStatus = transaction.checkStatus();
        assertTrue(txnStatus == Transaction.Status.COMMITTED);
        System.err.println("SUCCESS: successfully committed transaction. Transaction status=" + txnStatus);

        txn2Status = transaction2.checkStatus();
        assertTrue(txn2Status == Transaction.Status.ABORTED);
        System.err.println("SUCCESS: successfully aborted transaction. Transaction status=" + txn2Status);
    }
}
