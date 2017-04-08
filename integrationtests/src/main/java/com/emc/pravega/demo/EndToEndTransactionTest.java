/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package com.emc.pravega.demo;

import com.emc.pravega.controller.util.Config;
import com.emc.pravega.service.contracts.StreamSegmentStore;
import com.emc.pravega.service.server.host.handler.PravegaConnectionListener;
import com.emc.pravega.service.server.store.ServiceBuilder;
import com.emc.pravega.service.server.store.ServiceBuilderConfig;
import com.emc.pravega.stream.EventStreamWriter;
import com.emc.pravega.stream.EventWriterConfig;
import com.emc.pravega.stream.PingFailedException;
import com.emc.pravega.stream.ScalingPolicy;
import com.emc.pravega.stream.StreamConfiguration;
import com.emc.pravega.stream.Transaction;
import com.emc.pravega.stream.impl.Controller;
import com.emc.pravega.stream.impl.JavaSerializer;
import com.emc.pravega.stream.mock.MockClientFactory;
import java.util.concurrent.CompletableFuture;

import com.emc.pravega.testcommon.ZKCuratorUtils;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.test.TestingServer;
import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

@Slf4j
public class EndToEndTransactionTest {

    final static long MAX_LEASE_VALUE = 30000;
    final static long MAX_SCALE_GRACE_PERIOD = 60000;

    @Test
    public static void main(String[] args) throws Exception {
        @Cleanup
        TestingServer zkTestServer = ZKCuratorUtils.createTestServer();

        ServiceBuilder serviceBuilder = ServiceBuilder.newInMemoryBuilder(ServiceBuilderConfig.getDefaultConfig());
        serviceBuilder.initialize().get();
        StreamSegmentStore store = serviceBuilder.createStreamSegmentService();
        int port = Config.SERVICE_PORT;
        @Cleanup
        PravegaConnectionListener server = new PravegaConnectionListener(false, port, store);
        server.startListening();

        Thread.sleep(1000);
        @Cleanup
        ControllerWrapper controllerWrapper = new ControllerWrapper(zkTestServer.getConnectString(), port);
        controllerWrapper.awaitRunning();
        Controller controller = controllerWrapper.getController();

        final String testScope = "testScope";
        final String testStream = "testStream";

        if (!controller.createScope(testScope).get()) {
            log.error("FAILURE: Error creating test scope");
            return;
        }

        ScalingPolicy policy = ScalingPolicy.fixed(5);
        StreamConfiguration streamConfig =
                StreamConfiguration.builder()
                        .scope(testScope)
                        .streamName(testStream)
                        .scalingPolicy(policy)
                        .build();

        if (!controller.createStream(streamConfig).get()) {
            log.error("FAILURE: Error creating test stream");
            return;
        }

        final long lease = 4000;
        final long maxExecutionTime = 10000;
        final long scaleGracePeriod = 30000;

        @Cleanup
        MockClientFactory clientFactory = new MockClientFactory(testScope, controller);

        @Cleanup
        EventStreamWriter<String> producer = clientFactory.createEventWriter(
                testStream,
                new JavaSerializer<>(),
                EventWriterConfig.builder().build());

        // region Successful commit tests
        Transaction<String> transaction = producer.beginTxn(5000, 30000, 30000);

        for (int i = 0; i < 1; i++) {
            String event = "\n Transactional Publish \n";
            log.info("Producing event: " + event);
            transaction.writeEvent("", event);
            transaction.flush();
            Thread.sleep(500);
        }

        CompletableFuture<Object> commit = CompletableFuture.supplyAsync(() -> {
            try {
                transaction.commit();
            } catch (Exception e) {
                log.warn("Error committing transaction", e);
            }
            return null;
        });

        commit.join();

        Transaction.Status txnStatus = transaction.checkStatus();
        assertTrue(txnStatus == Transaction.Status.COMMITTING || txnStatus == Transaction.Status.COMMITTED);
        log.info("SUCCESS: successful in committing transaction. Transaction status=" + txnStatus);

        Thread.sleep(2000);

        txnStatus = transaction.checkStatus();
        assertTrue(txnStatus == Transaction.Status.COMMITTED);
        log.info("SUCCESS: successfully committed transaction. Transaction status=" + txnStatus);

        // endregion

        // region Successful abort tests

        Transaction<String> transaction2 = producer.beginTxn(5000, 30000, 30000);
        for (int i = 0; i < 1; i++) {
            String event = "\n Transactional Publish \n";
            log.info("Producing event: " + event);
            transaction2.writeEvent("", event);
            transaction2.flush();
            Thread.sleep(500);
        }

        CompletableFuture<Object> drop = CompletableFuture.supplyAsync(() -> {
            try {
                transaction2.abort();
            } catch (Exception e) {
                log.warn("Error aborting transaction", e);
            }
            return null;
        });

        drop.join();

        Transaction.Status txn2Status = transaction2.checkStatus();
        assertTrue(txn2Status == Transaction.Status.ABORTING || txn2Status == Transaction.Status.ABORTED);
        log.info("SUCCESS: successful in dropping transaction. Transaction status=" + txn2Status);

        Thread.sleep(2000);

        txn2Status = transaction2.checkStatus();
        assertTrue(txn2Status == Transaction.Status.ABORTED);
        log.info("SUCCESS: successfully aborted transaction. Transaction status=" + txn2Status);

        // endregion

        // region Successful timeout tests
        Transaction<String> tx1 = producer.beginTxn(lease, maxExecutionTime, scaleGracePeriod);

        Thread.sleep((long) (1.3 * lease));

        Transaction.Status txStatus = tx1.checkStatus();
        Assert.assertTrue(Transaction.Status.ABORTING == txStatus || Transaction.Status.ABORTED == txStatus);
        log.info("SUCCESS: successfully aborted transaction after timeout. Transaction status=" + txStatus);

        // endregion

        // region Successful ping tests

        Transaction<String> tx2 = producer.beginTxn(lease, maxExecutionTime, scaleGracePeriod);

        Thread.sleep((long) (0.75 * lease));

        Assert.assertEquals(Transaction.Status.OPEN, tx2.checkStatus());

        try {
            tx2.ping(lease);
            Assert.assertTrue(true);
        } catch (PingFailedException pfe) {
            Assert.assertTrue(false);
        }
        log.info("SUCCESS: successfully pinged transaction.");

        Thread.sleep((long) (0.5 * lease));

        Assert.assertEquals(Transaction.Status.OPEN, tx2.checkStatus());

        Thread.sleep((long) (0.8 * lease));

        txStatus = tx2.checkStatus();
        Assert.assertTrue(Transaction.Status.ABORTING == txStatus || Transaction.Status.ABORTED == txStatus);
        log.info("SUCCESS: successfully aborted transaction after pinging. Transaction status=" + txStatus);

        // endregion

        // region Ping failure due to MaxExecutionTime exceeded

        Transaction<String> tx3 = producer.beginTxn(lease, maxExecutionTime, scaleGracePeriod);

        Thread.sleep((long) (0.75 * lease));

        Assert.assertEquals(Transaction.Status.OPEN, tx3.checkStatus());

        try {
            //Assert.assertEquals(PingStatus.OK, pingStatus);
            tx3.ping(lease);
            Assert.assertTrue(true);
        } catch (PingFailedException pfe) {
            Assert.assertTrue(false);
        }

        Thread.sleep((long) (0.75 * lease));

        Assert.assertEquals(Transaction.Status.OPEN, tx3.checkStatus());

        try {
            // PingFailedException is expected to be thrown.
            tx3.ping(lease + 1);
            Assert.assertTrue(false);
        } catch (PingFailedException pfe) {
            Assert.assertTrue(true);
            log.info("SUCCESS: successfully received error after max expiry time");
        }

        Thread.sleep((long) (0.5 * lease));

        txStatus = tx3.checkStatus();
        Assert.assertTrue(Transaction.Status.ABORTING == txStatus || Transaction.Status.ABORTED == txStatus);
        log.info("SUCCESS: successfully aborted transaction after 1 successful ping and 1 unsuccessful" +
                "ping. Transaction status=" + txStatus);

        // endregion

        // region Ping failure due to very high lease value

        Transaction<String> tx4 = producer.beginTxn(lease, maxExecutionTime, scaleGracePeriod);

        try {
            tx4.ping(scaleGracePeriod + 1);
            Assert.assertTrue(false);
        } catch (PingFailedException pfe) {
            Assert.assertTrue(true);
        }

        try {
            tx4.ping(maxExecutionTime + 1);
            Assert.assertTrue(false);
        } catch (PingFailedException pfe) {
            Assert.assertTrue(true);
        }

        try {
            tx4.ping(MAX_LEASE_VALUE + 1);
            Assert.assertTrue(false);
        } catch (PingFailedException pfe) {
            Assert.assertTrue(true);
        }

        try {
            tx4.ping(MAX_SCALE_GRACE_PERIOD + 1);
            Assert.assertTrue(false);
        } catch (PingFailedException pfe) {
            Assert.assertTrue(true);
        }

        // endregion

        // region Ping failure due to controller going into disconnection state

        // Fill in these tests once we have controller.stop() implemented.

        System.exit(0);
    }
}
