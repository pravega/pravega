/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.emc.pravega.demo;

import com.emc.pravega.controller.stream.api.v1.CreateStreamStatus;
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
import com.emc.pravega.stream.impl.StreamConfigurationImpl;
import com.emc.pravega.stream.impl.segment.SegmentOutputConfiguration;
import com.emc.pravega.stream.mock.MockClientFactory;

import java.util.concurrent.CompletableFuture;

import org.apache.curator.test.TestingServer;
import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class EndToEndTransactionTest {

    @Test
    public static void main(String[] args) throws Exception {
        //@Cleanup
        TestingServer zkTestServer = new TestingServer();

        ServiceBuilder serviceBuilder = ServiceBuilder.newInMemoryBuilder(ServiceBuilderConfig.getDefaultConfig());
        serviceBuilder.initialize().get();
        StreamSegmentStore store = serviceBuilder.createStreamSegmentService();
        //@Cleanup
        PravegaConnectionListener server = new PravegaConnectionListener(false, 12345, store);
        server.startListening();

        Controller controller = ControllerWrapper.getController(zkTestServer.getConnectString());

        final String testScope = "testScope";
        final String testStream = "testStream";

        final long lease = 4000;
        final long maxExecutionTime = 10000;
        final long scaleGracePeriod = 30000;

        ScalingPolicy policy = new ScalingPolicy(ScalingPolicy.Type.FIXED_NUM_SEGMENTS, 0L, 0, 5);
        StreamConfiguration streamConfig = new StreamConfigurationImpl(testScope, testStream, policy);

        CompletableFuture<CreateStreamStatus> futureStatus = controller.createStream(streamConfig);
        CreateStreamStatus status = futureStatus.join();

        if (status != CreateStreamStatus.SUCCESS) {
            System.err.println("FAILURE: Error creating test stream");
            return;
        }

        MockClientFactory clientFactory = new MockClientFactory(testScope, controller);

        //@Cleanup
        EventStreamWriter<String> producer = clientFactory.createEventWriter(
                testStream,
                new JavaSerializer<>(),
                new EventWriterConfig(new SegmentOutputConfiguration()));

        // region Successful commit tests
        Transaction<String> transaction = producer.beginTxn(60000, 60000, 60000);

        for (int i = 0; i < 1; i++) {
            String event = "\n Transactional Publish \n";
            System.err.println("Producing event: " + event);
            transaction.writeEvent("", event);
            transaction.flush();
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

        commit.join();

        Transaction.Status txnStatus = transaction.checkStatus();
        assertTrue(txnStatus == Transaction.Status.COMMITTING || txnStatus == Transaction.Status.COMMITTED);
        System.err.println("SUCCESS: successful in committing transaction. Transaction status=" + txnStatus);

        Thread.sleep(2000);

        txnStatus = transaction.checkStatus();
        assertTrue(txnStatus == Transaction.Status.COMMITTED);
        System.err.println("SUCCESS: successfully committed transaction. Transaction status=" + txnStatus);

        // endregion

        // region Successful abort tests

        Transaction<String> transaction2 = producer.beginTxn(60000, 60000, 60000);
        for (int i = 0; i < 1; i++) {
            String event = "\n Transactional Publish \n";
            System.err.println("Producing event: " + event);
            transaction2.writeEvent("", event);
            transaction2.flush();
            Thread.sleep(500);
        }

        CompletableFuture<Object> drop = CompletableFuture.supplyAsync(() -> {
            try {
                transaction2.abort();
            } catch (Exception e) {
                e.printStackTrace();
            }
            return null;
        });

        drop.join();

        Transaction.Status txn2Status = transaction2.checkStatus();
        assertTrue(txn2Status == Transaction.Status.ABORTING || txn2Status == Transaction.Status.ABORTED);
        System.err.println("SUCCESS: successful in dropping transaction. Transaction status=" + txn2Status);

        Thread.sleep(2000);

        txn2Status = transaction2.checkStatus();
        assertTrue(txn2Status == Transaction.Status.ABORTED);
        System.err.println("SUCCESS: successfully aborted transaction. Transaction status=" + txn2Status);

        // endregion

        // region Successful timeout tests
        Transaction<String> tx1 = producer.beginTxn(lease, maxExecutionTime, scaleGracePeriod);

        Thread.sleep(5200);

        Transaction.Status txStatus = tx1.checkStatus();
        Assert.assertTrue(Transaction.Status.ABORTING == txStatus || Transaction.Status.ABORTED == txStatus);
        System.err.println("SUCCESS: successfully aborted transaction after timeout. Transaction status=" + txStatus);

        // endregion

        // region Successful ping tests

        Transaction<String> tx2 = producer.beginTxn(lease, maxExecutionTime, scaleGracePeriod);

        Thread.sleep(3000);

        Assert.assertEquals(Transaction.Status.OPEN, tx2.checkStatus());

        try {
            tx2.ping(lease);
            Assert.assertTrue(true);
        } catch (PingFailedException pfe) {
            Assert.assertTrue(false);
        }
        System.err.println("SUCCESS: successfully pinged transaction.");

        Thread.sleep(2000);

        Assert.assertEquals(Transaction.Status.OPEN, tx2.checkStatus());

        Thread.sleep(3200);

        txStatus = tx2.checkStatus();
        Assert.assertTrue(Transaction.Status.ABORTING == txStatus || Transaction.Status.ABORTED == txStatus);
        System.err.println("SUCCESS: successfully aborted transaction after pinging. Transaction status=" + txStatus);

        // endregion

        // region Ping failure due to MaxExecutionTime exceeded

        Transaction<String> tx3 = producer.beginTxn(lease, maxExecutionTime, scaleGracePeriod);

        Thread.sleep(3000);

        Assert.assertEquals(Transaction.Status.OPEN, tx3.checkStatus());

        try {
            //Assert.assertEquals(PingStatus.OK, pingStatus);
            tx3.ping(lease);
            Assert.assertTrue(true);
        } catch (PingFailedException pfe) {
            Assert.assertTrue(false);
        }

        Thread.sleep(3000);

        Assert.assertEquals(Transaction.Status.OPEN, tx3.checkStatus());

        try {
            // PingFailedException is expected to be thrown.
            tx3.ping(lease + 1);
            Assert.assertTrue(false);
        } catch (PingFailedException pfe) {
            Assert.assertTrue(true);
            System.err.println("SUCCESS: successfully received error after max expiry time");
        }

        Thread.sleep(2000);

        txStatus = tx3.checkStatus();
        Assert.assertTrue(Transaction.Status.ABORTING == txStatus || Transaction.Status.ABORTED == txStatus);
        System.err.println("SUCCESS: successfully aborted transaction after 1 successful ping and 1 unsuccessful" +
                "ping. Transaction status=" + txStatus);

        // endregion

        // region Ping failure due to controller going into disconnection state

        // Fill in these tests once we have controller.stop() implemented.

        // endregion

        // region

        producer.close();
        server.close();
        zkTestServer.close();
    }
}
