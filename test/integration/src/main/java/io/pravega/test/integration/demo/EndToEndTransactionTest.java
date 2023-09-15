/**
 * Copyright Pravega Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.pravega.test.integration.demo;

import io.pravega.client.ClientConfig;
import io.pravega.client.connection.impl.ConnectionFactory;
import io.pravega.client.connection.impl.ConnectionPoolImpl;
import io.pravega.client.connection.impl.SocketConnectionFactoryImpl;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.Transaction;
import io.pravega.client.stream.TransactionalEventStreamWriter;
import io.pravega.client.control.impl.Controller;
import io.pravega.client.stream.impl.UTF8StringSerializer;
import io.pravega.client.stream.mock.MockClientFactory;
import io.pravega.controller.util.Config;
import io.pravega.segmentstore.contracts.StreamSegmentStore;
import io.pravega.segmentstore.server.host.handler.IndexAppendProcessor;
import io.pravega.segmentstore.server.host.handler.PravegaConnectionListener;
import io.pravega.segmentstore.server.store.ServiceBuilder;
import io.pravega.segmentstore.server.store.ServiceBuilderConfig;
import io.pravega.test.common.TestingServerStarter;

import java.util.concurrent.CompletableFuture;

import io.pravega.test.integration.utils.ControllerWrapper;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.test.TestingServer;
import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

@Slf4j
public class EndToEndTransactionTest {

    final static long MAX_LEASE_VALUE = 30000;

    @Test
    public static void main(String[] args) throws Exception {
        @Cleanup
        TestingServer zkTestServer = new TestingServerStarter().start();

        ServiceBuilder serviceBuilder = ServiceBuilder.newInMemoryBuilder(ServiceBuilderConfig.getDefaultConfig());
        serviceBuilder.initialize();
        StreamSegmentStore store = serviceBuilder.createStreamSegmentService();
        int port = Config.SERVICE_PORT;
        IndexAppendProcessor indexAppendProcessor = new IndexAppendProcessor(serviceBuilder.getLowPriorityExecutor(), store);
        @Cleanup
        PravegaConnectionListener server = new PravegaConnectionListener(false, port, store,
                serviceBuilder.createTableStoreService(), serviceBuilder.getLowPriorityExecutor(),
                Config.TLS_PROTOCOL_VERSION.toArray(new String[Config.TLS_PROTOCOL_VERSION.size()]), indexAppendProcessor);
        server.startListening();

        Thread.sleep(1000);
        @Cleanup
        ControllerWrapper controllerWrapper = new ControllerWrapper(zkTestServer.getConnectString(), port);
        Controller controller = controllerWrapper.getController();

        controllerWrapper.awaitRunning();

        final String testScope = "testScope";
        final String testStream = "testStream";

        if (!controller.createScope(testScope).get()) {
            log.error("FAILURE: Error creating test scope");
            return;
        }

        ScalingPolicy policy = ScalingPolicy.fixed(5);
        StreamConfiguration streamConfig =
                StreamConfiguration.builder()
                        .scalingPolicy(policy)
                        .build();

        if (!controller.createStream(testScope, testStream, streamConfig).get()) {
            log.error("FAILURE: Error creating test stream");
            return;
        }

        final long txnTimeout = 4000;

        ClientConfig config = ClientConfig.builder().build();
        @Cleanup
        ConnectionFactory connectionFactory = new SocketConnectionFactoryImpl(config);
        @Cleanup
        MockClientFactory clientFactory = new MockClientFactory(testScope, controller, new ConnectionPoolImpl(config, connectionFactory));

        @Cleanup
        TransactionalEventStreamWriter<String> producer = clientFactory.createTransactionalEventWriter(
                "writer",
                testStream,
                new UTF8StringSerializer(),
                EventWriterConfig.builder().transactionTimeoutTime(txnTimeout).build());

        // region Successful commit tests
        Transaction<String> transaction = producer.beginTxn();

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

        Transaction<String> transaction2 = producer.beginTxn();
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
        Transaction<String> tx1 = producer.beginTxn();

        Thread.sleep((long) (1.3 * txnTimeout));

        Transaction.Status txStatus = tx1.checkStatus();
        Assert.assertTrue(Transaction.Status.ABORTING == txStatus || Transaction.Status.ABORTED == txStatus);
        log.info("SUCCESS: successfully aborted transaction after timeout. Transaction status=" + txStatus);

        // endregion

        // region Ping failure due to controller going into disconnection state

        // Fill in these tests once we have controller.stop() implemented.

        System.exit(0);
    }
}
