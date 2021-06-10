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
import io.pravega.client.admin.impl.ReaderGroupManagerImpl;
import io.pravega.client.connection.impl.ConnectionFactory;
import io.pravega.client.connection.impl.SocketConnectionFactoryImpl;
import io.pravega.client.control.impl.Controller;
import io.pravega.client.stream.EventStreamReader;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.ReaderConfig;
import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.Transaction;
import io.pravega.client.stream.TransactionalEventStreamWriter;
import io.pravega.client.stream.impl.ClientFactoryImpl;
import io.pravega.client.stream.mock.MockClientFactory;
import io.pravega.common.concurrent.ExecutorServiceHelpers;
import io.pravega.controller.util.Config;
import io.pravega.segmentstore.contracts.StreamSegmentStore;
import io.pravega.segmentstore.contracts.tables.TableStore;
import io.pravega.segmentstore.server.host.handler.PravegaConnectionListener;
import io.pravega.segmentstore.server.store.ServiceBuilder;
import io.pravega.segmentstore.server.store.ServiceBuilderConfig;
import io.pravega.shared.NameUtils;
import io.pravega.test.common.TestingServerStarter;
import io.pravega.test.integration.utils.IntegerSerializer;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.curator.test.TestingServer;

@Slf4j
public class EndToEndAutoScaleUpWithTxnTest {
    static final StreamConfiguration CONFIG = StreamConfiguration.builder()
                                                                 .scalingPolicy(ScalingPolicy.fixed(1))//byEventRate(10, 2, 1))
                                                                 .build();

    public static void main(String[] args) throws Exception {
        @Cleanup
        TestingServer zkTestServer = new TestingServerStarter().start();
        int port = Config.SERVICE_PORT;
        @Cleanup
        ServiceBuilder serviceBuilder = ServiceBuilder.newInMemoryBuilder(ServiceBuilderConfig.getDefaultConfig());
        serviceBuilder.initialize();
        StreamSegmentStore store = serviceBuilder.createStreamSegmentService();
        TableStore tableStore = serviceBuilder.createTableStoreService();
        @Cleanup
        PravegaConnectionListener server = new PravegaConnectionListener(false, 12345, store, tableStore,
                serviceBuilder.getLowPriorityExecutor());
        server.startListening();

        @Cleanup
        ControllerWrapper controllerWrapper = new ControllerWrapper(zkTestServer.getConnectString(), port);
        Controller controller = controllerWrapper.getController();
        controllerWrapper.getControllerService().createScope(NameUtils.INTERNAL_SCOPE_NAME, 0L).get();

        @Cleanup
        ConnectionFactory connectionFactory = new SocketConnectionFactoryImpl(ClientConfig.builder().build());
        @Cleanup
        ClientFactoryImpl internalCF = new ClientFactoryImpl(NameUtils.INTERNAL_SCOPE_NAME, controller, connectionFactory);

        @Cleanup("shutdownNow")
        val executor = ExecutorServiceHelpers.newScheduledThreadPool(1, "test");

        controllerWrapper.awaitRunning();
        controllerWrapper.getControllerService().createScope("test", 0L).get();

        controller.createStream("test", "test", CONFIG).get();
        @Cleanup
        MockClientFactory clientFactory = new MockClientFactory("test", controller, internalCF.getConnectionPool());

        // Mocking pravega service by putting scale up and scale down requests for the stream
        EventWriterConfig writerConfig = EventWriterConfig.builder()
                                                          .transactionTimeoutTime(30000)
                                                          .build();
        TransactionalEventStreamWriter<Integer> test = clientFactory.createTransactionalEventWriter("writer", "test", new IntegerSerializer(), writerConfig);

        // region Successful commit tests
        Transaction<Integer> txn1 = null;

        
        for (int i = 0; i < 100; i++) {
            txn1 = test.beginTxn();
            txn1.writeEvent(i);
            txn1.commit();
        }
        
        while(true) {
            if (txn1.checkStatus().equals(Transaction.Status.COMMITTED)) {
                break;
            } 
            Thread.sleep(1000);
        }

        @Cleanup
        ReaderGroupManagerImpl readerGroupManager = new ReaderGroupManagerImpl("test", controller, clientFactory);
        readerGroupManager.createReaderGroup("readergrp",
                ReaderGroupConfig.builder()
                                 .disableAutomaticCheckpoints()
                                 .groupRefreshTimeMillis(1000)
                                 .stream("test/test")
                                 .build());

        @Cleanup
        EventStreamReader<Integer> reader = clientFactory.createReader("1",
                "readergrp",
                new IntegerSerializer(),
                ReaderConfig.builder().build());

        int previous = -1;
        while(previous < 99) {
            val event = reader.readNextEvent(1000);
            if (event == null) continue;
            if (event.getEvent() == previous + 1) {
                previous = event.getEvent();
            } else {
                throw new RuntimeException("order order");
            }
        }
    }
}


