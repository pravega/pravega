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
package io.pravega.test.integration.endtoendtest;

import io.pravega.client.ClientConfig;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.admin.impl.ReaderGroupManagerImpl;
import io.pravega.client.connection.impl.ConnectionFactory;
import io.pravega.client.connection.impl.SocketConnectionFactoryImpl;
import io.pravega.client.stream.EventRead;
import io.pravega.client.stream.EventStreamReader;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.ReaderConfig;
import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.Serializer;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.Transaction;
import io.pravega.client.stream.TransactionalEventStreamWriter;
import io.pravega.client.stream.TxnFailedException;
import io.pravega.client.stream.impl.ClientFactoryImpl;
import io.pravega.client.control.impl.Controller;
import io.pravega.client.stream.impl.JavaSerializer;
import io.pravega.client.stream.impl.StreamImpl;
import io.pravega.client.stream.impl.UTF8StringSerializer;
import io.pravega.common.Exceptions;
import io.pravega.segmentstore.contracts.StreamSegmentStore;
import io.pravega.segmentstore.contracts.tables.TableStore;
import io.pravega.segmentstore.server.host.handler.PravegaConnectionListener;
import io.pravega.segmentstore.server.store.ServiceBuilder;
import io.pravega.segmentstore.server.store.ServiceBuilderConfig;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.TestUtils;
import io.pravega.test.common.TestingServerStarter;
import io.pravega.test.common.ThreadPooledTestSuite;
import io.pravega.test.integration.demo.ControllerWrapper;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static io.pravega.test.common.AssertExtensions.assertEventuallyEquals;
import static io.pravega.test.common.AssertExtensions.assertThrows;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@Slf4j
public class EndToEndTxnWithTest extends ThreadPooledTestSuite {

    private static final String STREAM = "stream";
    private static final String SCOPE = "scope";

    private final int controllerPort = TestUtils.getAvailableListenPort();
    private final String serviceHost = "localhost";
    private final int servicePort = TestUtils.getAvailableListenPort();
    private final int containerCount = 4;
    private TestingServer zkTestServer;
    private PravegaConnectionListener server;
    private ControllerWrapper controllerWrapper;
    private ServiceBuilder serviceBuilder;

    @Override
    protected int getThreadPoolSize() {
        return 1;
    }

    @Before
    public void setUp() throws Exception {
        zkTestServer = new TestingServerStarter().start();

        serviceBuilder = ServiceBuilder.newInMemoryBuilder(ServiceBuilderConfig.getDefaultConfig());
        serviceBuilder.initialize();
        StreamSegmentStore store = serviceBuilder.createStreamSegmentService();
        TableStore tableStore = serviceBuilder.createTableStoreService();
        server = new PravegaConnectionListener(false, servicePort, store, tableStore, serviceBuilder.getLowPriorityExecutor());
        server.startListening();

        controllerWrapper = new ControllerWrapper(zkTestServer.getConnectString(),
                false,
                controllerPort,
                serviceHost,
                servicePort,
                containerCount);
        controllerWrapper.awaitRunning();
    }

    @After
    public void tearDown() throws Exception {
        controllerWrapper.close();
        server.close();
        serviceBuilder.close();
        zkTestServer.close();
    }

    @Test(timeout = 10000)
    public void testTxnWithScale() throws Exception {
        StreamConfiguration config = StreamConfiguration.builder()
                                                        .scalingPolicy(ScalingPolicy.fixed(1))
                                                        .build();
        Controller controller = controllerWrapper.getController();
        controllerWrapper.getControllerService().createScope("test").get();
        controller.createStream("test", "test", config).get();
        @Cleanup
        ConnectionFactory connectionFactory = new SocketConnectionFactoryImpl(ClientConfig.builder().build());
        @Cleanup
        ClientFactoryImpl clientFactory = new ClientFactoryImpl("test", controller, connectionFactory);
        @Cleanup
        TransactionalEventStreamWriter<String> test = clientFactory.createTransactionalEventWriter("writer", "test", new UTF8StringSerializer(),
                EventWriterConfig.builder().transactionTimeoutTime(10000).build());
        Transaction<String> transaction1 = test.beginTxn();
        transaction1.writeEvent("0", "txntest1");
        transaction1.commit();

        assertEventuallyEquals(Transaction.Status.COMMITTED, () -> transaction1.checkStatus(), 5000);
        
        // scale
        Stream stream = new StreamImpl("test", "test");
        Map<Double, Double> map = new HashMap<>();
        map.put(0.0, 0.33);
        map.put(0.33, 0.66);
        map.put(0.66, 1.0);
        Boolean result = controller.scaleStream(stream, Collections.singletonList(0L), map, executorService()).getFuture().get();

        assertTrue(result);

        Transaction<String> transaction2 = test.beginTxn();
        transaction2.writeEvent("0", "txntest2");
        transaction2.commit();
        @Cleanup
        ReaderGroupManager groupManager = new ReaderGroupManagerImpl("test", controller, clientFactory);
        groupManager.createReaderGroup("reader", ReaderGroupConfig.builder().disableAutomaticCheckpoints().groupRefreshTimeMillis(0).stream("test/test").build());
        @Cleanup
        EventStreamReader<String> reader = clientFactory.createReader("readerId", "reader", new UTF8StringSerializer(),
                ReaderConfig.builder().build());
        EventRead<String> event = reader.readNextEvent(5000);
        assertNotNull(event.getEvent());
        assertEquals("txntest1", event.getEvent());
        assertNull(reader.readNextEvent(100).getEvent());
        groupManager.getReaderGroup("reader").initiateCheckpoint("cp", executorService());
        event = reader.readNextEvent(5000);
        assertEquals("cp", event.getCheckpointName());
        event = reader.readNextEvent(5000);
        assertNotNull(event.getEvent());
        assertEquals("txntest2", event.getEvent());
    }

    @Test(timeout = 30000)
    public void testTxnWithErrors() throws Exception {
        StreamConfiguration config = StreamConfiguration.builder()
                                                        .scalingPolicy(ScalingPolicy.fixed(1))
                                                        .build();
        Controller controller = controllerWrapper.getController();
        controllerWrapper.getControllerService().createScope(SCOPE).get();
        controller.createStream(SCOPE, STREAM, config).get();
        @Cleanup
        ConnectionFactory connectionFactory = new SocketConnectionFactoryImpl(ClientConfig.builder().build());
        @Cleanup
        ClientFactoryImpl clientFactory = new ClientFactoryImpl(SCOPE, controller, connectionFactory);
        @Cleanup
        TransactionalEventStreamWriter<String> test = clientFactory.createTransactionalEventWriter("writer", STREAM, new UTF8StringSerializer(),
                EventWriterConfig.builder().transactionTimeoutTime(10000).build());
        Transaction<String> transaction = test.beginTxn();
        transaction.writeEvent("0", "txntest1");
        //abort the transaction to simulate a txn abort due to a missing ping request.
        controller.abortTransaction(Stream.of(SCOPE, STREAM), transaction.getTxnId()).join();
        //check the status of the transaction.
        assertEventuallyEquals(Transaction.Status.ABORTED, () -> controller.checkTransactionStatus(Stream.of(SCOPE, STREAM), transaction.getTxnId()).join(), 10000);
        transaction.writeEvent("0", "txntest2");
        //verify that commit fails with TxnFailedException.
        assertThrows("TxnFailedException should be thrown", () -> transaction.commit(), t -> t instanceof TxnFailedException);
    }



    @Test(timeout = 10000)
    public void testTxnConfig() throws Exception {
        // create stream test
        StreamConfiguration config = StreamConfiguration.builder()
                .scalingPolicy(ScalingPolicy.byEventRate(10, 2, 1))
                .build();
        Controller controller = controllerWrapper.getController();
        controllerWrapper.getControllerService().createScope("test").get();
        controller.createStream("test", "test", config).get();
        @Cleanup
        ConnectionFactory connectionFactory = new SocketConnectionFactoryImpl(ClientConfig.builder().build());
        @Cleanup
        ClientFactoryImpl clientFactory = new ClientFactoryImpl("test", controller, connectionFactory);

        // create writers with different configs and try creating transactions against those configs
        EventWriterConfig defaultConfig = EventWriterConfig.builder().build();
        assertNotNull(createTxn(clientFactory, defaultConfig, "test"));

        EventWriterConfig validConfig = EventWriterConfig.builder().transactionTimeoutTime(10000).build();
        assertNotNull(createTxn(clientFactory, validConfig, "test"));

        AssertExtensions.assertThrows("low timeout period not honoured",
                () -> {
                    EventWriterConfig lowTimeoutConfig = EventWriterConfig.builder().transactionTimeoutTime(1000).build();
                    createTxn(clientFactory, lowTimeoutConfig, "test");
                },
                e -> Exceptions.unwrap(e) instanceof IllegalArgumentException);

        EventWriterConfig highTimeoutConfig = EventWriterConfig.builder().transactionTimeoutTime(200 * 1000).build();
        AssertExtensions.assertThrows("lease value too large, max value is 120000",
                () -> createTxn(clientFactory, highTimeoutConfig, "test"),
                e -> e instanceof IllegalArgumentException);
    }

    @Test(timeout = 20000)
    public void testGetTxnWithScale() throws Exception {
        final StreamConfiguration config = StreamConfiguration.builder()
                                                        .scalingPolicy(ScalingPolicy.fixed(1))
                                                        .build();
        final Serializer<String> serializer = new UTF8StringSerializer();
        final EventWriterConfig writerConfig = EventWriterConfig.builder().transactionTimeoutTime(10000).build();

        final Controller controller = controllerWrapper.getController();

        controllerWrapper.getControllerService().createScope("test").get();
        controller.createStream("test", "test", config).get();
        @Cleanup
        ConnectionFactory connectionFactory = new SocketConnectionFactoryImpl(ClientConfig.builder().build());
        @Cleanup
        ClientFactoryImpl clientFactory = new ClientFactoryImpl("test", controller, connectionFactory);

        @Cleanup
        EventStreamWriter<String> streamWriter = clientFactory.createEventWriter("test", serializer, writerConfig);
        streamWriter.writeEvent("key", "e").join();

        @Cleanup
        TransactionalEventStreamWriter<String> txnWriter = clientFactory.createTransactionalEventWriter( "test", serializer, writerConfig);

        Transaction<String> txn = txnWriter.beginTxn();
        txn.writeEvent("key", "1");
        txn.flush();
        // the txn is not yet committed here.
        UUID txnId =  txn.getTxnId();

        // scale up stream
        scaleUpStream();

        // write event using stream writer
        streamWriter.writeEvent("key", "e").join();

        Transaction<String> txn1 = txnWriter.getTxn(txnId);
        txn1.writeEvent("key", "2");
        txn1.flush();
        // commit the transaction
        txn1.commit();
        assertEventuallyEquals(Transaction.Status.COMMITTED, txn1::checkStatus, 5000);

        @Cleanup
        ReaderGroupManager groupManager = new ReaderGroupManagerImpl("test", controller, clientFactory);
        groupManager.createReaderGroup("reader", ReaderGroupConfig.builder().disableAutomaticCheckpoints().groupRefreshTimeMillis(0).stream("test/test").build());
        @Cleanup
        EventStreamReader<String> reader = clientFactory.createReader("readerId", "reader", new UTF8StringSerializer(),
                ReaderConfig.builder().build());
        EventRead<String> event = reader.readNextEvent(5000);
        assertEquals("e", event.getEvent());

        assertNull(reader.readNextEvent(100).getEvent());
        groupManager.getReaderGroup("reader").initiateCheckpoint("cp1", executorService());
        event = reader.readNextEvent(5000);
        assertEquals("Checkpoint event expected", "cp1", event.getCheckpointName());

        event = reader.readNextEvent(5000);
        assertEquals("second event post scale up", "e", event.getEvent());

        assertNull(reader.readNextEvent(100).getEvent());
        groupManager.getReaderGroup("reader").initiateCheckpoint("cp2", executorService());
        event = reader.readNextEvent(5000);
        assertEquals("Checkpoint event expected", "cp2", event.getCheckpointName());

        event = reader.readNextEvent(5000);
        assertEquals("txn events", "1", event.getEvent());
        event = reader.readNextEvent(5000);
        assertEquals("txn events", "2", event.getEvent());

    }

    private void scaleUpStream() throws InterruptedException, java.util.concurrent.ExecutionException {
        Stream stream = new StreamImpl("test", "test");
        Map<Double, Double> map = new HashMap<>();
        map.put(0.0, 0.5);
        map.put(0.5, 1.0);
        Boolean result = controllerWrapper.getController().scaleStream(stream, Collections.singletonList(0L), map, executorService()).getFuture().get();
        assertTrue(result);
    }

    private UUID createTxn(EventStreamClientFactory clientFactory, EventWriterConfig config, String streamName) {
        @Cleanup
        TransactionalEventStreamWriter<String> test = clientFactory.createTransactionalEventWriter("writer", streamName, new JavaSerializer<>(),
                config);
        return test.beginTxn().getTxnId();
    }
}
