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
import io.pravega.client.control.impl.Controller;
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
import io.pravega.client.stream.impl.JavaSerializer;
import io.pravega.client.stream.impl.StreamImpl;
import io.pravega.client.stream.impl.UTF8StringSerializer;
import io.pravega.common.Exceptions;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.ThreadPooledTestSuite;
import io.pravega.test.integration.PravegaResource;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.junit.ClassRule;
import org.junit.Test;

import static io.pravega.test.common.AssertExtensions.assertEventuallyEquals;
import static io.pravega.test.common.AssertExtensions.assertThrows;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@Slf4j
public class EndToEndTxnWithTest extends ThreadPooledTestSuite {

    @ClassRule
    public static final PravegaResource PRAVEGA = new PravegaResource();

    @Override
    protected int getThreadPoolSize() {
        return 1;
    }

    @Test(timeout = 10000)
    public void testTxnWithScale() throws Exception {
        StreamConfiguration config = StreamConfiguration.builder()
                                                        .scalingPolicy(ScalingPolicy.fixed(1))
                                                        .build();
        Controller controller = PRAVEGA.getLocalController();
        controller.createScope("test").get();
        String streamName = "testTxnWithScale";
        controller.createStream("test", streamName, config).get();
        @Cleanup
        ConnectionFactory connectionFactory = new SocketConnectionFactoryImpl(ClientConfig.builder().build());
        @Cleanup
        ClientFactoryImpl clientFactory = new ClientFactoryImpl("test", controller, connectionFactory);
        @Cleanup
        TransactionalEventStreamWriter<String> test = clientFactory.createTransactionalEventWriter("writer", streamName, new UTF8StringSerializer(),
                EventWriterConfig.builder().transactionTimeoutTime(10000).build());
        Transaction<String> transaction1 = test.beginTxn();
        transaction1.writeEvent("0", "txntest1");
        transaction1.commit();

        assertEventuallyEquals(Transaction.Status.COMMITTED, () -> transaction1.checkStatus(), 5000);

        // scale
        Stream stream = new StreamImpl("test", streamName);
        Map<Double, Double> map = new HashMap<>();
        map.put(0.0, 0.33);
        map.put(0.33, 0.66);
        map.put(0.66, 1.0);
        Boolean result = controller.scaleStream(stream, Collections.singletonList(0L), map, executorService()).getFuture().get();

        assertTrue(result);

        Transaction<String> transaction2 = test.beginTxn();
        transaction2.writeEvent("0", "txntest2");
        transaction2.commit();
        String group = "testTxnWithScale-group";
        @Cleanup
        ReaderGroupManager groupManager = new ReaderGroupManagerImpl("test", controller, clientFactory);
        groupManager.createReaderGroup(group, ReaderGroupConfig.builder().disableAutomaticCheckpoints().groupRefreshTimeMillis(0).stream("test/" + streamName).build());
        @Cleanup
        EventStreamReader<String> reader = clientFactory.createReader("readerId", group, new UTF8StringSerializer(),
                ReaderConfig.builder().build());
        EventRead<String> event = reader.readNextEvent(5000);
        assertNotNull(event.getEvent());
        assertEquals("txntest1", event.getEvent());
        assertNull(reader.readNextEvent(100).getEvent());
        groupManager.getReaderGroup(group).initiateCheckpoint("cp", executorService());
        event = reader.readNextEvent(5000);
        assertEquals("cp", event.getCheckpointName());
        event = reader.readNextEvent(5000);
        assertNotNull(event.getEvent());
        assertEquals("txntest2", event.getEvent());
    }

    @Test(timeout = 30000)
    public void testTxnWithErrors() throws Exception {
        String scope = "scope";
        String stream = "testTxnWithErrors";
        StreamConfiguration config = StreamConfiguration.builder()
                                                        .scalingPolicy(ScalingPolicy.fixed(1))
                                                        .build();
        Controller controller = PRAVEGA.getLocalController();
        controller.createScope(scope).get();
        controller.createStream(scope, stream, config).get();
        @Cleanup
        ConnectionFactory connectionFactory = new SocketConnectionFactoryImpl(ClientConfig.builder().build());
        @Cleanup
        ClientFactoryImpl clientFactory = new ClientFactoryImpl(scope, controller, connectionFactory);
        @Cleanup
        TransactionalEventStreamWriter<String> test = clientFactory.createTransactionalEventWriter("writer", stream, new UTF8StringSerializer(),
                EventWriterConfig.builder().transactionTimeoutTime(10000).build());
        Transaction<String> transaction = test.beginTxn();
        transaction.writeEvent("0", "txntest1");
        //abort the transaction to simulate a txn abort due to a missing ping request.
        controller.abortTransaction(Stream.of(scope, stream), transaction.getTxnId()).join();
        //check the status of the transaction.
        assertEventuallyEquals(Transaction.Status.ABORTED, () -> controller.checkTransactionStatus(Stream.of(scope, stream), transaction.getTxnId()).join(), 10000);
        assertEquals(1, controller.listCompletedTransactions(Stream.of(scope, stream)).join().size());
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
        Controller controller = PRAVEGA.getLocalController();
        controller.createScope("test").get();
        String streamName = "testTxnConfig";
        controller.createStream("test", streamName, config).get();
        @Cleanup
        ConnectionFactory connectionFactory = new SocketConnectionFactoryImpl(ClientConfig.builder().build());
        @Cleanup
        ClientFactoryImpl clientFactory = new ClientFactoryImpl("test", controller, connectionFactory);

        // create writers with different configs and try creating transactions against those configs
        EventWriterConfig defaultConfig = EventWriterConfig.builder().build();
        assertNotNull(createTxn(clientFactory, defaultConfig, streamName));

        EventWriterConfig validConfig = EventWriterConfig.builder().transactionTimeoutTime(10000).build();
        assertNotNull(createTxn(clientFactory, validConfig, streamName));

        AssertExtensions.assertThrows("low timeout period not honoured",
                () -> {
                    EventWriterConfig lowTimeoutConfig = EventWriterConfig.builder().transactionTimeoutTime(1000).build();
                    createTxn(clientFactory, lowTimeoutConfig, streamName);
                },
                e -> Exceptions.unwrap(e) instanceof IllegalArgumentException);

        EventWriterConfig highTimeoutConfig = EventWriterConfig.builder().transactionTimeoutTime(700 * 1000).build();
        AssertExtensions.assertThrows("lease value too large, max value is 600000",
                () -> createTxn(clientFactory, highTimeoutConfig, streamName),
                e -> e instanceof IllegalArgumentException);
    }

    @Test(timeout = 20000)
    public void testGetTxnWithScale() throws Exception {
        String streamName = "testGetTxnWithScale";
        final StreamConfiguration config = StreamConfiguration.builder()
                                                        .scalingPolicy(ScalingPolicy.fixed(1))
                                                        .build();
        final Serializer<String> serializer = new UTF8StringSerializer();
        final EventWriterConfig writerConfig = EventWriterConfig.builder().transactionTimeoutTime(10000).build();

        final Controller controller = PRAVEGA.getLocalController();

        controller.createScope("test").get();
        controller.createStream("test", streamName, config).get();
        @Cleanup
        ConnectionFactory connectionFactory = new SocketConnectionFactoryImpl(ClientConfig.builder().build());
        @Cleanup
        ClientFactoryImpl clientFactory = new ClientFactoryImpl("test", controller, connectionFactory);

        @Cleanup
        EventStreamWriter<String> streamWriter = clientFactory.createEventWriter(streamName, serializer, writerConfig);
        streamWriter.writeEvent("key", "e").join();

        @Cleanup
        TransactionalEventStreamWriter<String> txnWriter = clientFactory.createTransactionalEventWriter(streamName, serializer, writerConfig);

        Transaction<String> txn = txnWriter.beginTxn();
        txn.writeEvent("key", "1");
        txn.flush();
        // the txn is not yet committed here.
        UUID txnId =  txn.getTxnId();

        // scale up stream
        scaleUpStream(streamName);

        // write event using stream writer
        streamWriter.writeEvent("key", "e").join();

        Transaction<String> txn1 = txnWriter.getTxn(txnId);
        txn1.writeEvent("key", "2");
        txn1.flush();
        // commit the transaction
        txn1.commit();
        assertEventuallyEquals(Transaction.Status.COMMITTED, txn1::checkStatus, 5000);
        String group = "testGetTxnWithScale-group";
        @Cleanup
        ReaderGroupManager groupManager = new ReaderGroupManagerImpl("test", controller, clientFactory);
        groupManager.createReaderGroup(group, ReaderGroupConfig.builder().disableAutomaticCheckpoints().groupRefreshTimeMillis(0).stream("test/" + streamName).build());
        @Cleanup
        EventStreamReader<String> reader = clientFactory.createReader("readerId", group, new UTF8StringSerializer(),
                ReaderConfig.builder().build());
        EventRead<String> event = reader.readNextEvent(5000);
        assertEquals("e", event.getEvent());

        assertNull(reader.readNextEvent(100).getEvent());
        groupManager.getReaderGroup(group).initiateCheckpoint("cp1", executorService());
        event = reader.readNextEvent(5000);
        assertEquals("Checkpoint event expected", "cp1", event.getCheckpointName());

        event = reader.readNextEvent(5000);
        assertEquals("second event post scale up", "e", event.getEvent());

        assertNull(reader.readNextEvent(100).getEvent());
        groupManager.getReaderGroup(group).initiateCheckpoint("cp2", executorService());
        event = reader.readNextEvent(5000);
        assertEquals("Checkpoint event expected", "cp2", event.getCheckpointName());

        event = reader.readNextEvent(5000);
        assertEquals("txn events", "1", event.getEvent());
        event = reader.readNextEvent(5000);
        assertEquals("txn events", "2", event.getEvent());

    }

    private void scaleUpStream(String streamName) throws InterruptedException, java.util.concurrent.ExecutionException {
        Stream stream = new StreamImpl("test", streamName);
        Map<Double, Double> map = new HashMap<>();
        map.put(0.0, 0.5);
        map.put(0.5, 1.0);
        Boolean result = PRAVEGA.getLocalController().scaleStream(stream, Collections.singletonList(0L), map, executorService()).getFuture().get();
        assertTrue(result);
    }

    private UUID createTxn(EventStreamClientFactory clientFactory, EventWriterConfig config, String streamName) {
        @Cleanup
        TransactionalEventStreamWriter<String> test = clientFactory.createTransactionalEventWriter("writer", streamName, new JavaSerializer<>(),
                config);
        return test.beginTxn().getTxnId();
    }
}
