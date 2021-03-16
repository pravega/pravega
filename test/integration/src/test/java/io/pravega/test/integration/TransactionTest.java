/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.test.integration;

import io.pravega.client.stream.EventStreamReader;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.ReaderConfig;
import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.client.stream.ReinitializationRequiredException;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.Transaction;
import io.pravega.client.stream.TransactionalEventStreamWriter;
import io.pravega.client.stream.TxnFailedException;
import io.pravega.client.stream.impl.JavaSerializer;
import io.pravega.client.stream.mock.MockClientFactory;
import io.pravega.client.stream.mock.MockStreamManager;
import io.pravega.segmentstore.contracts.StreamSegmentStore;
import io.pravega.segmentstore.contracts.tables.TableStore;
import io.pravega.segmentstore.server.host.handler.PravegaConnectionListener;
import io.pravega.segmentstore.server.store.ServiceBuilder;
import io.pravega.segmentstore.server.store.ServiceBuilderConfig;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.LeakDetectorTestSuite;
import io.pravega.test.common.TestUtils;
import java.io.Serializable;
import lombok.Cleanup;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TransactionTest extends LeakDetectorTestSuite {
    private static final ServiceBuilder SERVICE_BUILDER = ServiceBuilder.newInMemoryBuilder(ServiceBuilderConfig.getDefaultConfig());

    @BeforeClass
    public static void setup() throws Exception {
        SERVICE_BUILDER.initialize();
    }

    @AfterClass
    public static void teardown() {
        SERVICE_BUILDER.close();
    }

    @Test(timeout = 10000)
    @SuppressWarnings("deprecation")
    public void testTransactionalWritesOrderedCorrectly() throws TxnFailedException, ReinitializationRequiredException {
        int readTimeout = 5000;
        String readerName = "reader";
        String groupName = "testTransactionalWritesOrderedCorrectly-group";
        String endpoint = "localhost";
        String streamName = "testTransactionalWritesOrderedCorrectly";
        int port = TestUtils.getAvailableListenPort();
        String txnEvent = "TXN Event\n";
        String nonTxEvent = "Non-TX Event\n";
        String routingKey = "RoutingKey";
        StreamSegmentStore store = SERVICE_BUILDER.createStreamSegmentService();
        TableStore tableStore = SERVICE_BUILDER.createTableStoreService();

        @Cleanup
        PravegaConnectionListener server = new PravegaConnectionListener(false, port, store, tableStore,
                SERVICE_BUILDER.getLowPriorityExecutor());
        server.startListening();
        @Cleanup
        MockStreamManager streamManager = new MockStreamManager("scope", endpoint, port);
        streamManager.createScope("scope");
        streamManager.createStream("scope", streamName, StreamConfiguration.builder().build());
        streamManager.createReaderGroup(groupName,
                                        ReaderGroupConfig.builder()
                                                         .stream(Stream.of("scope", streamName))
                                                         .disableAutomaticCheckpoints()
                                                         .build());
        @Cleanup
        MockClientFactory clientFactory = streamManager.getClientFactory();
        EventWriterConfig eventWriterConfig = EventWriterConfig.builder().transactionTimeoutTime(60000).build();
        @Cleanup
        EventStreamWriter<String> producer = clientFactory.createEventWriter(streamName, new JavaSerializer<>(),
                                                                                eventWriterConfig);
        @Cleanup
        TransactionalEventStreamWriter<String> txnProducer = clientFactory.createTransactionalEventWriter(streamName,
                                                                                                          new JavaSerializer<>(),
                                                                                                          eventWriterConfig);
        producer.writeEvent(routingKey, nonTxEvent);
        Transaction<String> transaction = txnProducer.beginTxn();
        producer.writeEvent(routingKey, nonTxEvent);
        transaction.writeEvent(routingKey, txnEvent);
        producer.writeEvent(routingKey, nonTxEvent);
        transaction.writeEvent(routingKey, txnEvent);
        producer.flush();
        producer.writeEvent(routingKey, nonTxEvent);
        transaction.writeEvent(routingKey, txnEvent);
        producer.writeEvent(routingKey, nonTxEvent);
        transaction.writeEvent(routingKey, txnEvent);
        transaction.flush();
        producer.writeEvent(routingKey, nonTxEvent);
        transaction.writeEvent(routingKey, txnEvent);
        producer.flush();
        transaction.writeEvent(routingKey, txnEvent);
        transaction.commit();
        producer.writeEvent(routingKey, nonTxEvent);
        AssertExtensions.assertThrows(TxnFailedException.class, () -> transaction.writeEvent(routingKey, txnEvent));

        @Cleanup
        EventStreamReader<Serializable> consumer = streamManager.getClientFactory().createReader(readerName,
                                                                                                 groupName,
                                                                                                 new JavaSerializer<>(),
                                                                                                 ReaderConfig.builder().build());

        assertEquals(nonTxEvent, consumer.readNextEvent(readTimeout).getEvent());
        assertEquals(nonTxEvent, consumer.readNextEvent(readTimeout).getEvent());
        assertEquals(nonTxEvent, consumer.readNextEvent(readTimeout).getEvent());
        assertEquals(nonTxEvent, consumer.readNextEvent(readTimeout).getEvent());
        assertEquals(nonTxEvent, consumer.readNextEvent(readTimeout).getEvent());
        assertEquals(nonTxEvent, consumer.readNextEvent(readTimeout).getEvent());

        assertEquals(txnEvent, consumer.readNextEvent(readTimeout).getEvent());
        assertEquals(txnEvent, consumer.readNextEvent(readTimeout).getEvent());
        assertEquals(txnEvent, consumer.readNextEvent(readTimeout).getEvent());
        assertEquals(txnEvent, consumer.readNextEvent(readTimeout).getEvent());
        assertEquals(txnEvent, consumer.readNextEvent(readTimeout).getEvent());
        assertEquals(txnEvent, consumer.readNextEvent(readTimeout).getEvent());

        assertEquals(nonTxEvent, consumer.readNextEvent(readTimeout).getEvent());
    }

    @Test(timeout = 10000)
    @SuppressWarnings("deprecation")
    public void testDoubleCommit() throws TxnFailedException {
        String endpoint = "localhost";
        String streamName = "testDoubleCommit";
        int port = TestUtils.getAvailableListenPort();
        String event = "Event\n";
        String routingKey = "RoutingKey";
        StreamSegmentStore store = SERVICE_BUILDER.createStreamSegmentService();
        TableStore tableStore = SERVICE_BUILDER.createTableStoreService();

        @Cleanup
        PravegaConnectionListener server = new PravegaConnectionListener(false, port, store, tableStore,
                SERVICE_BUILDER.getLowPriorityExecutor());
        server.startListening();
        @Cleanup
        MockStreamManager streamManager = new MockStreamManager("scope", endpoint, port);
        streamManager.createScope("scope");
        streamManager.createStream("scope", streamName, null);
        @Cleanup
        MockClientFactory clientFactory = streamManager.getClientFactory();
        EventWriterConfig eventWriterConfig = EventWriterConfig.builder()
                          .transactionTimeoutTime(60000)
                          .build();
        @Cleanup
        TransactionalEventStreamWriter<String> producer = clientFactory.createTransactionalEventWriter(streamName,
                                                                                                       new JavaSerializer<>(),
                                                                                                       eventWriterConfig);
        Transaction<String> transaction = producer.beginTxn();
        transaction.writeEvent(routingKey, event);
        transaction.commit();
        AssertExtensions.assertThrows(TxnFailedException.class, () -> transaction.commit());
    }

    @Test(timeout = 10000)
    @SuppressWarnings("deprecation")
    public void testDrop() throws TxnFailedException, ReinitializationRequiredException {
        String endpoint = "localhost";
        String groupName = "testDrop-group";
        String streamName = "testDrop";
        int port = TestUtils.getAvailableListenPort();
        String txnEvent = "TXN Event\n";
        String nonTxEvent = "Non-TX Event\n";
        String routingKey = "RoutingKey";
        StreamSegmentStore store = SERVICE_BUILDER.createStreamSegmentService();
        TableStore tableStore = SERVICE_BUILDER.createTableStoreService();

        @Cleanup
        PravegaConnectionListener server = new PravegaConnectionListener(false, port, store, tableStore,
                SERVICE_BUILDER.getLowPriorityExecutor());
        server.startListening();
        @Cleanup
        MockStreamManager streamManager = new MockStreamManager("scope", endpoint, port);
        streamManager.createScope("scope");
        streamManager.createStream("scope", streamName, StreamConfiguration.builder().build());
        streamManager.createReaderGroup(groupName,
                                        ReaderGroupConfig.builder()
                                                         .stream(Stream.of("scope", streamName))
                                                         .disableAutomaticCheckpoints()
                                                         .build());
        @Cleanup
        MockClientFactory clientFactory = streamManager.getClientFactory();
        EventWriterConfig eventWriterConfig = EventWriterConfig.builder()
                          .transactionTimeoutTime(60000)
                          .build();
        @Cleanup
        TransactionalEventStreamWriter<String> txnProducer = clientFactory.createTransactionalEventWriter(streamName,
                                                                                                       new JavaSerializer<>(),
                                                                                                       eventWriterConfig);

        Transaction<String> transaction = txnProducer.beginTxn();
        transaction.writeEvent(routingKey, txnEvent);
        transaction.flush();
        transaction.abort();
        transaction.abort();

        AssertExtensions.assertThrows(TxnFailedException.class, () -> transaction.writeEvent(routingKey, txnEvent));
        AssertExtensions.assertThrows(TxnFailedException.class, () -> transaction.commit());
        @Cleanup
        EventStreamReader<Serializable> consumer = streamManager.getClientFactory().createReader("reader",
                                                                                                 groupName,
                                                                                                 new JavaSerializer<>(),
                                                                                                 ReaderConfig.builder().build());
        @Cleanup
        EventStreamWriter<String> producer = clientFactory.createEventWriter(streamName, new JavaSerializer<>(),
                                                                             eventWriterConfig);
        producer.writeEvent(routingKey, nonTxEvent);
        producer.flush();
        assertEquals(nonTxEvent, consumer.readNextEvent(1500).getEvent());
    }
}
