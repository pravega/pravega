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

import io.pravega.client.ClientConfig;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.EventStreamReader;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.ReaderConfig;
import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.client.stream.ReinitializationRequiredException;
import io.pravega.client.stream.ScalingPolicy;
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
import io.pravega.test.integration.utils.SetupUtils;
import java.io.Serializable;

import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

@Slf4j
public class TransactionTest extends LeakDetectorTestSuite {
    private ServiceBuilder serviceBuilder;

    @Before
    public void setup() throws Exception {
        this.serviceBuilder = ServiceBuilder.newInMemoryBuilder(ServiceBuilderConfig.getDefaultConfig());
        this.serviceBuilder.initialize();
    }

    @After
    public void teardown() {
        this.serviceBuilder.close();
    }

    @Test(timeout = 10000)
    @SuppressWarnings("deprecation")
    public void testTransactionalWritesOrderedCorrectly() throws TxnFailedException, ReinitializationRequiredException {
        int readTimeout = 5000;
        String readerName = "reader";
        String groupName = "group";
        String endpoint = "localhost";
        String streamName = "abc";
        int port = TestUtils.getAvailableListenPort();
        String txnEvent = "TXN Event\n";
        String nonTxEvent = "Non-TX Event\n";
        String routingKey = "RoutingKey";
        StreamSegmentStore store = this.serviceBuilder.createStreamSegmentService();
        TableStore tableStore = serviceBuilder.createTableStoreService();

        @Cleanup
        PravegaConnectionListener server = new PravegaConnectionListener(false, port, store, tableStore,
                serviceBuilder.getLowPriorityExecutor());
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
        String streamName = "abc";
        int port = TestUtils.getAvailableListenPort();
        String event = "Event\n";
        String routingKey = "RoutingKey";
        StreamSegmentStore store = this.serviceBuilder.createStreamSegmentService();
        TableStore tableStore = serviceBuilder.createTableStoreService();

        @Cleanup
        PravegaConnectionListener server = new PravegaConnectionListener(false, port, store, tableStore,
                this.serviceBuilder.getLowPriorityExecutor());
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
        String groupName = "group";
        String streamName = "abc";
        int port = TestUtils.getAvailableListenPort();
        String txnEvent = "TXN Event\n";
        String nonTxEvent = "Non-TX Event\n";
        String routingKey = "RoutingKey";
        StreamSegmentStore store = this.serviceBuilder.createStreamSegmentService();
        TableStore tableStore = serviceBuilder.createTableStoreService();

        @Cleanup
        PravegaConnectionListener server = new PravegaConnectionListener(false, port, store, tableStore,
                this.serviceBuilder.getLowPriorityExecutor());
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
    @Test(timeout = 30000)
    public void testDeleteStreamWithOpenTransaction() throws Exception {
        @Cleanup("stopAllServices")
        SetupUtils setupUtils = new SetupUtils();
        setupUtils.startAllServices(1);

        final String scope = setupUtils.getScope();
        final ClientConfig clientConfig = ClientConfig.builder()
                .controllerURI(setupUtils.getControllerUri())
                .build();

        @Cleanup
        final StreamManager streamManager = StreamManager.create(clientConfig);

        streamManager.createScope(scope);
        final String stream = "test";
        streamManager.createStream(scope, stream, StreamConfiguration.builder()
                .scalingPolicy(ScalingPolicy.fixed(3))
                .build());

        @Cleanup
        final EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(scope, clientConfig);

        @Cleanup
        final TransactionalEventStreamWriter<String> writer =
                clientFactory.createTransactionalEventWriter("writerId1", stream, new JavaSerializer<>(),
                        EventWriterConfig.builder().build());

        // Transactions 0-4 will be opened, written, flushed, committed.
        // Transactions 5-6 will be opened, written, flushed.
        // Transactions 7-8 will be opened, written.
        // Transactions 9-10 will be opened.
        for (int i = 0; i < 11; i++) {
            final Transaction<String> txn = writer.beginTxn();
            log.info("i={}, txnId={}", i, txn.getTxnId());
            if (i <= 8) {
                txn.writeEvent("foo");
            }
            if (i <= 6) {
                txn.flush();
            }
            if (i <= 4) {
                txn.commit();
            }
        }
        boolean sealed = streamManager.sealStream(scope, stream);
        Assert.assertTrue(sealed);
        boolean deleted = streamManager.deleteStream(scope, stream);
        Assert.assertTrue(deleted);
    }
}
