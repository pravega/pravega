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
import io.pravega.segmentstore.contracts.Attributes;
import io.pravega.segmentstore.contracts.SegmentType;
import io.pravega.segmentstore.contracts.StreamSegmentStore;
import io.pravega.segmentstore.contracts.tables.TableStore;
import io.pravega.segmentstore.server.host.handler.IndexAppendProcessor;
import io.pravega.segmentstore.server.host.handler.PravegaConnectionListener;
import io.pravega.segmentstore.server.store.ServiceBuilder;
import io.pravega.segmentstore.server.store.ServiceBuilderConfig;
import io.pravega.shared.NameUtils;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.LeakDetectorTestSuite;
import io.pravega.test.common.TestUtils;
import java.io.Serializable;
import java.time.Duration;
import java.util.Collections;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

@Slf4j
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
                SERVICE_BUILDER.getLowPriorityExecutor(), new IndexAppendProcessor(SERVICE_BUILDER.getLowPriorityExecutor(), store));
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
    public void testDoubleCommit() throws Exception {
        String endpoint = "localhost";
        String streamName = "testDoubleCommit";
        int port = TestUtils.getAvailableListenPort();
        String event = "Event\n";
        String routingKey = "RoutingKey";
        StreamSegmentStore store = SERVICE_BUILDER.createStreamSegmentService();
        TableStore tableStore = SERVICE_BUILDER.createTableStoreService();

        @Cleanup
        PravegaConnectionListener server = new PravegaConnectionListener(false, port, store, tableStore,
                SERVICE_BUILDER.getLowPriorityExecutor(), new IndexAppendProcessor(SERVICE_BUILDER.getLowPriorityExecutor(), store));
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
        assertIndexSegment(store, NameUtils.getIndexSegmentName(NameUtils.getQualifiedStreamSegmentName("scope", streamName, 0)), 1, 24);
    }

    private void assertIndexSegment(StreamSegmentStore store, String segment, long expectedEventCount, long eventLength) throws Exception {
        AssertExtensions.assertEventuallyEquals(expectedEventCount,
                () -> store.getAttributes(segment, Collections.singleton(Attributes.EVENT_COUNT), true, Duration.ofMinutes(1))
                           .get().get(Attributes.EVENT_COUNT), 5000);
        val si = store.getStreamSegmentInfo(segment, Duration.ofMinutes(1)).join();
        val segmentType = SegmentType.fromAttributes(si.getAttributes());
        assertFalse(segmentType.isInternal() || segmentType.isCritical() || segmentType.isSystem() || segmentType.isTableSegment());
        assertEquals(SegmentType.STREAM_SEGMENT, segmentType);
        assertEquals(eventLength, si.getLength());
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
                SERVICE_BUILDER.getLowPriorityExecutor(), new IndexAppendProcessor(SERVICE_BUILDER.getLowPriorityExecutor(), store));
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
        String endpoint = "localhost";
        String scopeName = "scope";
        String streamName = "abc";
        int port = TestUtils.getAvailableListenPort();
        StreamSegmentStore store = SERVICE_BUILDER.createStreamSegmentService();
        TableStore tableStore = SERVICE_BUILDER.createTableStoreService();

        @Cleanup
        PravegaConnectionListener server = new PravegaConnectionListener(false, port, store, tableStore,
                SERVICE_BUILDER.getLowPriorityExecutor(), new IndexAppendProcessor(SERVICE_BUILDER.getLowPriorityExecutor(), store));
        server.startListening();
        @Cleanup
        MockStreamManager streamManager = new MockStreamManager(scopeName, endpoint, port);
        streamManager.createScope(scopeName);
        streamManager.createStream(scopeName, streamName, StreamConfiguration.builder().build());

        MockClientFactory clientFactory = streamManager.getClientFactory();

        @Cleanup
        final TransactionalEventStreamWriter<String> writer =
                clientFactory.createTransactionalEventWriter("writerId1", streamName, new JavaSerializer<>(),
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
        boolean sealed = streamManager.sealStream(scopeName, streamName);
        Assert.assertTrue(sealed);
        boolean deleted = streamManager.deleteStream(scopeName, streamName);
        Assert.assertTrue(deleted);
    }
}
