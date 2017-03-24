/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.integrationtests;

import com.emc.pravega.service.contracts.StreamSegmentStore;
import com.emc.pravega.service.server.host.handler.PravegaConnectionListener;
import com.emc.pravega.service.server.store.ServiceBuilder;
import com.emc.pravega.service.server.store.ServiceBuilderConfig;
import com.emc.pravega.stream.EventStreamReader;
import com.emc.pravega.stream.EventStreamWriter;
import com.emc.pravega.stream.EventWriterConfig;
import com.emc.pravega.stream.ReaderConfig;
import com.emc.pravega.stream.ReaderGroupConfig;
import com.emc.pravega.stream.ReinitializationRequiredException;
import com.emc.pravega.stream.StreamConfiguration;
import com.emc.pravega.stream.Transaction;
import com.emc.pravega.stream.TxnFailedException;
import com.emc.pravega.stream.impl.JavaSerializer;
import com.emc.pravega.stream.mock.MockStreamManager;
import com.emc.pravega.testcommon.AssertExtensions;
import com.emc.pravega.testcommon.TestUtils;

import java.io.Serializable;
import java.util.Collections;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

import io.netty.util.ResourceLeakDetector;
import io.netty.util.ResourceLeakDetector.Level;
import io.netty.util.internal.logging.InternalLoggerFactory;
import io.netty.util.internal.logging.Slf4JLoggerFactory;
import lombok.Cleanup;

public class TransactionTest {
    private Level originalLevel;
    private ServiceBuilder serviceBuilder;

    @Before
    public void setup() throws Exception {
        originalLevel = ResourceLeakDetector.getLevel();
        ResourceLeakDetector.setLevel(Level.PARANOID);
        InternalLoggerFactory.setDefaultFactory(Slf4JLoggerFactory.INSTANCE);
        this.serviceBuilder = ServiceBuilder.newInMemoryBuilder(ServiceBuilderConfig.getDefaultConfig());
        this.serviceBuilder.initialize().get();
    }

    @After
    public void teardown() {
        this.serviceBuilder.close();
        ResourceLeakDetector.setLevel(originalLevel);
    }

    @Test
    public void testTransactionalWritesOrderedCorrectly() throws TxnFailedException, ReinitializationRequiredException {
        int readTimeout = 5000;
        String readerName = "reader";
        String groupName = "group";
        String endpoint = "localhost";
        String streamName = "abc";
        int port = TestUtils.randomPort();
        String txnEvent = "TXN Event\n";
        String nonTxEvent = "Non-TX Event\n";
        String routingKey = "RoutingKey";
        StreamSegmentStore store = this.serviceBuilder.createStreamSegmentService();
        @Cleanup
        PravegaConnectionListener server = new PravegaConnectionListener(false, port, store);
        server.startListening();
        @Cleanup
        MockStreamManager streamManager = new MockStreamManager("scope", endpoint, port);
        streamManager.createScope("scope");
        streamManager.createStream("scope", streamName, StreamConfiguration.builder().build());
        streamManager.createReaderGroup(groupName, ReaderGroupConfig.builder().build(), Collections.singleton(streamName));
        @Cleanup
        EventStreamWriter<String> producer = streamManager.getClientFactory()
                                                          .createEventWriter(streamName,
                                                                             new JavaSerializer<>(),
                                                                             EventWriterConfig.builder().build());
        producer.writeEvent(routingKey, nonTxEvent);
        Transaction<String> transaction = producer.beginTxn(60000, 60000, 60000);
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
        AssertExtensions.assertThrows(IllegalStateException.class, () -> transaction.writeEvent(routingKey, txnEvent));

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

    @Test
    public void testDoubleCommit() throws TxnFailedException {
        String endpoint = "localhost";
        String streamName = "abc";
        int port = TestUtils.randomPort();
        String event = "Event\n";
        String routingKey = "RoutingKey";
        StreamSegmentStore store = this.serviceBuilder.createStreamSegmentService();
        @Cleanup
        PravegaConnectionListener server = new PravegaConnectionListener(false, port, store);
        server.startListening();
        @Cleanup
        MockStreamManager streamManager = new MockStreamManager("scope", endpoint, port);
        streamManager.createScope("scope");
        streamManager.createStream("scope", streamName, null);
        @Cleanup
        EventStreamWriter<String> producer = streamManager.getClientFactory()
                                                          .createEventWriter(streamName,
                                                                             new JavaSerializer<>(),
                                                                             EventWriterConfig.builder().build());
        Transaction<String> transaction = producer.beginTxn(60000, 60000, 60000);
        transaction.writeEvent(routingKey, event);
        transaction.commit();
        AssertExtensions.assertThrows(TxnFailedException.class, () -> transaction.commit());
    }

    @Test
    public void testDrop() throws TxnFailedException, ReinitializationRequiredException {
        String endpoint = "localhost";
        String groupName = "group";
        String streamName = "abc";
        int port = TestUtils.randomPort();
        String txnEvent = "TXN Event\n";
        String nonTxEvent = "Non-TX Event\n";
        String routingKey = "RoutingKey";
        StreamSegmentStore store = this.serviceBuilder.createStreamSegmentService();
        @Cleanup
        PravegaConnectionListener server = new PravegaConnectionListener(false, port, store);
        server.startListening();
        @Cleanup
        MockStreamManager streamManager = new MockStreamManager("scope", endpoint, port);
        streamManager.createScope("scope");
        streamManager.createStream("scope", streamName, StreamConfiguration.builder().build());
        streamManager.createReaderGroup(groupName, ReaderGroupConfig.builder().build(), Collections.singleton(streamName));
        @Cleanup
        EventStreamWriter<String> producer = streamManager.getClientFactory()
                                                          .createEventWriter(streamName,
                                                                             new JavaSerializer<>(),
                                                                             EventWriterConfig.builder().build());

        Transaction<String> transaction = producer.beginTxn(60000, 60000, 60000);
        transaction.writeEvent(routingKey, txnEvent);
        transaction.flush();
        transaction.abort();
        transaction.abort();

        AssertExtensions.assertThrows(IllegalStateException.class, () -> transaction.writeEvent(routingKey, txnEvent));
        AssertExtensions.assertThrows(TxnFailedException.class, () -> transaction.commit());
        @Cleanup
        EventStreamReader<Serializable> consumer = streamManager.getClientFactory().createReader("reader",
                                                                                                 groupName,
                                                                                                 new JavaSerializer<>(),
                                                                                                 ReaderConfig.builder().build());
        producer.writeEvent(routingKey, nonTxEvent);
        producer.flush();
        assertEquals(nonTxEvent, consumer.readNextEvent(1500).getEvent());
    }
}
