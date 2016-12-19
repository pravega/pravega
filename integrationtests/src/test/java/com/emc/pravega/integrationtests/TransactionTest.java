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

package com.emc.pravega.integrationtests;

import com.emc.pravega.service.contracts.StreamSegmentStore;
import com.emc.pravega.service.server.host.handler.PravegaConnectionListener;
import com.emc.pravega.service.server.store.ServiceBuilder;
import com.emc.pravega.service.server.store.ServiceBuilderConfig;
import com.emc.pravega.stream.Consumer;
import com.emc.pravega.stream.ConsumerConfig;
import com.emc.pravega.stream.Producer;
import com.emc.pravega.stream.ProducerConfig;
import com.emc.pravega.stream.Transaction;
import com.emc.pravega.stream.TxFailedException;
import com.emc.pravega.stream.impl.JavaSerializer;
import com.emc.pravega.stream.mock.MockClientFactory;
import com.emc.pravega.testcommon.AssertExtensions;
import com.emc.pravega.testcommon.TestUtils;

import java.io.Serializable;

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
        InternalLoggerFactory.setDefaultFactory(new Slf4JLoggerFactory());
        this.serviceBuilder = ServiceBuilder.newInMemoryBuilder(ServiceBuilderConfig.getDefaultConfig());
        this.serviceBuilder.initialize().get();
    }

    @After
    public void teardown() {
        this.serviceBuilder.close();
        ResourceLeakDetector.setLevel(originalLevel);
    }

    @Test
    public void testTransactionalWritesOrderedCorrectly() throws TxFailedException {
        int readTimeout = 5000;
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

        MockClientFactory clientFactory = new MockClientFactory("scope", endpoint, port);
        clientFactory.createStream(streamName, null);
        @Cleanup
        Producer<String> producer = clientFactory.createProducer(streamName,
                                                                 new JavaSerializer<>(),
                                                                 new ProducerConfig(null));
        producer.publish(routingKey, nonTxEvent);
        Transaction<String> transaction = producer.beginTransaction(60000);
        producer.publish(routingKey, nonTxEvent);
        transaction.publish(routingKey, txnEvent);
        producer.publish(routingKey, nonTxEvent);
        transaction.publish(routingKey, txnEvent);
        producer.flush();
        producer.publish(routingKey, nonTxEvent);
        transaction.publish(routingKey, txnEvent);
        producer.publish(routingKey, nonTxEvent);
        transaction.publish(routingKey, txnEvent);
        transaction.flush();
        producer.publish(routingKey, nonTxEvent);
        transaction.publish(routingKey, txnEvent);
        producer.flush();
        transaction.publish(routingKey, txnEvent);
        transaction.commit();
        producer.publish(routingKey, nonTxEvent);
        AssertExtensions.assertThrows(IllegalStateException.class,
                                      () -> transaction.publish(routingKey, txnEvent));

        Consumer<Serializable> consumer = clientFactory.createConsumer(streamName,
                                                                       new JavaSerializer<>(),
                                                                       new ConsumerConfig(),
                                                                       clientFactory.getInitialPosition(streamName));

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
    public void testDoubleCommit() throws TxFailedException {
        String endpoint = "localhost";
        String streamName = "abc";
        int port = TestUtils.randomPort();
        String event = "Event\n";
        String routingKey = "RoutingKey";
        StreamSegmentStore store = this.serviceBuilder.createStreamSegmentService();
        @Cleanup
        PravegaConnectionListener server = new PravegaConnectionListener(false, port, store);
        server.startListening();
        MockClientFactory clientFactory = new MockClientFactory("scope", endpoint, port);
        clientFactory.createStream(streamName, null);
        @Cleanup
        Producer<String> producer = clientFactory.createProducer(streamName, new JavaSerializer<>(), new ProducerConfig(null));
        Transaction<String> transaction = producer.beginTransaction(60000);
        transaction.publish(routingKey, event);
        transaction.commit();
        AssertExtensions.assertThrows(TxFailedException.class, () -> transaction.commit() );    
    }
    
    @Test
    public void testDrop() throws TxFailedException {
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
        MockClientFactory clientFactory = new MockClientFactory("scope", endpoint, port);
        clientFactory.createStream(streamName, null);
        @Cleanup
        Producer<String> producer = clientFactory.createProducer(streamName, new JavaSerializer<>(), new ProducerConfig(null));
   
        Transaction<String> transaction = producer.beginTransaction(60000);
        transaction.publish(routingKey, txnEvent);
        transaction.flush();
        transaction.drop();
        transaction.drop();
        AssertExtensions.assertThrows(IllegalStateException.class, () -> transaction.publish(routingKey, txnEvent));
        AssertExtensions.assertThrows(TxFailedException.class, () -> transaction.commit());
        
        Consumer<Serializable> consumer = clientFactory.createConsumer(streamName,
                                                                       new JavaSerializer<>(),
                                                                       new ConsumerConfig(),
                                                                       clientFactory.getInitialPosition(streamName));
        producer.publish(routingKey, nonTxEvent);
        producer.flush();
        assertEquals(nonTxEvent, consumer.readNextEvent(1500).getEvent());
    }
}
