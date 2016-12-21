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
import com.emc.pravega.stream.EventStreamReader;
import com.emc.pravega.stream.ReaderConfig;
import com.emc.pravega.stream.EventStreamWriter;
import com.emc.pravega.stream.WriterConfig;
import com.emc.pravega.stream.Transaction;
import com.emc.pravega.stream.TxFailedException;
import com.emc.pravega.stream.impl.JavaSerializer;
import com.emc.pravega.stream.impl.StreamImpl;
import com.emc.pravega.stream.mock.MockStreamManager;
import com.emc.pravega.testcommon.AssertExtensions;
import com.emc.pravega.testcommon.TestUtils;
import io.netty.util.ResourceLeakDetector;
import io.netty.util.ResourceLeakDetector.Level;
import io.netty.util.internal.logging.InternalLoggerFactory;
import io.netty.util.internal.logging.Slf4JLoggerFactory;
import lombok.Cleanup;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.Serializable;

import static org.junit.Assert.assertEquals;

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
        @Cleanup
        MockStreamManager streamManager = new MockStreamManager("scope", endpoint, port);
        StreamImpl stream = (StreamImpl) streamManager.createStream(streamName, null);
        @Cleanup
        EventStreamWriter<String> writer = stream.createProducer(new JavaSerializer<>(), new WriterConfig(null));
        writer.writeEvent(routingKey, nonTxEvent);
        Transaction<String> transaction = writer.startTransaction(60000);
        writer.writeEvent(routingKey, nonTxEvent);
        transaction.writeEvent(routingKey, txnEvent);
        writer.writeEvent(routingKey, nonTxEvent);
        transaction.writeEvent(routingKey, txnEvent);
        writer.flush();
        writer.writeEvent(routingKey, nonTxEvent);
        transaction.writeEvent(routingKey, txnEvent);
        writer.writeEvent(routingKey, nonTxEvent);
        transaction.writeEvent(routingKey, txnEvent);
        transaction.flush();
        writer.writeEvent(routingKey, nonTxEvent);
        transaction.writeEvent(routingKey, txnEvent);
        writer.flush();
        transaction.writeEvent(routingKey, txnEvent);
        transaction.commit();
        writer.writeEvent(routingKey, nonTxEvent);
        AssertExtensions.assertThrows(IllegalStateException.class,
                                      () -> transaction.writeEvent(routingKey, txnEvent));
        EventStreamReader<Serializable> reader = stream.createConsumer(new JavaSerializer<>(), new ReaderConfig(), streamManager.getInitialPosition(streamName), null);
        assertEquals(nonTxEvent, reader.readNextEvent(readTimeout));
        assertEquals(nonTxEvent, reader.readNextEvent(readTimeout));
        assertEquals(nonTxEvent, reader.readNextEvent(readTimeout));
        assertEquals(nonTxEvent, reader.readNextEvent(readTimeout));
        assertEquals(nonTxEvent, reader.readNextEvent(readTimeout));
        assertEquals(nonTxEvent, reader.readNextEvent(readTimeout));

        assertEquals(txnEvent, reader.readNextEvent(readTimeout));
        assertEquals(txnEvent, reader.readNextEvent(readTimeout));
        assertEquals(txnEvent, reader.readNextEvent(readTimeout));
        assertEquals(txnEvent, reader.readNextEvent(readTimeout));
        assertEquals(txnEvent, reader.readNextEvent(readTimeout));
        assertEquals(txnEvent, reader.readNextEvent(readTimeout));

        assertEquals(nonTxEvent, reader.readNextEvent(readTimeout));
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
        @Cleanup
        MockStreamManager streamManager = new MockStreamManager("scope", endpoint, port);
        StreamImpl stream = (StreamImpl) streamManager.createStream(streamName, null);
        @Cleanup
        EventStreamWriter<String> writer = stream.createProducer(new JavaSerializer<>(), new WriterConfig(null));
        Transaction<String> transaction = writer.startTransaction(60000);
        transaction.writeEvent(routingKey, event);
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
        @Cleanup
        MockStreamManager streamManager = new MockStreamManager("scope", endpoint, port);
        StreamImpl stream = (StreamImpl) streamManager.createStream(streamName, null);
        @Cleanup
        EventStreamWriter<String> writer = stream.createProducer(new JavaSerializer<>(), new WriterConfig(null));
        Transaction<String> transaction = writer.startTransaction(60000);
        transaction.writeEvent(routingKey, txnEvent);
        transaction.flush();
        transaction.drop();
        transaction.drop();
        AssertExtensions.assertThrows(IllegalStateException.class, () -> transaction.writeEvent(routingKey, txnEvent));
        AssertExtensions.assertThrows(TxFailedException.class, () -> transaction.commit());
        
        EventStreamReader<Serializable> reader = stream.createConsumer(new JavaSerializer<>(), new ReaderConfig(), streamManager.getInitialPosition(streamName), null);
        writer.writeEvent(routingKey, nonTxEvent);
        writer.flush();
        assertEquals(nonTxEvent, reader.readNextEvent(1500));
    }
}
