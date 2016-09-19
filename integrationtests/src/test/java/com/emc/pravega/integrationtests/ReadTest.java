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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.emc.pravega.common.netty.CommandDecoder;
import com.emc.pravega.common.netty.ConnectionFactory;
import com.emc.pravega.common.netty.WireCommands.ReadSegment;
import com.emc.pravega.common.netty.WireCommands.SegmentRead;
import com.emc.pravega.service.contracts.AppendContext;
import com.emc.pravega.service.contracts.ReadResult;
import com.emc.pravega.service.contracts.ReadResultEntry;
import com.emc.pravega.service.contracts.ReadResultEntryContents;
import com.emc.pravega.service.contracts.ReadResultEntryType;
import com.emc.pravega.service.contracts.StreamSegmentStore;
import com.emc.pravega.service.server.host.handler.PravegaConnectionListener;
import com.emc.pravega.service.server.mocks.InMemoryServiceBuilder;
import com.emc.pravega.service.server.store.ServiceBuilder;
import com.emc.pravega.service.server.store.ServiceBuilderConfig;
import com.emc.pravega.stream.Consumer;
import com.emc.pravega.stream.ConsumerConfig;
import com.emc.pravega.stream.Producer;
import com.emc.pravega.stream.ProducerConfig;
import com.emc.pravega.stream.Transaction;
import com.emc.pravega.stream.TxFailedException;
import com.emc.pravega.stream.impl.JavaSerializer;
import com.emc.pravega.stream.impl.SingleSegmentStreamImpl;
import com.emc.pravega.stream.impl.SingleSegmentStreamManagerImpl;
import com.emc.pravega.stream.impl.netty.ConnectionFactoryImpl;
import com.emc.pravega.stream.impl.segment.EndOfSegmentException;
import com.emc.pravega.stream.impl.segment.SegmentInputConfiguration;
import com.emc.pravega.stream.impl.segment.SegmentInputStream;
import com.emc.pravega.stream.impl.segment.SegmentOutputStream;
import com.emc.pravega.stream.impl.segment.SegmentSealedException;
import com.emc.pravega.stream.impl.segment.SingleSegmentStreamControllerImpl;

import static org.junit.Assert.*;

import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.util.ResourceLeakDetector;
import io.netty.util.ResourceLeakDetector.Level;
import io.netty.util.internal.logging.InternalLoggerFactory;
import io.netty.util.internal.logging.Slf4JLoggerFactory;
import lombok.Cleanup;

public class ReadTest {
    
    private Level originalLevel;
    private ServiceBuilder serviceBuilder;

    @Before
    public void setup() throws Exception {
        originalLevel = ResourceLeakDetector.getLevel();
        ResourceLeakDetector.setLevel(Level.PARANOID);
        InternalLoggerFactory.setDefaultFactory(new Slf4JLoggerFactory());
        this.serviceBuilder = new InMemoryServiceBuilder(ServiceBuilderConfig.getDefaultConfig());
        this.serviceBuilder.getContainerManager().initialize(Duration.ofMinutes(1)).get();
    }

    @After
    public void teardown() {
        this.serviceBuilder.close();
        ResourceLeakDetector.setLevel(originalLevel);
    }

    @Test
    public void testReadDirectlyFromStore() throws InterruptedException, ExecutionException, IOException {
        String segmentName = "testReadFromStore";
        int entries = 10;
        byte[] data = new byte[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9 };
        UUID clientId = UUID.randomUUID();

        StreamSegmentStore segmentStore = serviceBuilder.createStreamSegmentService();

        fillStoreForSegment(segmentName, clientId, data, entries, segmentStore);

        ReadResult result = segmentStore.read(segmentName, 0, entries * data.length, Duration.ZERO).get();
        int count = 0;
        while (result.hasNext()) {
            ReadResultEntry entry = result.next();
            ReadResultEntryType type = entry.getType();
            assertEquals(ReadResultEntryType.Cache, type);
            ReadResultEntryContents contents = entry.getContent().get();
            assertEquals(data.length, contents.getLength());
            byte[] entryData = new byte[data.length];
            contents.getData().read(entryData);
            assertArrayEquals(data, entryData);
            count++;
        }
        assertEquals(entries, count);
    }

    @Test
    public void testReceivingReadCall() throws Exception {
        String segmentName = "testReceivingReadCall";
        int entries = 10;
        byte[] data = new byte[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9 };
        UUID clientId = UUID.randomUUID();
        CommandDecoder decoder = new CommandDecoder();

        StreamSegmentStore segmentStore = serviceBuilder.createStreamSegmentService();

        fillStoreForSegment(segmentName, clientId, data, entries, segmentStore);

        EmbeddedChannel channel = AppendTest.createChannel(segmentStore);

        SegmentRead result = (SegmentRead) AppendTest.sendRequest(channel, decoder, new ReadSegment(segmentName, 0, 10000));

        assertEquals(result.getSegment(), segmentName);
        assertEquals(result.getOffset(), 0);
        assertTrue(result.isAtTail());
        assertFalse(result.isEndOfSegment());

        ByteBuffer expected = ByteBuffer.allocate(entries * data.length);
        for (int i = 0; i < entries; i++) {
            expected.put(data);
        }
        expected.rewind();
        assertEquals(expected, result.getData());
    }
    
    @Test
    public void readThroughSegmentClient() throws SegmentSealedException, EndOfSegmentException {
        String endpoint = "localhost";
        String segmentName = "abc";
        int port = 8765;
        String testString = "Hello world\n";
        StreamSegmentStore store = this.serviceBuilder.createStreamSegmentService();
        @Cleanup
        PravegaConnectionListener server = new PravegaConnectionListener(false, port, store);
        server.startListening();

        ConnectionFactory clientCF = new ConnectionFactoryImpl(false, port);
        SingleSegmentStreamControllerImpl segmentClient = new SingleSegmentStreamControllerImpl(endpoint, clientCF);
        segmentClient.createSegment(segmentName);
        @Cleanup("close")
        SegmentOutputStream out = segmentClient.openSegmentForAppending(segmentName, null);
        out.write(ByteBuffer.wrap(testString.getBytes()), new CompletableFuture<>());
        out.flush();
        
        @Cleanup("close")
        SegmentInputStream in = segmentClient.openSegmentForReading(segmentName, new SegmentInputConfiguration());
        ByteBuffer result = in.read();
        assertEquals(ByteBuffer.wrap(testString.getBytes()), result);
    }
    
    @Test
    public void readThroughStreamClient() {
        String endpoint = "localhost";
        String streamName = "abc";
        int port = 8910;
        String testString = "Hello world\n";
        StreamSegmentStore store = this.serviceBuilder.createStreamSegmentService();
        @Cleanup
        PravegaConnectionListener server = new PravegaConnectionListener(false, port, store);
        server.startListening();
        @Cleanup
        SingleSegmentStreamManagerImpl streamManager = new SingleSegmentStreamManagerImpl(endpoint, port, "Scope");
        SingleSegmentStreamImpl stream = (SingleSegmentStreamImpl) streamManager.createStream(streamName, null);
        JavaSerializer<String> serializer = new JavaSerializer<>();
        @Cleanup
        Producer<String> producer = stream.createProducer(serializer, new ProducerConfig(null));
        producer.publish("RoutingKey", testString);
        producer.flush();
        
        @Cleanup
        Consumer<String> consumer = stream.createConsumer(serializer, new ConsumerConfig());
        String read = consumer.getNextEvent(5000);
        assertEquals(testString, read);
    }
    
    @Test
    public void readDataWrittenTransactionally() throws TxFailedException {
        String endpoint = "localhost";
        String streamName = "abc";
        int port = 8910;
        String testString = "Hello world\n";
        StreamSegmentStore store = this.serviceBuilder.createStreamSegmentService();
        @Cleanup
        PravegaConnectionListener server = new PravegaConnectionListener(false, port, store);
        server.startListening();
        @Cleanup
        SingleSegmentStreamManagerImpl streamManager = new SingleSegmentStreamManagerImpl(endpoint, port, "Scope");
        SingleSegmentStreamImpl stream = (SingleSegmentStreamImpl) streamManager.createStream(streamName, null);
        JavaSerializer<String> serializer = new JavaSerializer<>();
        @Cleanup
        Producer<String> producer = stream.createProducer(serializer, new ProducerConfig(null));
        Transaction<String> transaction = producer.startTransaction(60000);
        transaction.publish("RoutingKey", testString);
        transaction.commit();
        
        @Cleanup
        Consumer<String> consumer = stream.createConsumer(serializer, new ConsumerConfig());
        String read = consumer.getNextEvent(5000);
        assertEquals(testString, read);
    }
    

    private void fillStoreForSegment(String segmentName, UUID clientId, byte[] data, int numEntries,
            StreamSegmentStore segmentStore) {
        segmentStore.createStreamSegment(segmentName, Duration.ZERO);
        for (int eventNumber = 1; eventNumber <= numEntries; eventNumber++) {
            AppendContext appendContext = new AppendContext(clientId, eventNumber);
            try {
                segmentStore.append(segmentName, data, appendContext, Duration.ZERO).get();
            } catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
