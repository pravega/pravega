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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import io.pravega.segmentstore.server.host.handler.IndexAppendProcessor;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.pravega.client.ClientConfig;
import io.pravega.client.connection.impl.ConnectionPoolImpl;
import io.pravega.client.connection.impl.SocketConnectionFactoryImpl;
import io.pravega.client.security.auth.DelegationTokenProviderFactory;
import io.pravega.client.segment.impl.ConditionalOutputStream;
import io.pravega.client.segment.impl.ConditionalOutputStreamFactoryImpl;
import io.pravega.client.segment.impl.Segment;
import io.pravega.client.segment.impl.SegmentMetadataClient;
import io.pravega.client.segment.impl.SegmentMetadataClientFactoryImpl;
import io.pravega.client.segment.impl.SegmentOutputStream;
import io.pravega.client.segment.impl.SegmentOutputStreamFactoryImpl;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.control.impl.Controller;
import io.pravega.client.stream.impl.PendingEvent;
import io.pravega.client.stream.mock.MockController;
import io.pravega.common.concurrent.Futures;
import io.pravega.segmentstore.contracts.StreamSegmentStore;
import io.pravega.segmentstore.contracts.tables.TableStore;
import io.pravega.segmentstore.server.host.handler.PravegaConnectionListener;
import io.pravega.segmentstore.server.store.ServiceBuilder;
import io.pravega.segmentstore.server.store.ServiceBuilderConfig;
import io.pravega.shared.protocol.netty.WireCommands;
import io.pravega.test.common.LeakDetectorTestSuite;
import io.pravega.test.common.TestUtils;
import lombok.Cleanup;

public class AppendReconnectTest extends LeakDetectorTestSuite {
    private ServiceBuilder serviceBuilder;
    private final Consumer<Segment> segmentSealedCallback = segment -> { };

    @Before
    public void setup() throws Exception {
        this.serviceBuilder = ServiceBuilder.newInMemoryBuilder(ServiceBuilderConfig.getDefaultConfig());
        this.serviceBuilder.initialize();
    }

    @After
    public void teardown() {
        this.serviceBuilder.close();
    }

    @Test(timeout = 30000)
    public void reconnectOnSegmentClient() throws Exception {
        String endpoint = "localhost";
        int port = TestUtils.getAvailableListenPort();
        byte[] payload = "Hello world\n".getBytes();
        String scope = "scope";
        String stream = "stream";
        StreamSegmentStore store = this.serviceBuilder.createStreamSegmentService();

        @Cleanup
        PravegaConnectionListener server = new PravegaConnectionListener(false, port, store, mock(TableStore.class),
                serviceBuilder.getLowPriorityExecutor(), new IndexAppendProcessor(serviceBuilder.getLowPriorityExecutor(), store));
        server.startListening();

        @Cleanup
        SocketConnectionFactoryImpl clientCF = new SocketConnectionFactoryImpl(ClientConfig.builder().build());
        @Cleanup
        ConnectionPoolImpl connectionPool = new ConnectionPoolImpl(ClientConfig.builder().build(), clientCF);
        Controller controller = new MockController(endpoint, port, connectionPool, true);
        controller.createScope(scope);
        controller.createStream(scope, stream, StreamConfiguration.builder().build());

        SegmentOutputStreamFactoryImpl segmentClient = new SegmentOutputStreamFactoryImpl(controller, connectionPool);

        Segment segment = Futures.getAndHandleExceptions(controller.getCurrentSegments(scope, stream), RuntimeException::new).getSegments().iterator().next();
        @Cleanup
        SegmentOutputStream out = segmentClient.createOutputStreamForSegment(segment, segmentSealedCallback, EventWriterConfig.builder().build(),
                DelegationTokenProviderFactory.createWithEmptyToken());
        CompletableFuture<Void> ack = new CompletableFuture<>();
        out.write(PendingEvent.withoutHeader(null, ByteBuffer.wrap(payload), ack));
        for (AutoCloseable c : connectionPool.getActiveChannels()) {
            c.close();
        }
        CompletableFuture<Void> ack2 = new CompletableFuture<>();
        out.write(PendingEvent.withoutHeader(null, ByteBuffer.wrap(payload), ack2));
        ack.get(5, TimeUnit.SECONDS);
        ack2.get(5, TimeUnit.SECONDS);
        @Cleanup
        SegmentMetadataClient metadataClient = new SegmentMetadataClientFactoryImpl(controller, connectionPool).createSegmentMetadataClient(segment,
                DelegationTokenProviderFactory.createWithEmptyToken());
        assertEquals(payload.length * 2, metadataClient.fetchCurrentSegmentLength().join().longValue());
    }
    
    @Test(timeout = 30000)
    public void reconnectThroughConditionalClient() throws Exception {
        String endpoint = "localhost";
        int port = TestUtils.getAvailableListenPort();
        byte[] payload = "Hello world\n".getBytes();
        String scope = "scope";
        String stream = "stream";
        StreamSegmentStore store = this.serviceBuilder.createStreamSegmentService();

        @Cleanup
        PravegaConnectionListener server = new PravegaConnectionListener(false, port, store, mock(TableStore.class),
                serviceBuilder.getLowPriorityExecutor(), new IndexAppendProcessor(serviceBuilder.getLowPriorityExecutor(), store));
        server.startListening();

        @Cleanup
        SocketConnectionFactoryImpl clientCF = new SocketConnectionFactoryImpl(ClientConfig.builder().build());
        @Cleanup
        ConnectionPoolImpl connectionPool = new ConnectionPoolImpl(ClientConfig.builder().build(), clientCF);
        Controller controller = new MockController(endpoint, port, connectionPool, true);
        controller.createScope(scope);
        controller.createStream(scope, stream, StreamConfiguration.builder().build());

        ConditionalOutputStreamFactoryImpl segmentClient = new ConditionalOutputStreamFactoryImpl(controller, connectionPool);

        Segment segment = Futures.getAndHandleExceptions(controller.getCurrentSegments(scope, stream), RuntimeException::new).getSegments().iterator().next();
        @Cleanup
        ConditionalOutputStream out = segmentClient.createConditionalOutputStream(segment, DelegationTokenProviderFactory.createWithEmptyToken(), EventWriterConfig.builder().build());
        assertTrue(out.write(ByteBuffer.wrap(payload), 0));
        for (AutoCloseable c : connectionPool.getActiveChannels()) {
            c.close();
        }
        assertTrue(out.write(ByteBuffer.wrap(payload), payload.length + WireCommands.TYPE_PLUS_LENGTH_SIZE));
        @Cleanup
        SegmentMetadataClient metadataClient = new SegmentMetadataClientFactoryImpl(controller, connectionPool).createSegmentMetadataClient(segment,
                DelegationTokenProviderFactory.createWithEmptyToken());
        assertEquals((payload.length + WireCommands.TYPE_PLUS_LENGTH_SIZE) * 2,
                     metadataClient.fetchCurrentSegmentLength().join().longValue());
    }
}
