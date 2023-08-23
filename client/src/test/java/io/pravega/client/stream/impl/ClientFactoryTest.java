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
package io.pravega.client.stream.impl;


import io.pravega.client.ClientConfig;
import io.pravega.client.admin.impl.ReaderGroupManagerImpl;
import io.pravega.client.connection.impl.ConnectionFactory;
import io.pravega.client.connection.impl.ConnectionPoolImpl;
import io.pravega.client.control.impl.Controller;
import io.pravega.client.segment.impl.ConditionalOutputStreamFactory;
import io.pravega.client.segment.impl.Segment;
import io.pravega.client.segment.impl.SegmentInputStreamFactory;
import io.pravega.client.segment.impl.SegmentMetadataClientFactory;
import io.pravega.client.segment.impl.SegmentOutputStream;
import io.pravega.client.segment.impl.SegmentOutputStreamFactory;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.state.StateSynchronizer;
import io.pravega.client.state.SynchronizerConfig;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import io.pravega.client.stream.ReaderConfig;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.mock.MockClientFactory;
import io.pravega.client.stream.mock.MockController;
import io.pravega.client.stream.mock.MockSegmentStreamFactory;
import io.pravega.shared.NameUtils;
import lombok.Cleanup;
import lombok.val;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static io.pravega.client.stream.impl.ReaderGroupImpl.getEndSegmentsForStreams;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class ClientFactoryTest {

    @Mock
    private ConnectionFactory connectionFactory;
    @Mock
    private Controller controllerClient;
    @Mock
    private SegmentInputStreamFactory inFactory;
    @Mock
    private SegmentOutputStreamFactory outFactory;
    @Mock
    private ConditionalOutputStreamFactory condFactory;
    @Mock
    private SegmentMetadataClientFactory metaFactory;

    @Test
    public void testCloseWithExternalController() {
        ClientFactoryImpl clientFactory = new ClientFactoryImpl("scope", controllerClient, ClientConfig.builder().build());
        clientFactory.close();
        verify(controllerClient, times(1)).close();
    }

    @Test
    public void testCloseWithExternalControllerConnectionFactory() {
        ClientFactoryImpl clientFactory = new ClientFactoryImpl("scope", controllerClient, connectionFactory);
        clientFactory.close();
        verify(connectionFactory, times(1)).close();
        verify(controllerClient, times(1)).close();
    }

    @Test
    public void testEventWriter() {
        String scope = "scope";
        String stream = "stream1";
        // setup mocks
        @Cleanup
        ClientFactoryImpl clientFactory = new ClientFactoryImpl(scope, controllerClient,  new ConnectionPoolImpl(ClientConfig.builder().build(), connectionFactory), inFactory, outFactory, condFactory, metaFactory);
        NavigableMap<Double, SegmentWithRange> segments = new TreeMap<>();
        Segment segment = new Segment(scope, stream, 0L);
        segments.put(1.0, new SegmentWithRange(segment, 0.0, 1.0));
        StreamSegments currentSegments = new StreamSegments(segments);
        SegmentOutputStream outStream = mock(SegmentOutputStream.class);
        when(controllerClient.getCurrentSegments(scope, stream))
                .thenReturn(CompletableFuture.completedFuture(currentSegments));
        when(outFactory.createOutputStreamForSegment(eq(segment), any(), any(), any())).thenReturn(outStream);

        EventWriterConfig writerConfig = EventWriterConfig.builder().build();
        @Cleanup
        EventStreamWriter<String> writer = clientFactory.createEventWriter(stream, new JavaSerializer<String>(), writerConfig);
        assertEquals(writerConfig, writer.getConfig());
    }

    @Test(expected = IllegalStateException.class)
    public void testEventWriterSealedStream() {
        String scope = "scope";
        String stream = "stream1";
        // setup mocks
        @Cleanup
        ClientFactoryImpl clientFactory = new ClientFactoryImpl(scope, controllerClient, connectionFactory);
        StreamSegments currentSegments = new StreamSegments(new TreeMap<>());
        when(controllerClient.getCurrentSegments(scope, stream))
                .thenReturn(CompletableFuture.completedFuture(currentSegments));

        EventWriterConfig writerConfig = EventWriterConfig.builder().build();
        @Cleanup
        EventStreamWriter<String> writer = clientFactory.createEventWriter(stream, new JavaSerializer<String>(), writerConfig);
        assertEquals(writerConfig, writer.getConfig());
        writer.close();
    }

    @Test
    public void testTxnWriter() {
        @Cleanup
        ClientFactoryImpl clientFactory = new ClientFactoryImpl("scope", controllerClient, connectionFactory);
        EventWriterConfig writerConfig = EventWriterConfig.builder().build();
        val txnWriter = clientFactory.createTransactionalEventWriter("writer1", "stream1", new JavaSerializer<String>(), writerConfig);
        assertEquals(writerConfig, txnWriter.getConfig());
        val txnWriter2 = clientFactory.createTransactionalEventWriter( "stream1", new JavaSerializer<String>(), writerConfig);
        assertEquals(writerConfig, txnWriter2.getConfig());
    }

    @Test
    public void testCreateReaders() {
        String scope = "scope";
        String streamName = "streamtest";
        Stream stream = Stream.of(scope, streamName);
        String groupName = "readerGroup";
        String readerGroupStream = NameUtils.getStreamForReaderGroup(groupName);

        //Create factories
        MockSegmentStreamFactory segmentStreamFactory = new MockSegmentStreamFactory();
        @Cleanup
        MockClientFactory clientFactory = new MockClientFactory(scope, segmentStreamFactory);
        MockController controller = (MockController) clientFactory.getController();

        //Create streams
        controller.createScope(scope).join();
        controller.createStream(scope, streamName,
                StreamConfiguration.builder().scalingPolicy(ScalingPolicy.fixed(2)).build());
        controller.createStream(scope, readerGroupStream,
                StreamConfiguration.builder().scalingPolicy(ScalingPolicy.fixed(1)).build());

        //Reader group state synchronizer
        ReaderGroupConfig config = ReaderGroupConfig.builder().disableAutomaticCheckpoints().stream(stream).build();
        StateSynchronizer<ReaderGroupState> sync = clientFactory.createStateSynchronizer(readerGroupStream,
                new ReaderGroupManagerImpl.ReaderGroupStateUpdatesSerializer(),
                new ReaderGroupManagerImpl.ReaderGroupStateInitSerializer(),
                SynchronizerConfig.builder()
                        .build());

        Map<SegmentWithRange, Long> segments = ReaderGroupImpl.getSegmentsForStreams(controller, config);
        sync.initialize(new ReaderGroupState.ReaderGroupStateInit(config,
                segments,
                getEndSegmentsForStreams(config), false));
        ReaderConfig readerConfig = ReaderConfig.builder().build();
        val reader = clientFactory.createReader("readerId", "readerGroup", new JavaSerializer<>(), readerConfig);
        assertEquals(readerConfig, reader.getConfig());
    }

    @Test
    public void testCreateReadersWithClientConfig() {
        String scope = "scope";
        String streamName = "streamtest";
        Stream stream = Stream.of(scope, streamName);
        String groupName = "readerGroup";
        String readerGroupStream = NameUtils.getStreamForReaderGroup(groupName);

        //Create factories
        MockSegmentStreamFactory segmentStreamFactory = new MockSegmentStreamFactory();
        @Cleanup
        MockClientFactory clientFactory = new MockClientFactory(scope, segmentStreamFactory);
        MockController controller = (MockController) clientFactory.getController();

        //Create streams
        controller.createScope(scope).join();
        controller.createStream(scope, streamName,
                StreamConfiguration.builder().scalingPolicy(ScalingPolicy.fixed(2)).build());
        controller.createStream(scope, readerGroupStream,
                StreamConfiguration.builder().scalingPolicy(ScalingPolicy.fixed(1)).build());

        //Reader group state synchronizer
        ReaderGroupConfig config = ReaderGroupConfig.builder().disableAutomaticCheckpoints().stream(stream).build();
        StateSynchronizer<ReaderGroupState> sync = clientFactory.createStateSynchronizer(readerGroupStream,
                new ReaderGroupManagerImpl.ReaderGroupStateUpdatesSerializer(),
                new ReaderGroupManagerImpl.ReaderGroupStateInitSerializer(),
                SynchronizerConfig.builder()
                        .build());

        Map<SegmentWithRange, Long> segments = ReaderGroupImpl.getSegmentsForStreams(controller, config);
        sync.initialize(new ReaderGroupState.ReaderGroupStateInit(config,
                segments,
                getEndSegmentsForStreams(config), false));
        ReaderConfig readerConfig = ReaderConfig.builder().build();
        val reader = clientFactory.createReader("readerId", "readerGroup", new JavaSerializer<>(), readerConfig, ClientConfig.builder().build());
        assertEquals(readerConfig, reader.getConfig());
    }

    @Test
    public void testGetters() {
        @Cleanup
        ClientFactoryImpl clientFactory = new ClientFactoryImpl("scope", controllerClient, connectionFactory);
        assertNotNull(clientFactory.getConnectionPool());
        assertNotNull(clientFactory.getController());
    }
    
}
