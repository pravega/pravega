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

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.pravega.client.ClientConfig;
import io.pravega.client.admin.KeyValueTableManager;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.admin.impl.KeyValueTableManagerImpl;
import io.pravega.client.admin.impl.ReaderGroupManagerImpl;
import io.pravega.client.admin.impl.StreamManagerImpl;
import io.pravega.client.connection.impl.ConnectionFactory;
import io.pravega.client.connection.impl.ConnectionPool;
import io.pravega.client.connection.impl.ConnectionPoolImpl;
import io.pravega.client.connection.impl.SocketConnectionFactoryImpl;
import io.pravega.client.control.impl.Controller;
import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.tables.KeyValueTableConfiguration;
import io.pravega.common.util.AsyncIterator;
import io.pravega.segmentstore.contracts.StreamSegmentStore;
import io.pravega.segmentstore.contracts.tables.TableStore;
import io.pravega.segmentstore.server.host.handler.IndexAppendProcessor;
import io.pravega.segmentstore.server.host.handler.PravegaConnectionListener;
import io.pravega.segmentstore.server.store.ServiceBuilder;
import io.pravega.segmentstore.server.store.ServiceBuilderConfig;
import io.pravega.shared.NameUtils;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.TestUtils;
import io.pravega.test.common.TestingServerStarter;
import io.pravega.test.integration.utils.ControllerWrapper;
import lombok.Cleanup;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.net.URI;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.common.collect.Lists.newArrayList;
import static io.pravega.shared.NameUtils.getScopedStreamName;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class ScopeTest {
    private final int controllerPort = TestUtils.getAvailableListenPort();
    private final String serviceHost = "localhost";
    private final int servicePort = TestUtils.getAvailableListenPort();
    private final int containerCount = 4;
    private TestingServer zkTestServer;
    private PravegaConnectionListener server;
    private ControllerWrapper controllerWrapper;
    private ServiceBuilder serviceBuilder;

    @Before
    public void setUp() throws Exception {
        zkTestServer = new TestingServerStarter().start();

        serviceBuilder = ServiceBuilder.newInMemoryBuilder(ServiceBuilderConfig.getDefaultConfig());
        serviceBuilder.initialize();
        StreamSegmentStore store = serviceBuilder.createStreamSegmentService();
        TableStore tableStore = serviceBuilder.createTableStoreService();
        server = new PravegaConnectionListener(false, servicePort, store, tableStore, serviceBuilder.getLowPriorityExecutor(),
                new IndexAppendProcessor(serviceBuilder.getLowPriorityExecutor(), store));
        server.startListening();

        controllerWrapper = new ControllerWrapper(zkTestServer.getConnectString(),
                false,
                controllerPort,
                serviceHost,
                servicePort,
                containerCount);
        controllerWrapper.awaitRunning();
    }

    @After
    public void tearDown() throws Exception {
        controllerWrapper.close();
        server.close();
        serviceBuilder.close();
        zkTestServer.close();
    }

    @Test(timeout = 30000)
    public void testListStreamForTag() {
        final String scope = "sc";
        final String stream1 = "s1";
        final String stream2 = "s2";
        final String stream3 = "s3";
        final Set<String> tagSet1 = Set.of("t1", "t2", "t3");
        final Set<String> tagSet2 = Set.of("t2", "t3", "t4");
        final Set<String> tagSet3 = Set.of("t3", "t4", "t5");
        StreamConfiguration cfg = StreamConfiguration.builder()
                                                     .scalingPolicy(ScalingPolicy.byDataRate(10, 2, 4))
                                                     .build();

        @Cleanup
        StreamManager manager = StreamManager.create(URI.create("tcp://localhost:" + this.controllerPort));
        manager.createScope(scope);
        // fetch tags of a non-existent stream.
        AssertExtensions.assertThrows("Non-existent Stream",
                                      () -> manager.getStreamTags(scope, stream1),
                                      t -> t instanceof StatusRuntimeException && (((StatusRuntimeException) t).getStatus().getCode().equals(Status.Code.NOT_FOUND)));

        manager.createStream(scope, stream1, cfg.toBuilder().tags(tagSet1).build());
        manager.createStream(scope, stream2, cfg.toBuilder().tags(tagSet2).build());
        manager.createStream(scope, stream3, cfg.toBuilder().tags(tagSet3).build());
        assertEquals(tagSet1, manager.getStreamTags(scope, stream1));
        assertEquals(tagSet2, manager.getStreamTags(scope, stream2));
        assertEquals(tagSet3, manager.getStreamTags(scope, stream3));
        assertEquals(singletonList(Stream.of(scope, stream1)), newArrayList(manager.listStreams(scope, "t1")));
        List<Stream> listedStreams = newArrayList(manager.listStreams(scope, "t3"));
        List<Stream> expectedStreams = Arrays.asList(Stream.of(scope, stream3), Stream.of(scope, stream2), Stream.of(scope, stream1));
        assertTrue((listedStreams.size() == expectedStreams.size()) && listedStreams.containsAll(expectedStreams) &&
                           expectedStreams.containsAll(listedStreams));

        // update a stream tag and verify if it is reflected.
        manager.updateStream(scope, stream3, cfg.toBuilder().clearTags().tag("t4").tag("t5").build());
        listedStreams = newArrayList(manager.listStreams(scope, "t3"));
        expectedStreams = Arrays.asList(Stream.of(scope, stream2), Stream.of(scope, stream1));
        assertTrue((listedStreams.size() == expectedStreams.size()) && listedStreams.containsAll(expectedStreams) &&
                           expectedStreams.containsAll(listedStreams));

        // seal and delete stream
        manager.sealStream(scope, stream2);
        manager.deleteStream(scope, stream2);
        //check if list streams is updated.
        assertEquals(singletonList(Stream.of(scope, stream1)), newArrayList(manager.listStreams(scope, "t3")));

        manager.sealStream(scope, stream1);
        manager.deleteStream(scope, stream1);
        assertEquals(emptyList(), newArrayList(manager.listStreams(scope, "t3")));

        manager.sealStream(scope, stream3);
        manager.deleteStream(scope, stream3);
        assertEquals(emptyList(), newArrayList(manager.listStreams(scope, "t4")));
    }

    @Test(timeout = 30000)
    public void testListStreams() throws Exception {
        final String scope = "test";
        final String streamName1 = "test1";
        final String streamName2 = "test2";
        final String streamName3 = "test3";
        final Map<String, Integer> foundCount = new HashMap<>();
        foundCount.put(streamName1, 0);
        foundCount.put(streamName2, 0);
        foundCount.put(streamName3, 0);
        foundCount.put(NameUtils.getMarkStreamForStream(streamName1), 0);
        foundCount.put(NameUtils.getMarkStreamForStream(streamName2), 0);
        foundCount.put(NameUtils.getMarkStreamForStream(streamName3), 0);
        StreamConfiguration config = StreamConfiguration.builder()
                                                        .scalingPolicy(ScalingPolicy.byEventRate(10, 2, 1))
                                                        .build();

        @Cleanup
        Controller controller = controllerWrapper.getController();
        ClientConfig clientConfig = ClientConfig.builder().controllerURI(URI.create("tcp://localhost")).build();
        @Cleanup
        ConnectionPool cp = new ConnectionPoolImpl(clientConfig, new SocketConnectionFactoryImpl(clientConfig));

        controllerWrapper.getControllerService().createScope(scope, 0L).get();
        controller.createStream(scope, streamName1, config).get();
        controller.createStream(scope, streamName2, config).get();
        controller.createStream(scope, streamName3, config).get();
        @Cleanup
        StreamManager manager = new StreamManagerImpl(controller, cp);

        Iterator<Stream> iterator = manager.listStreams(scope);
        assertTrue(iterator.hasNext());
        Stream next = iterator.next();
        foundCount.computeIfPresent(next.getStreamName(), (x, y) -> ++y);

        assertTrue(iterator.hasNext());
        next = iterator.next();
        foundCount.computeIfPresent(next.getStreamName(), (x, y) -> ++y);

        assertTrue(iterator.hasNext());
        next = iterator.next();
        foundCount.computeIfPresent(next.getStreamName(), (x, y) -> ++y);

        assertTrue(iterator.hasNext());
        next = iterator.next();
        foundCount.computeIfPresent(next.getStreamName(), (x, y) -> ++y);

        assertTrue(iterator.hasNext());
        next = iterator.next();
        foundCount.computeIfPresent(next.getStreamName(), (x, y) -> ++y);

        assertTrue(iterator.hasNext());
        next = iterator.next();
        foundCount.computeIfPresent(next.getStreamName(), (x, y) -> ++y);

        assertFalse(iterator.hasNext());

        assertTrue(foundCount.entrySet().stream().allMatch(x -> x.getValue() == 1));

        AsyncIterator<Stream> asyncIterator = controller.listStreams(scope);
        next = asyncIterator.getNext().join();
        foundCount.computeIfPresent(next.getStreamName(), (x, y) -> ++y);

        next = asyncIterator.getNext().join();
        foundCount.computeIfPresent(next.getStreamName(), (x, y) -> ++y);

        next = asyncIterator.getNext().join();
        foundCount.computeIfPresent(next.getStreamName(), (x, y) -> ++y);

        next = asyncIterator.getNext().join();
        foundCount.computeIfPresent(next.getStreamName(), (x, y) -> ++y);

        next = asyncIterator.getNext().join();
        foundCount.computeIfPresent(next.getStreamName(), (x, y) -> ++y);

        next = asyncIterator.getNext().join();
        foundCount.computeIfPresent(next.getStreamName(), (x, y) -> ++y);

        next = asyncIterator.getNext().join();
        assertNull(next);
        
        assertTrue(foundCount.entrySet().stream().allMatch(x -> x.getValue() == 2));
    }

    @Test
    public void testForceDeleteScope() throws Exception {
        final String scope = "test";
        final String streamName1 = "test1";
        final String streamName2 = "test2";
        final String streamName3 = "test3";
        final String kvtName1 = "kvt1";
        final String kvtName2 = "kvt2";
        final String groupName1 = "rg1";
        final String groupName2 = "rg2";

        StreamConfiguration config = StreamConfiguration.builder()
                .scalingPolicy(ScalingPolicy.fixed(1))
                .build();

        @Cleanup
        Controller controller = controllerWrapper.getController();
        ClientConfig clientConfig = ClientConfig.builder().controllerURI(URI.create("tcp://localhost:" + controllerPort)).build();
        @Cleanup
        ConnectionPool cp = new ConnectionPoolImpl(clientConfig, new SocketConnectionFactoryImpl(clientConfig));
        @Cleanup
        ConnectionFactory connectionFactory = new SocketConnectionFactoryImpl(clientConfig);

        controllerWrapper.getControllerService().createScope(scope, 0L).get();
        controller.createStream(scope, streamName1, config).get();
        controller.createStream(scope, streamName2, config).get();
        controller.createStream(scope, streamName3, config).get();
        @Cleanup
        StreamManager streamManager = new StreamManagerImpl(controller, cp);
        @Cleanup
        KeyValueTableManager keyValueTableManager = new KeyValueTableManagerImpl(clientConfig);
        @Cleanup
        ReaderGroupManager readerGroupManager = new ReaderGroupManagerImpl(scope, clientConfig, connectionFactory);

        KeyValueTableConfiguration kvtConfig = KeyValueTableConfiguration.builder().partitionCount(2).primaryKeyLength(4).secondaryKeyLength(4).build();
        keyValueTableManager.createKeyValueTable(scope, kvtName1, kvtConfig);
        keyValueTableManager.createKeyValueTable(scope, kvtName2, kvtConfig);

        readerGroupManager.createReaderGroup(groupName1, ReaderGroupConfig.builder()
                .stream(getScopedStreamName(scope, streamName1)).build());
        readerGroupManager.createReaderGroup(groupName2, ReaderGroupConfig.builder()
                .stream(getScopedStreamName(scope, streamName2)).build());

        assertTrue(streamManager.deleteScopeRecursive(scope));
    }

    @Test
    public void testDeleteScopeRecursive() throws Exception {
        final String scope = "testDeleteScope";
        final String testFalseScope = "falseScope";
        final String streamName1 = "test1";
        final String streamName2 = "test2";
        final String streamName3 = "test3";
        final String streamName4 = "test4";
        final String kvtName1 = "kvt1";
        final String kvtName2 = "kvt2";
        final String kvtName3 = "kvt3";
        final String groupName1 = "rg1";
        final String groupName2 = "rg2";
        final String groupName3 = "rg3";

        StreamConfiguration config = StreamConfiguration.builder()
                .scalingPolicy(ScalingPolicy.fixed(1))
                .build();
        @Cleanup
        Controller controller = controllerWrapper.getController();
        ClientConfig clientConfig = ClientConfig.builder().controllerURI(URI.create("tcp://localhost:" + controllerPort)).build();
        @Cleanup
        ConnectionPool cp = new ConnectionPoolImpl(clientConfig, new SocketConnectionFactoryImpl(clientConfig));
        @Cleanup
        ConnectionFactory connectionFactory = new SocketConnectionFactoryImpl(clientConfig);

        // Create scope
        controllerWrapper.getControllerService().createScope(scope, 0L).get();
        assertTrue(controller.checkScopeExists(scope).get());

        // Create streams
        assertTrue(controller.createStream(scope, streamName1, config).get());
        assertTrue(controller.createStream(scope, streamName2, config).get());
        assertTrue(controller.createStream(scope, streamName3, config).get());

        @Cleanup
        StreamManager streamManager = new StreamManagerImpl(controller, cp);
        @Cleanup
        KeyValueTableManager keyValueTableManager = new KeyValueTableManagerImpl(clientConfig);
        @Cleanup
        ReaderGroupManager readerGroupManager = new ReaderGroupManagerImpl(scope, clientConfig, connectionFactory);

        // 1. Call deleteScopeRecursive() without creating a scope
        assertTrue(streamManager.deleteScopeRecursive(testFalseScope));

        // Create KVT under the scope
        KeyValueTableConfiguration kvtConfig = KeyValueTableConfiguration.builder().partitionCount(2).primaryKeyLength(4).secondaryKeyLength(4).build();
        assertTrue(keyValueTableManager.createKeyValueTable(scope, kvtName1, kvtConfig));
        assertTrue(keyValueTableManager.createKeyValueTable(scope, kvtName2, kvtConfig));

        // Create RG under the same scope
        assertTrue(readerGroupManager.createReaderGroup(groupName1, ReaderGroupConfig.builder()
                .stream(getScopedStreamName(scope, streamName1)).build()));
        assertTrue(readerGroupManager.createReaderGroup(groupName2, ReaderGroupConfig.builder()
                .stream(getScopedStreamName(scope, streamName2)).build()));

        // Call deleteScopeRecursive to delete the scope recursively
        assertTrue(streamManager.deleteScopeRecursive(scope));

        // Validate that the scope is deleted
        assertFalse(controller.checkScopeExists(scope).get());

        // Validate create operation of Stream/RG/KVT should throw error
        AssertExtensions.assertThrows("Failed to create Reader Group as Scope does not exits",
                () -> readerGroupManager.createReaderGroup(groupName3, ReaderGroupConfig.builder()
                        .stream(getScopedStreamName(scope, streamName2)).build()),
                e -> e instanceof IllegalArgumentException);
        AssertExtensions.assertThrows("Scope does not exist",
                () -> controller.createStream(scope, streamName4, config).get(),
                e -> e instanceof IllegalArgumentException);
        AssertExtensions.assertThrows("Scope does not exist",
                () -> keyValueTableManager.createKeyValueTable(scope, kvtName3, kvtConfig),
                e -> e instanceof IllegalArgumentException);
    }
}
