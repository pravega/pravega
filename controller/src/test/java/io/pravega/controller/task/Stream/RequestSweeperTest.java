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
package io.pravega.controller.task.Stream;

import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.common.concurrent.ExecutorServiceHelpers;
import io.pravega.common.tracing.RequestTracker;
import io.pravega.controller.PravegaZkCuratorResource;
import io.pravega.controller.mocks.EventHelperMock;
import io.pravega.controller.mocks.EventStreamWriterMock;
import io.pravega.controller.mocks.SegmentHelperMock;
import io.pravega.controller.server.SegmentHelper;
import io.pravega.controller.server.security.auth.GrpcAuthHelper;
import io.pravega.controller.store.host.HostControllerStore;
import io.pravega.controller.store.host.HostStoreFactory;
import io.pravega.controller.store.host.impl.HostMonitorConfigImpl;
import io.pravega.controller.store.index.HostIndex;
import io.pravega.controller.store.stream.AbstractStreamMetadataStore;
import io.pravega.controller.store.stream.State;
import io.pravega.controller.store.stream.StreamMetadataStore;
import io.pravega.controller.store.stream.StreamStoreFactory;
import io.pravega.controller.store.task.TaskStoreFactory;
import io.pravega.controller.task.EventHelper;
import io.pravega.shared.controller.event.ControllerEvent;
import io.pravega.shared.controller.event.ControllerEventSerializer;
import io.pravega.shared.controller.event.ScaleOpEvent;
import io.pravega.shared.controller.event.SealStreamEvent;
import java.nio.ByteBuffer;
import java.util.AbstractMap;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Consumer;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.RetryPolicy;
import org.apache.curator.retry.RetryOneTime;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.ClassRule;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;

/**
 * RequestSweeper test cases.
 */
@Slf4j
public abstract class RequestSweeperTest {
    private static final String HOSTNAME = "host-1234";
    private static final String SCOPE = "scope";
    private static final RetryPolicy RETRY_POLICY = new RetryOneTime(2000);
    @ClassRule
    public static final PravegaZkCuratorResource PRAVEGA_ZK_CURATOR_RESOURCE = new PravegaZkCuratorResource(RETRY_POLICY);

    protected final ScheduledExecutorService executor = ExecutorServiceHelpers.newScheduledThreadPool(10, "test");

    private final String stream1 = "stream1";

    private StreamMetadataStore streamStore;

    private final HostControllerStore hostStore = HostStoreFactory.createInMemoryStore(HostMonitorConfigImpl.dummyConfig());

    private StreamMetadataTasks streamMetadataTasks;
    private EventStreamWriterMock<ControllerEvent> requestEventWriter;
    private SegmentHelper segmentHelperMock;
    private final RequestTracker requestTracker = new RequestTracker(true);

    abstract StreamMetadataStore getStream();

    @Before
    public void setUp() throws Exception {
        streamStore = getStream();

        segmentHelperMock = SegmentHelperMock.getSegmentHelperMock();
        EventHelper helperMock = EventHelperMock.getEventHelperMock(executor, HOSTNAME, ((AbstractStreamMetadataStore) 
                streamStore).getHostTaskIndex());
        streamMetadataTasks = new StreamMetadataTasks(streamStore, StreamStoreFactory.createInMemoryBucketStore(),
                TaskStoreFactory.createInMemoryStore(executor), segmentHelperMock, executor, HOSTNAME, 
                GrpcAuthHelper.getDisabledAuthHelper(), helperMock);
        requestEventWriter = spy(new EventStreamWriterMock<>());
        streamMetadataTasks.setRequestEventWriter(requestEventWriter);

        final ScalingPolicy policy1 = ScalingPolicy.fixed(2);
        final StreamConfiguration configuration1 = StreamConfiguration.builder().scalingPolicy(policy1).build();

        // region createStream
        streamStore.createScope(SCOPE, null, executor).join();
        long start = System.currentTimeMillis();
        streamStore.createStream(SCOPE, stream1, configuration1, start, null, executor).join();
        streamStore.setState(SCOPE, stream1, State.ACTIVE, null, executor).join();
        // endregion
    }

    @After
    public void tearDown() throws Exception {
        streamMetadataTasks.close();
        streamStore.close();
        ExecutorServiceHelpers.shutdown(executor);
    }

    @Test(timeout = 30000)
    public void testRequestSweeper() throws ExecutionException, InterruptedException {
        AbstractMap.SimpleEntry<Double, Double> segment1 = new AbstractMap.SimpleEntry<>(0.5, 0.75);
        AbstractMap.SimpleEntry<Double, Double> segment2 = new AbstractMap.SimpleEntry<>(0.75, 1.0);
        List<Long> sealedSegments = Collections.singletonList(1L);

        CompletableFuture<Void> wait1 = new CompletableFuture<>();
        CompletableFuture<Void> wait2 = new CompletableFuture<>();
        LinkedBlockingQueue<CompletableFuture<Void>> waitQueue = new LinkedBlockingQueue<>();
        waitQueue.put(wait1);
        waitQueue.put(wait2);
        CompletableFuture<Void> signal1 = new CompletableFuture<>();
        CompletableFuture<Void> signal2 = new CompletableFuture<>();
        LinkedBlockingQueue<CompletableFuture<Void>> signalQueue = new LinkedBlockingQueue<>();
        signalQueue.put(signal1);
        signalQueue.put(signal2);
        doAnswer(x -> {
            signalQueue.take().complete(null);
            return waitQueue.take();
        }).when(requestEventWriter).writeEvent(any(), any());
        
        streamMetadataTasks.manualScale(SCOPE, stream1, sealedSegments, Arrays.asList(segment1, segment2), 
                System.currentTimeMillis(), 0L);

        signal1.join();
        // since we dont complete writeEventFuture, manual scale will not complete and index is not removed
        // verify that index has the entry.
        HostIndex hostIndex = getHostIndex();
        List<String> entities = hostIndex.getEntities(HOSTNAME).join();
        assertEquals(1, entities.size());
        byte[] data = hostIndex.getEntityData(HOSTNAME, entities.get(0)).join();
        ControllerEventSerializer serializer = new ControllerEventSerializer();
        ControllerEvent event = serializer.fromByteBuffer(ByteBuffer.wrap(data));
        assertTrue(event instanceof ScaleOpEvent);
        
        RequestSweeper requestSweeper = new RequestSweeper(streamStore, executor, streamMetadataTasks);
        CompletableFuture<Void> failoverFuture = requestSweeper.handleFailedProcess(HOSTNAME);

        // verify that the event is posted.. signal 2 future should be completed. 
        signal2.join();
        // let wait2 be complete as well. 
        wait2.complete(null);
        
        // wait for failover to complete
        failoverFuture.join();
        
        // verify that entity is removed. 
        entities = hostIndex.getEntities(HOSTNAME).join();
        assertTrue(entities.isEmpty());
        
        // verify that the host is removed.
        Set<String> hosts = hostIndex.getHosts().join();
        assertTrue(hosts.isEmpty());
    }

    @Test(timeout = 30000L)
    public void testRequestSweeperLimits() {
        doAnswer(x -> CompletableFuture.completedFuture(null)).when(requestEventWriter).writeEvent(any(), any());

        List<List<String>> processed = new LinkedList<>();
        Consumer<List<String>> consumer = processed::add;
        
        TestRequestSweeper requestSweeper = new TestRequestSweeper(streamStore, executor, streamMetadataTasks, consumer);

        ControllerEventSerializer serializer = new ControllerEventSerializer();
        
        HostIndex hostIndex = getHostIndex();
        ControllerEvent event1 = new SealStreamEvent("scope", "stream1", 0L);
        ControllerEvent event2 = new SealStreamEvent("scope", "stream2", 0L);
        ControllerEvent event3 = new SealStreamEvent("scope", "stream3", 0L);
        hostIndex.addEntity(HOSTNAME, "entity1", serializer.toByteBuffer(event1).array()).join();
        hostIndex.addEntity(HOSTNAME, "entity2", serializer.toByteBuffer(event2).array()).join();
        hostIndex.addEntity(HOSTNAME, "entity3", serializer.toByteBuffer(event3).array()).join();
        
        List<String> entities = hostIndex.getEntities(HOSTNAME).join();
        assertEquals(entities.size(), 3);
        Set<String> hosts = hostIndex.getHosts().join();
        assertEquals(hosts.size(), 1);

        requestSweeper.handleFailedProcess(HOSTNAME).join();

        // after failover all entities and host must have been removed
        entities = hostIndex.getEntities(HOSTNAME).join();
        assertTrue(entities.isEmpty());
        hosts = hostIndex.getHosts().join();
        assertTrue(hosts.isEmpty());
        
        // the processing loop should have been called four times. 
        // first three times, it should get one entity. 
        // in the exit condition it should get empty list and exit.
        assertEquals(processed.size(), 4);
        assertEquals(processed.get(0).size(), 1);
        assertEquals(processed.get(1).size(), 1);
        assertEquals(processed.get(2).size(), 1);
        assertEquals(processed.get(3).size(), 0);
    }

    abstract HostIndex getHostIndex();
    
    static class TestRequestSweeper extends RequestSweeper {
        private final Consumer<List<String>> consumer;
        
        public TestRequestSweeper(StreamMetadataStore metadataStore, ScheduledExecutorService executor, StreamMetadataTasks streamMetadataTasks, Consumer<List<String>> consumer) {
            super(metadataStore, executor, streamMetadataTasks, 1);
            this.consumer = consumer;
        }

        @Override
        CompletableFuture<List<String>> postRequest(String oldHostId) {
            return super.postRequest(oldHostId)
                    .thenApply(list -> {
                        consumer.accept(list);
                        return list;
                    });
        }
    }
}