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
package io.pravega.test.integration.controller.server;

import com.google.common.collect.ImmutableMap;
import io.pravega.client.segment.impl.Segment;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.StreamCut;
import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.client.control.impl.Controller;
import io.pravega.client.stream.impl.StreamImpl;
import io.pravega.client.stream.impl.StreamSegments;
import io.pravega.client.stream.impl.StreamCutImpl;
import io.pravega.client.tables.KeyValueTableConfiguration;
import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.ExecutorServiceHelpers;
import io.pravega.common.concurrent.Futures;
import io.pravega.controller.store.stream.StoreException;
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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;

import lombok.extern.slf4j.Slf4j;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static io.pravega.test.common.AssertExtensions.assertThrows;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@Slf4j
public class ControllerServiceTest {

    private final int controllerPort = TestUtils.getAvailableListenPort();
    private final String serviceHost = "localhost";
    private final int servicePort = TestUtils.getAvailableListenPort();
    private final int containerCount = 4;
    private TestingServer zkTestServer;
    private PravegaConnectionListener server;
    private ControllerWrapper controllerWrapper;
    private ServiceBuilder serviceBuilder;
    private ScheduledExecutorService executor;

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
        
        controllerWrapper = new ControllerWrapper(zkTestServer.getConnectString(), false,
                                                                    controllerPort, serviceHost, servicePort, containerCount);
        controllerWrapper.awaitRunning();
        executor = ExecutorServiceHelpers.newScheduledThreadPool(1, "collector");
    }
    
    @After
    public void tearDown() throws Exception {
        ExecutorServiceHelpers.shutdown(executor);
        controllerWrapper.close();
        server.close();
        serviceBuilder.close();
        zkTestServer.close();
    }

    @Test
    public void streamTagTest() {
        final String scope = "sc";
        final String stream = "st";
        final String stream2 = "st2";

        Controller controller = controllerWrapper.getController();
        controller.createScope(scope).join();
        System.out.println("scope created");
        // Create Stream with tags t1, t2
        StreamConfiguration strCfg = StreamConfiguration.builder().scalingPolicy(ScalingPolicy.fixed(1)).tag("t1").tag("t2").build();
        controller.createStream(scope, stream, strCfg).join();
        assertEquals(Set.of("t1", "t2"), controller.getStreamConfiguration(scope, stream).join().getTags());
        // Update stream to have tags t2, t3
        StreamConfiguration strCfgNew = strCfg.toBuilder().clearTags().tags(Set.of("t2", "t3")).build();
        controller.updateStream(scope, stream, strCfgNew).join();
        // Check if the stream tags are infact t2, t3
        assertEquals(Set.of("t2", "t3"), controller.getStreamConfiguration(scope, stream).join().getTags());

        // List Streams with tag t2. only one stream should be listed
        assertEquals(Collections.singletonList(stream), listStreamsForTag(scope, controller, "t2"));

        // Create stream2 with tags t1, t2
        controller.createStream(scope, stream2, strCfg).join();

        // List Streams with tag t2. two stream should be listed
        assertEquals(Arrays.asList(stream2, stream), listStreamsForTag(scope, controller, "t2"));

        controller.sealStream(scope, stream2).join();
        controller.deleteStream(scope, stream2).join();

        // List Streams with tag t2. two stream should be listed
        assertEquals(Arrays.asList(stream), listStreamsForTag(scope, controller, "t2"));
        assertEquals(strCfgNew, controller.getStreamConfiguration(scope, stream).join());

        controller.sealStream(scope, stream).join();
        controller.deleteStream(scope, stream).join();
        assertEquals(Collections.emptyList(), listStreamsForTag(scope, controller, "t2"));

    }

    private ArrayList<String> listStreamsForTag(String scope, Controller controller, String tag) {
        ArrayList<String> resultList = new ArrayList<String>();
        controller.listStreamsForTag(scope, tag)
                  .collectRemaining(stream1 -> resultList.add(stream1.getStreamName()))
                  .join();
        return resultList;
    }

    @Test(timeout = 40000)
    public void streamMetadataTest() throws Exception {
        final String scope = "testScope";
        final String stream = "testStream";

        StreamConfiguration streamConfiguration = StreamConfiguration.builder()
                .scalingPolicy(ScalingPolicy.fixed(1))
                .build();
        Controller controller = controllerWrapper.getController();
        // Create test scope. This operation should succeed.
        assertTrue(controller.createScope(scope).join());

        // Delete the test scope. This operation should also succeed.
        assertTrue(controller.deleteScope(scope).join());

        // Try creating a stream. It should fail, since the scope does not exist.
        assertFalse(Futures.await(controller.createStream(scope, stream, streamConfiguration)));

        // Again create the scope.
        assertTrue(controller.createScope(scope).join());

        // Try creating the stream again. It should succeed now, since the scope exists.
        assertTrue(controller.createStream(scope, stream, streamConfiguration).join());

        // Delete test scope. This operation should fail, since it is not empty.
        assertFalse(Futures.await(controller.deleteScope(scope)));

        // Delete a non-existent scope.
        assertFalse(controller.deleteScope("non_existent_scope").get());

        // Create a scope with invalid characters. It should fail.
        assertFalse(Futures.await(controller.createScope("abc/def")));

        // Try creating already existing scope. 
        assertFalse(controller.createScope(scope).join());

        // Try creating stream with invalid characters. It should fail.
        assertFalse(Futures.await(controller.createStream(scope, "abc/def", StreamConfiguration.builder()
                                                                             .scalingPolicy(ScalingPolicy.fixed(1))
                                                                             .build())));

        // Try creating already existing stream.
        assertFalse(controller.createStream(scope, stream, streamConfiguration).join());
    }
    
    @Test(timeout = 80000)
    public void testControllerService() throws Exception {
        final String scope1 = "scope1";
        final String scope2 = "scope2";
        controllerWrapper.getControllerService().createScope("scope1", 0L).get();
        controllerWrapper.getControllerService().createScope("scope2", 0L).get();
        Controller controller = controllerWrapper.getController();

        final String streamName1 = "stream1";
        final String streamName2 = "stream2";
        final String streamName3 = "stream3";
        final ScalingPolicy scalingPolicy = ScalingPolicy.fixed(2);
        final StreamConfiguration config1 = StreamConfiguration.builder()
                .scalingPolicy(scalingPolicy)
                .build();
        final StreamConfiguration config2 = StreamConfiguration.builder()
                .scalingPolicy(scalingPolicy)
                .build();
        final StreamConfiguration config3 = StreamConfiguration.builder()
                .scalingPolicy(ScalingPolicy.fixed(3))
                .build();

        createAStream(scope1, streamName1, controller, config1);
        //Same name in different scope
        createAStream(scope2, streamName1, controller, config2);
        //Different name in same scope
        createAStream(scope1, streamName2, controller, config3);
        createAStream(scope1, streamName3, controller, config3);

        final String kvtName1 = "kvtable1";
        final String kvtName2 = "kvtable2";
        final KeyValueTableConfiguration kvtConfig1 = KeyValueTableConfiguration.builder()
                .partitionCount(2).primaryKeyLength(4).secondaryKeyLength(4).build();

        createAKeyValueTable(scope1, kvtName1, controller, kvtConfig1);
        //Same name in different scope
        createAKeyValueTable(scope2, kvtName1, controller, kvtConfig1);
        //Different name in different scope
        createAKeyValueTable(scope2, kvtName2, controller, kvtConfig1);

        final String scopeSeal = "scopeSeal";
        final String streamNameSeal = "streamSeal";
        sealAStream(controllerWrapper, controller, scalingPolicy, scopeSeal, streamNameSeal);
        
        sealASealedStream(controller, scopeSeal, streamNameSeal);
 
        sealNonExistantStream(controller, scopeSeal);

        streamDuplicationNotAllowed(scope1, streamName1, controller, config1);
       
        //update stream config section

        updateStreamName(controller, scope1, scalingPolicy);

        updateScalingPolicy(controller, scope1, streamName1);

        updateTargetRate(controller, scope1, streamName1);

        updateScaleFactor(controller, scope1, streamName1);

        updataMinSegmentes(controller, scope1, streamName1);

        updateConfigOfNonExistantStream(controller);

        //get currently active segments

        getActiveSegments(controller, scope1, streamName1);

        getActiveSegmentsForNonExistentStream(controller);

        //get positions at a given time stamp

        getSegmentsAtTime(controller, scope1, streamName1);
        getSegmentsAtTime(controller, scope1, streamName2);

        getSegmentsForNonExistentStream(controller);
        
        getSegmentsBeforeCreation(controller, scope1, streamName1);

        getSegmentsAfterCreation(controller, scope1, streamName1);

        readerGroupsTest(controller, scope1, streamName1, streamName2, streamName3);

        updateSubscriberStreamCutTest(controller, scope2, streamName1);
    }

    private static void getSegmentsAfterCreation(Controller controller, final String scope,
                                                 final String streamName) throws InterruptedException,
                                                                          ExecutionException {
        CompletableFuture<Map<Segment, Long>> segments = controller.getSegmentsAtTime(new StreamImpl(scope, streamName), System.currentTimeMillis() + 3600);
        assertFalse("FAILURE: Fetching positions at given time in furture after stream creation failed", segments.get().isEmpty());
    }

    private static void getSegmentsBeforeCreation(Controller controller, final String scope,
                                                  final String streamName) throws InterruptedException,
                                                                           ExecutionException {
        CompletableFuture<Map<Segment, Long>> segments = controller.getSegmentsAtTime(new StreamImpl(scope, streamName), System.currentTimeMillis() - 36000);
        assertFalse("FAILURE: Fetching positions at given time before stream creation failed", segments.get().size() == 1);
    }

    private static void getSegmentsForNonExistentStream(Controller controller) throws InterruptedException {
        Stream stream = new StreamImpl("scope", "streamName");
        try {
            CompletableFuture<Map<Segment, Long>> segments = controller.getSegmentsAtTime(stream, System.currentTimeMillis());
            assertTrue("FAILURE: Fetching positions for non existent stream", segments.get().isEmpty());
            log.info("SUCCESS: Positions cannot be fetched for non existent stream");
        } catch (ExecutionException | CompletionException e) {
            assertTrue("FAILURE: Fetching positions for non existent stream", Exceptions.unwrap(e) instanceof StoreException.DataNotFoundException);
            log.info("SUCCESS: Positions cannot be fetched for non existent stream");
        }
    }

    private static void getSegmentsAtTime(Controller controller, final String scope,
                                            final String streamName) throws InterruptedException, ExecutionException {
        CompletableFuture<Map<Segment, Long>> segments = controller.getSegmentsAtTime(new StreamImpl(scope, streamName), System.currentTimeMillis());
        assertFalse("FAILURE: Fetching positions at given time stamp failed", segments.get().isEmpty()); 
    }

    private static void getActiveSegmentsForNonExistentStream(Controller controller) {
        
        AssertExtensions.assertFutureThrows("", controller.getCurrentSegments("scope", "streamName"),
            e -> Exceptions.unwrap(e) instanceof StoreException.DataNotFoundException);
    }

    private static void getActiveSegments(Controller controller, final String scope,
                                          final String streamName) throws InterruptedException, ExecutionException {
        CompletableFuture<StreamSegments> getActiveSegments = controller.getCurrentSegments(scope, streamName);
        assertFalse("FAILURE: Fetching active segments failed", getActiveSegments.get().getSegments().isEmpty());
        
    }


    private static void updateConfigOfNonExistantStream(Controller controller) {
        assertFalse(Futures.await(controller.updateStream("scope", "streamName", StreamConfiguration.builder()
                                                                             .scalingPolicy(ScalingPolicy.byEventRate(200, 2, 3))
                                                                             .build())));
    }

    private static void updataMinSegmentes(Controller controller, final String scope,
                                           final String streamName) throws InterruptedException, ExecutionException {
        assertTrue(controller.updateStream(scope, streamName, StreamConfiguration.builder()
                                          .scalingPolicy(ScalingPolicy.byEventRate(200, 2, 3))
                                          .build()).get());
        assertEquals(3, controller.getCurrentSegments(scope, streamName).get().getSegments().size());
    }

    private static void updateScaleFactor(Controller controller, final String scope,
                                          final String streamName) throws InterruptedException, ExecutionException {
        assertTrue(controller.updateStream(scope, streamName, StreamConfiguration.builder()
                                          .scalingPolicy(ScalingPolicy.byEventRate(100, 3, 2))
                                          .build()).get());
    }

    private static void updateTargetRate(Controller controller, final String scope,
                                         final String streamName) throws InterruptedException, ExecutionException {
        assertTrue(controller.updateStream(scope, streamName, StreamConfiguration.builder()
                                          .scalingPolicy(ScalingPolicy.byEventRate(200, 2, 2))
                                          .build()).get());
    }

    private static void updateScalingPolicy(Controller controller, final String scope,
                                            final String streamName) throws InterruptedException, ExecutionException {
        assertTrue(controller.updateStream(scope, streamName, StreamConfiguration.builder()
                                          .scalingPolicy(ScalingPolicy.byEventRate(100, 2, 2))
                                          .build()).get());
    }

    private static void updateStreamName(Controller controller, final String scope,
                                         final ScalingPolicy scalingPolicy) {
        assertFalse(Futures.await(controller.updateStream(scope, "stream4", StreamConfiguration.builder()
                                                                             .scalingPolicy(scalingPolicy)
                                                                             .build())));
    }

    private static void readerGroupsTest(Controller controller, final String scope, final String stream1,
                                         final String stream2, final String stream3) throws InterruptedException, ExecutionException {
        final String scopedStreamName1 = NameUtils.getScopedStreamName(scope, stream1);
        final String scopedStreamName2 = NameUtils.getScopedStreamName(scope, stream2);
        final String scopedStreamName3 = NameUtils.getScopedStreamName(scope, stream3);
        final Segment seg0 = new Segment(scope, stream1, 0L);
        final Segment seg1 = new Segment(scope, stream1, 1L);
        ImmutableMap<Segment, Long> startStreamCut = ImmutableMap.of(seg0, 10L, seg1, 10L);
        Map<Stream, StreamCut> startSC = ImmutableMap.of(
                    Stream.of(scope, stream1), new StreamCutImpl(Stream.of(scope, stream1), startStreamCut),
                    Stream.of(scope, stream2), new StreamCutImpl(Stream.of(scope, stream2), startStreamCut));
        ImmutableMap<Segment, Long> endStreamCut = ImmutableMap.of(seg0, 200L, seg1, 300L);
        Map<Stream, StreamCut> endSC = ImmutableMap.of(
                Stream.of(scope, stream1), new StreamCutImpl(Stream.of(scope, stream1), endStreamCut),
                Stream.of(scope, stream2), new StreamCutImpl(Stream.of(scope, stream2), endStreamCut));
        ReaderGroupConfig rgConfig = ReaderGroupConfig.builder()
                .automaticCheckpointIntervalMillis(30000L)
                .groupRefreshTimeMillis(20000L)
                .maxOutstandingCheckpointRequest(2)
                .retentionType(ReaderGroupConfig.StreamDataRetention.AUTOMATIC_RELEASE_AT_LAST_CHECKPOINT)
                .startingStreamCuts(startSC)
                .endingStreamCuts(endSC).build();
        final ReaderGroupConfig rgConfig1 = ReaderGroupConfig.cloneConfig(rgConfig, UUID.randomUUID(), 0L);
        // Create Reader Group rg1
        ReaderGroupConfig createRGResult = controller.createReaderGroup(scope, "rg1", rgConfig1).get();
        assertEquals(rgConfig1.getReaderGroupId(), createRGResult.getReaderGroupId());

        assertThrows(IllegalArgumentException.class, () -> controller.createReaderGroup(scope, "bad_rg_name", rgConfig1).get());
        assertThrows(IllegalArgumentException.class, () -> controller.createReaderGroup("badscope", "rg3", rgConfig1).get());

        ReaderGroupConfig rgConfig2 = ReaderGroupConfig.builder()
                .retentionType(ReaderGroupConfig.StreamDataRetention.MANUAL_RELEASE_AT_USER_STREAMCUT)
                .stream(scopedStreamName1).stream(scopedStreamName2)
                .automaticCheckpointIntervalMillis(7000L)
                .build();
        rgConfig2 = ReaderGroupConfig.cloneConfig(rgConfig2, UUID.randomUUID(), 0L);
        // Create Reader Group rg2
        createRGResult = controller.createReaderGroup(scope, "rg2", rgConfig2).get();
        assertEquals(rgConfig2.getReaderGroupId(), createRGResult.getReaderGroupId());

        List<String> subscribers = controller.listSubscribers(scope, stream1).get();
        assertTrue(subscribers.size() == 2);

        assertTrue(controller.deleteReaderGroup(scope, "rg2", rgConfig2.getReaderGroupId()).get());

        assertThrows(IllegalArgumentException.class, () -> controller.getReaderGroupConfig(scope, "rg2").get());

        subscribers = controller.listSubscribers(scope, stream1).get();
        assertTrue(subscribers.size() == 1);

        ReaderGroupConfig config = controller.getReaderGroupConfig(scope, "rg1").get();
        assertEquals(rgConfig1.getGroupRefreshTimeMillis(), config.getGroupRefreshTimeMillis());
        assertEquals(rgConfig1.getGeneration(), config.getGeneration());
        assertEquals(rgConfig1.getMaxOutstandingCheckpointRequest(), config.getMaxOutstandingCheckpointRequest());
        assertEquals(rgConfig1.getRetentionType(), config.getRetentionType());
        assertEquals(rgConfig1.getReaderGroupId(), config.getReaderGroupId());
        assertEquals(rgConfig1.getStartingStreamCuts().keySet().size(), config.getStartingStreamCuts().keySet().size());
        assertEquals(rgConfig1.getEndingStreamCuts().keySet().size(), config.getEndingStreamCuts().keySet().size());
        assertTrue(config.getStartingStreamCuts().keySet().contains(Stream.of(scope, stream1)));
        assertTrue(config.getStartingStreamCuts().keySet().contains(Stream.of(scope, stream2)));

        Map<Stream, StreamCut> startSCNew = ImmutableMap.of(
                Stream.of(scope, stream2), new StreamCutImpl(Stream.of(scope, stream2), startStreamCut),
                Stream.of(scope, stream3), new StreamCutImpl(Stream.of(scope, stream3), startStreamCut));
        Map<Stream, StreamCut> endSCNew = ImmutableMap.of(
                Stream.of(scope, stream2), new StreamCutImpl(Stream.of(scope, stream2), endStreamCut),
                Stream.of(scope, stream3), new StreamCutImpl(Stream.of(scope, stream3), endStreamCut));
        ReaderGroupConfig newRGConfig = ReaderGroupConfig.builder()
                .automaticCheckpointIntervalMillis(1000L)
                .groupRefreshTimeMillis(5000L)
                .maxOutstandingCheckpointRequest(7)
                .retentionType(ReaderGroupConfig.StreamDataRetention.AUTOMATIC_RELEASE_AT_LAST_CHECKPOINT)
                .startingStreamCuts(startSCNew)
                .endingStreamCuts(endSCNew).build();
        newRGConfig = ReaderGroupConfig.cloneConfig(newRGConfig, rgConfig1.getReaderGroupId(), rgConfig1.getGeneration());
        // Update Reader Group rg1
        assertNotNull(controller.updateReaderGroup(scope, "rg1", newRGConfig).get());
        ReaderGroupConfig updatedConfig = controller.getReaderGroupConfig(scope, "rg1").get();
        assertEquals(newRGConfig.getGroupRefreshTimeMillis(), updatedConfig.getGroupRefreshTimeMillis());
        assertEquals(newRGConfig.getGeneration() + 1, updatedConfig.getGeneration());
        assertEquals(newRGConfig.getMaxOutstandingCheckpointRequest(), updatedConfig.getMaxOutstandingCheckpointRequest());
        assertEquals(newRGConfig.getRetentionType(), updatedConfig.getRetentionType());
        assertEquals(newRGConfig.getReaderGroupId(), updatedConfig.getReaderGroupId());
        assertEquals(newRGConfig.getStartingStreamCuts().keySet().size(), updatedConfig.getStartingStreamCuts().keySet().size());
        assertEquals(newRGConfig.getEndingStreamCuts().keySet().size(), updatedConfig.getEndingStreamCuts().keySet().size());
        assertTrue(updatedConfig.getStartingStreamCuts().keySet().contains(Stream.of(scope, stream3)));
        assertTrue(updatedConfig.getStartingStreamCuts().keySet().contains(Stream.of(scope, stream2)));

        // re-create ReaderGroup "rg2"
        ReaderGroupConfig rgConfigRecreate = ReaderGroupConfig.builder().disableAutomaticCheckpoints()
                .stream(scopedStreamName3)
                .retentionType(ReaderGroupConfig.StreamDataRetention.AUTOMATIC_RELEASE_AT_LAST_CHECKPOINT)
                .build();
        rgConfigRecreate = ReaderGroupConfig.cloneConfig(rgConfigRecreate, UUID.randomUUID(), 0L);
        ReaderGroupConfig recreateRGResponse = controller.createReaderGroup(scope, "rg2", rgConfigRecreate).get();
        assertEquals(rgConfigRecreate.getReaderGroupId(), recreateRGResponse.getReaderGroupId());
        assertEquals(rgConfigRecreate.getRetentionType(), recreateRGResponse.getRetentionType());

        // Update a ReaderGroup from Subscriber to Non-subscriber
        final String readerGroupName = "rg3";
        ReaderGroupConfig rgConfigSubscriber = ReaderGroupConfig.builder().disableAutomaticCheckpoints()
                .stream(scopedStreamName1).retentionType(ReaderGroupConfig.StreamDataRetention.MANUAL_RELEASE_AT_USER_STREAMCUT)
                .build();
        rgConfigSubscriber = ReaderGroupConfig.cloneConfig(rgConfigSubscriber, UUID.randomUUID(), 0L);
        ReaderGroupConfig subscriberRG = controller.createReaderGroup(scope, readerGroupName, rgConfigSubscriber).join();
        assertEquals(rgConfigSubscriber.getReaderGroupId(), subscriberRG.getReaderGroupId());

        subscribers = controller.listSubscribers(scope, stream1).get();
        assertEquals(1, subscribers.size());

        ReaderGroupConfig rgConfigNonSubscriber = ReaderGroupConfig.builder().disableAutomaticCheckpoints()
                .stream(scopedStreamName1)
                .build();
        rgConfigNonSubscriber = ReaderGroupConfig.cloneConfig(rgConfigNonSubscriber, rgConfigSubscriber.getReaderGroupId(), rgConfigSubscriber.getGeneration());
        long updatedGen = controller.updateReaderGroup(scope, readerGroupName, rgConfigNonSubscriber).join();
        assertEquals(rgConfigNonSubscriber.getGeneration() + 1, updatedGen);

        updatedConfig = controller.getReaderGroupConfig(scope, readerGroupName).join();
        assertEquals(rgConfigNonSubscriber.getReaderGroupId(), updatedConfig.getReaderGroupId());
        assertEquals(rgConfigNonSubscriber.getRetentionType(), updatedConfig.getRetentionType());
        assertEquals(rgConfigNonSubscriber.getGeneration() + 1, updatedConfig.getGeneration());

        subscribers = controller.listSubscribers(scope, stream1).get();
        assertEquals(0, subscribers.size());

        // Update ReaderGroup from Non-Subscriber to Subscriber
        ReaderGroupConfig subscriberConfig = ReaderGroupConfig.builder()
                .stream(scopedStreamName1).retentionType(ReaderGroupConfig.StreamDataRetention.AUTOMATIC_RELEASE_AT_LAST_CHECKPOINT)
                .build();
        subscriberConfig = ReaderGroupConfig.cloneConfig(subscriberConfig, updatedConfig.getReaderGroupId(), updatedConfig.getGeneration());
        long gen = controller.updateReaderGroup(scope, readerGroupName, subscriberConfig).join();
        assertEquals(subscriberConfig.getGeneration() + 1, gen);
        ReaderGroupConfig newUpdatedConfig = controller.getReaderGroupConfig(scope, readerGroupName).join();
        assertEquals(subscriberConfig.getReaderGroupId(), newUpdatedConfig.getReaderGroupId());
        assertEquals(subscriberConfig.getRetentionType(), newUpdatedConfig.getRetentionType());
        assertEquals(gen, newUpdatedConfig.getGeneration());

        subscribers = controller.listSubscribers(scope, stream1).get();
        assertEquals(1, subscribers.size());
    }

    private static void updateSubscriberStreamCutTest(Controller controller, final String scope, final String stream) throws InterruptedException, ExecutionException {
        // add the first subscriber
        Segment seg0 = new Segment(scope, stream, 0L);
        Segment seg1 = new Segment(scope, stream, 1L);
        ImmutableMap<Segment, Long> startStreamCut = ImmutableMap.of(seg0, 10L, seg1, 10L);
        Map<Stream, StreamCut> startSC = ImmutableMap.of(Stream.of(scope, stream), new StreamCutImpl(Stream.of(scope, stream), startStreamCut));
        ImmutableMap<Segment, Long> endStreamCut = ImmutableMap.of(seg0, 200L, seg1, 300L);
        Map<Stream, StreamCut> endSC = ImmutableMap.of(Stream.of(scope, stream), new StreamCutImpl(Stream.of(scope, stream), endStreamCut));
        final ReaderGroupConfig rgConfig = ReaderGroupConfig.builder()
                .automaticCheckpointIntervalMillis(30000L)
                .groupRefreshTimeMillis(20000L)
                .maxOutstandingCheckpointRequest(2)
                .retentionType(ReaderGroupConfig.StreamDataRetention.AUTOMATIC_RELEASE_AT_LAST_CHECKPOINT)
                .startingStreamCuts(startSC)
                .endingStreamCuts(endSC).build();

        final String rg1 = "rg1";
        ReaderGroupConfig createConfig = controller.createReaderGroup(scope, rg1, rgConfig).get();
        assertFalse(ReaderGroupConfig.DEFAULT_UUID.equals(createConfig.getReaderGroupId()));
        assertEquals(0L, createConfig.getGeneration());

        List<String> subs = controller.listSubscribers(scope, stream).get();
        assertEquals(1, subs.size());
        String subscriber1 = NameUtils.getScopedReaderGroupName(scope, rg1);
        assertEquals(subscriber1, subs.get(0));

        Stream streamToBeUpdated = Stream.of(scope, stream);
        seg0 = new Segment(scope, stream, 0L);
        seg1 = new Segment(scope, stream, 1L);
        ImmutableMap<Segment, Long> streamCutPositions = ImmutableMap.of(seg0, 1L, seg1, 11L);
        StreamCut streamCut = new StreamCutImpl(streamToBeUpdated, streamCutPositions);

        assertTrue(controller.updateSubscriberStreamCut(scope, stream, subscriber1, createConfig.getReaderGroupId(), createConfig.getGeneration(), streamCut).get());

        ImmutableMap<Segment, Long> streamCutPositionsNew = ImmutableMap.of(seg0, 2L, seg1, 22L);
        StreamCut streamCutNew = new StreamCutImpl(streamToBeUpdated, streamCutPositionsNew);
        assertTrue(controller.updateSubscriberStreamCut(scope, stream, subscriber1, createConfig.getReaderGroupId(), createConfig.getGeneration(), streamCutNew).get());
    }

    private static void sealAStream(ControllerWrapper controllerWrapper, Controller controller,
                                   final ScalingPolicy scalingPolicy, final String scopeSeal,
                                   final String streamNameSeal) throws InterruptedException, ExecutionException {
        controllerWrapper.getControllerService().createScope("scopeSeal", 0L).get();

        final StreamConfiguration configSeal = StreamConfiguration.builder()
                .scalingPolicy(scalingPolicy)
                .build();
        assertTrue(controller.createStream(scopeSeal, streamNameSeal, configSeal).get());

        @SuppressWarnings("unused")
        StreamSegments result = controller.getCurrentSegments(scopeSeal, streamNameSeal).get();
        assertTrue(controller.sealStream(scopeSeal, streamNameSeal).get());

        StreamSegments currentSegs = controller.getCurrentSegments(scopeSeal, streamNameSeal).get();
        assertTrue("FAILURE: No active segments should be present in a sealed stream", currentSegs.getSegments().isEmpty());
        
    }

    private static void createAStream(String scope, String streamName, Controller controller,
                                      final StreamConfiguration config) throws InterruptedException,
                                                                        ExecutionException {
        assertTrue(controller.createStream(scope, streamName, config).get());
    }

    private static void sealNonExistantStream(Controller controller, final String scopeSeal) {
        assertFalse(Futures.await(controller.sealStream(scopeSeal, "nonExistentStream")));
    }

    private static void streamDuplicationNotAllowed(String scope, String streamName, Controller controller,
                                                    final StreamConfiguration config) throws InterruptedException,
                                                                                      ExecutionException {
        assertFalse(controller.createStream(scope, streamName, config).get());
    }

    private static void sealASealedStream(Controller controller, final String scopeSeal,
                                          final String streamNameSeal) throws InterruptedException, ExecutionException {
        assertTrue(controller.sealStream(scopeSeal, streamNameSeal).get());

        StreamSegments currentSegs = controller.getCurrentSegments(scopeSeal, streamNameSeal).get();
        assertTrue("FAILURE: No active segments should be present in a sealed stream", currentSegs.getSegments().isEmpty());
        
    }

    private static void createAKeyValueTable(String scope, String kvtName, Controller controller,
                                      final KeyValueTableConfiguration config) throws InterruptedException,
            ExecutionException {
        assertTrue(controller.createKeyValueTable(scope, kvtName, config).get());
    }

    private static void createAKeyValueTableZeroPC(String scope, String kvtName, Controller controller,
                                             final KeyValueTableConfiguration config) {
        assertThrows(IllegalArgumentException.class, () -> controller.createKeyValueTable(scope, kvtName, config).join());
    }

}
