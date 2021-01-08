/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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
import io.pravega.common.concurrent.Futures;
import io.pravega.controller.store.stream.StoreException;
import io.pravega.segmentstore.contracts.StreamSegmentStore;
import io.pravega.segmentstore.contracts.tables.TableStore;
import io.pravega.segmentstore.server.host.handler.PravegaConnectionListener;
import io.pravega.segmentstore.server.store.ServiceBuilder;
import io.pravega.segmentstore.server.store.ServiceBuilderConfig;
import io.pravega.shared.NameUtils;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.TestUtils;
import io.pravega.test.common.TestingServerStarter;
import io.pravega.test.integration.demo.ControllerWrapper;

import java.util.Map;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;

import lombok.extern.slf4j.Slf4j;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static io.pravega.test.common.AssertExtensions.assertThrows;
import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;

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
    
    @Before
    public void setUp() throws Exception {
        zkTestServer = new TestingServerStarter().start();
        
        serviceBuilder = ServiceBuilder.newInMemoryBuilder(ServiceBuilderConfig.getDefaultConfig());
        serviceBuilder.initialize();
        StreamSegmentStore store = serviceBuilder.createStreamSegmentService();
        TableStore tableStore = serviceBuilder.createTableStoreService();

        server = new PravegaConnectionListener(false, servicePort, store, tableStore, serviceBuilder.getLowPriorityExecutor());
        server.startListening();
        
        controllerWrapper = new ControllerWrapper(zkTestServer.getConnectString(), false,
                                                                    controllerPort, serviceHost, servicePort, containerCount);
        controllerWrapper.awaitRunning();
    }
    
    @After
    public void tearDown() throws Exception {
        controllerWrapper.close();
        server.close();
        serviceBuilder.close();
        zkTestServer.close();
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
        controllerWrapper.getControllerService().createScope("scope1").get();
        controllerWrapper.getControllerService().createScope("scope2").get();
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
        final String kvtZero = "kvtableZero";
        final KeyValueTableConfiguration kvtConfig1 = KeyValueTableConfiguration.builder()
                .partitionCount(2).build();
        final KeyValueTableConfiguration kvtConfigZeroPC = KeyValueTableConfiguration.builder()
                .partitionCount(0).build();

        createAKeyValueTable(scope1, kvtName1, controller, kvtConfig1);
        //Same name in different scope
        createAKeyValueTable(scope2, kvtName1, controller, kvtConfig1);
        //Different name in different scope
        createAKeyValueTable(scope2, kvtName2, controller, kvtConfig1);
        //KVTable with 0 partitions should fail
        createAKeyValueTableZeroPC(scope2, kvtZero, controller, kvtConfigZeroPC);

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
        assertFalse("FAILURE: Fetching positions at given time before stream creation failed", segments.get().size() != controller.getCurrentSegments(scope, streamName).get().getSegments().size());
       
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

    private static void getActiveSegmentsForNonExistentStream(Controller controller) throws InterruptedException {
        
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
        final ReaderGroupConfig rgConfig = ReaderGroupConfig.builder()
                .automaticCheckpointIntervalMillis(30000L)
                .groupRefreshTimeMillis(20000L)
                .maxOutstandingCheckpointRequest(2)
                .retentionType(ReaderGroupConfig.StreamDataRetention.AUTOMATIC_RELEASE_AT_LAST_CHECKPOINT)
                .generation(0L)
                .readerGroupId(UUID.randomUUID())
                .startingStreamCuts(startSC)
                .readerGroupId(UUID.randomUUID())
                .endingStreamCuts(endSC).build();

        CompletableFuture<Boolean> createRG = controller.createReaderGroup(scope, "rg1", rgConfig);
        assertTrue(createRG.get());

        assertTrue(controller.createReaderGroup(scope, "rg2", rgConfig).get());

        assertThrows(IllegalArgumentException.class, () -> controller.createReaderGroup(scope, "bad_rg_name", rgConfig).get());
        assertThrows(IllegalArgumentException.class, () -> controller.createReaderGroup("badscope", "rg3", rgConfig).get());

        List<String> subscribers = controller.listSubscribers(scope, stream1).get();
        assertTrue(subscribers.size() == 2);

        assertTrue(controller.deleteReaderGroup(scope, "rg2", rgConfig.getReaderGroupId(), rgConfig.getGeneration()).get());
        assertThrows(IllegalArgumentException.class, () -> controller.getReaderGroupConfig(scope, "rg2").get());

        ReaderGroupConfig config = controller.getReaderGroupConfig(scope, "rg1").get();
        assertEquals(rgConfig.getGroupRefreshTimeMillis(), config.getGroupRefreshTimeMillis());
        assertEquals(rgConfig.getGeneration(), config.getGeneration());
        assertEquals(rgConfig.getMaxOutstandingCheckpointRequest(), config.getMaxOutstandingCheckpointRequest());
        assertEquals(rgConfig.getRetentionType(), config.getRetentionType());
        assertEquals(rgConfig.getReaderGroupId(), config.getReaderGroupId());
        assertEquals(rgConfig.getStartingStreamCuts().keySet().size(), config.getStartingStreamCuts().keySet().size());
        assertEquals(rgConfig.getEndingStreamCuts().keySet().size(), config.getEndingStreamCuts().keySet().size());
        assertTrue(config.getStartingStreamCuts().keySet().contains(Stream.of(scope, stream1)));
        assertTrue(config.getStartingStreamCuts().keySet().contains(Stream.of(scope, stream2)));

        Map<Stream, StreamCut> startSCNew = ImmutableMap.of(
                Stream.of(scope, stream2), new StreamCutImpl(Stream.of(scope, stream2), startStreamCut),
                Stream.of(scope, stream3), new StreamCutImpl(Stream.of(scope, stream3), startStreamCut));
        Map<Stream, StreamCut> endSCNew = ImmutableMap.of(
                Stream.of(scope, stream2), new StreamCutImpl(Stream.of(scope, stream2), endStreamCut),
                Stream.of(scope, stream3), new StreamCutImpl(Stream.of(scope, stream3), endStreamCut));
        final ReaderGroupConfig newRGConfig = ReaderGroupConfig.builder()
                .automaticCheckpointIntervalMillis(1000L)
                .groupRefreshTimeMillis(5000L)
                .maxOutstandingCheckpointRequest(7)
                .retentionType(ReaderGroupConfig.StreamDataRetention.AUTOMATIC_RELEASE_AT_LAST_CHECKPOINT)
                .generation(0L)
                .readerGroupId(rgConfig.getReaderGroupId())
                .startingStreamCuts(startSCNew)
                .endingStreamCuts(endSCNew).build();
        assertTrue(controller.updateReaderGroup(scope, "rg1", newRGConfig).get());
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

        final ReaderGroupConfig rgConfig1 = ReaderGroupConfig.builder()
                .automaticCheckpointIntervalMillis(30000L)
                .groupRefreshTimeMillis(20000L)
                .maxOutstandingCheckpointRequest(2)
                .retentionType(ReaderGroupConfig.StreamDataRetention.AUTOMATIC_RELEASE_AT_LAST_CHECKPOINT)
                .generation(0L)
                .readerGroupId(UUID.randomUUID())
                .startingStreamCuts(startSC)
                .readerGroupId(UUID.randomUUID())
                .endingStreamCuts(endSC).build();
        assertTrue(controller.createReaderGroup(scope, "rg2", rgConfig1).get());
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
                .generation(0L)
                .readerGroupId(UUID.randomUUID())
                .startingStreamCuts(startSC)
                .endingStreamCuts(endSC).build();

        final String rg1 = "rg1";
        CompletableFuture<Boolean> createRG = controller.createReaderGroup(scope, rg1, rgConfig);
        assertTrue(createRG.get());

        List<String> subs = controller.listSubscribers(scope, stream).get();
        assertEquals(1, subs.size());
        String subscriber1 = NameUtils.getScopedReaderGroupName(scope, rg1);
        assertEquals(subscriber1, subs.get(0));

        Stream streamToBeUpdated = Stream.of(scope, stream);
        seg0 = new Segment(scope, stream, 0L);
        seg1 = new Segment(scope, stream, 1L);
        ImmutableMap<Segment, Long> streamCutPositions = ImmutableMap.of(seg0, 1L, seg1, 11L);
        StreamCut streamCut = new StreamCutImpl(streamToBeUpdated, streamCutPositions);

        assertTrue(controller.updateSubscriberStreamCut(scope, stream, subscriber1, rgConfig.getReaderGroupId(), 0L, streamCut).get());

        ImmutableMap<Segment, Long> streamCutPositionsNew = ImmutableMap.of(seg0, 2L, seg1, 22L);
        StreamCut streamCutNew = new StreamCutImpl(streamToBeUpdated, streamCutPositionsNew);
        assertTrue(controller.updateSubscriberStreamCut(scope, stream, subscriber1, rgConfig.getReaderGroupId(), 0L, streamCutNew).get());
    }

    private static void sealAStream(ControllerWrapper controllerWrapper, Controller controller,
                                   final ScalingPolicy scalingPolicy, final String scopeSeal,
                                   final String streamNameSeal) throws InterruptedException, ExecutionException {
        controllerWrapper.getControllerService().createScope("scopeSeal").get();

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
