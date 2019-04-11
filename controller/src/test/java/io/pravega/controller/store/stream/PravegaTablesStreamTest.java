/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.store.stream;

import com.google.common.collect.Lists;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.controller.mocks.SegmentHelperMock;
import io.pravega.controller.server.SegmentHelper;
import io.pravega.controller.server.rpc.auth.AuthHelper;
import io.pravega.controller.store.stream.records.EpochTransitionRecord;
import io.pravega.controller.store.stream.records.StreamSegmentRecord;
import io.pravega.test.common.TestingServerStarter;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.TestingServer;
import org.junit.Test;

import java.time.Duration;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static io.pravega.shared.segment.StreamSegmentNameUtils.computeSegmentId;
import static org.junit.Assert.*;

public class PravegaTablesStreamTest extends StreamTestBase {

    private TestingServer zkServer;
    private CuratorFramework cli;
    private PravegaTablesStreamMetadataStore store;
    private PravegaTablesStoreHelper storeHelper;
    private ZkOrderedStore orderer;
    @Override
    public void setup() throws Exception {
        zkServer = new TestingServerStarter().start();
        zkServer.start();
        int sessionTimeout = 8000;
        int connectionTimeout = 5000;
        cli = CuratorFrameworkFactory.newClient(zkServer.getConnectString(), sessionTimeout, connectionTimeout, new RetryOneTime(2000));
        cli.start();
        SegmentHelper segmentHelper = SegmentHelperMock.getSegmentHelperMockForTables(executor);
        AuthHelper authHelper = AuthHelper.getDisabledAuthHelper();
        storeHelper = new PravegaTablesStoreHelper(segmentHelper, authHelper, executor);
        orderer = new ZkOrderedStore("txnOrderer", new ZKStoreHelper(cli, executor), executor);
        store = new PravegaTablesStreamMetadataStore(segmentHelper, cli, executor, Duration.ofSeconds(1), authHelper);
    }

    @Override
    public void tearDown() throws Exception {
        store.close();
        cli.close();
        zkServer.close();
        executor.shutdown();
    }

    @Override
    void createScope(String scope) {
        store.createScope(scope).join();
    }

    @Override
    PersistentStreamBase getStream(String scope, String stream, int chunkSize, int shardSize) {
        PravegaTableScope pravegaTableScope = new PravegaTableScope(scope, storeHelper);
        pravegaTableScope.addStreamToScope(stream).join();
        
        return new PravegaTablesStream(scope, stream, storeHelper, orderer,  
                () -> 0, chunkSize, shardSize, pravegaTableScope::getStreamsInScopeTableName, executor);
    }

    @Test
    public void testPravegaTablesStream() throws Exception {
        double keyChunk = 1.0 / 5;
        final ScalingPolicy policy = ScalingPolicy.fixed(5);

        final String scope = "test";
        final String streamName = "test";
        store.createScope(scope).get();

        StreamConfiguration streamConfig = StreamConfiguration.builder()
                                                              .scalingPolicy(policy)
                                                              .build();

        store.createStream(scope, streamName, streamConfig, System.currentTimeMillis(), null, executor).get();
        store.setState(scope, streamName, State.ACTIVE, null, executor).get();
        store.getState(scope, streamName, false, null, executor);
        store.getState(scope, streamName, false, null, executor);
        store.getState(scope, streamName, false, null, executor);
        State state1 = store.getState(scope, streamName, false, null, executor).join();
        assertEquals(state1, State.ACTIVE);
        OperationContext context = store.createContext(scope, streamName);

        List<StreamSegmentRecord> segments = store.getActiveSegments(scope, streamName, context, executor).get();
        assertEquals(segments.size(), 5);
        assertTrue(segments.stream().allMatch(x -> Lists.newArrayList(0L, 1L, 2L, 3L, 4L).contains(x.segmentId())));

        long start = segments.get(0).getCreationTime();

        assertEquals(store.getConfiguration(scope, streamName, context, executor).get(), streamConfig);

        List<Map.Entry<Double, Double>> newRanges;

        // existing range 0 = 0 - .2, 1 = .2 - .4, 2 = .4 - .6, 3 = .6 - .8, 4 = .8 - 1.0

        // 3, 4 -> 5 = .6 - 1.0
        newRanges = Collections.singletonList(
                new AbstractMap.SimpleEntry<>(3 * keyChunk, 1.0));

        long scale1 = start + 10000;
        ArrayList<Long> sealedSegments = Lists.newArrayList(3L, 4L);
        long five = computeSegmentId(5, 1);
        VersionedMetadata<EpochTransitionRecord> versioned = store.submitScale(scope, streamName, sealedSegments, newRanges, scale1, null, context, executor).get();
        VersionedMetadata<State> state = store.getVersionedState(scope, streamName, null, executor).join();
        state = store.updateVersionedState(scope, streamName, State.SCALING, state, null, executor).join();
        versioned = store.startScale(scope, streamName, false, versioned, state, null, executor).join();
        store.scaleCreateNewEpochs(scope, streamName, versioned, context, executor).get();
        store.scaleSegmentsSealed(scope, streamName, sealedSegments.stream().collect(Collectors.toMap(x -> x, x -> 0L)), versioned,
                context, executor).get();
        store.completeScale(scope, streamName, versioned, null, executor).join();
        store.setState(scope, streamName, State.ACTIVE, null, executor).join();
        state1 = store.getState(scope, streamName, false, null, executor).join();
        assertEquals(state1, State.ACTIVE);

        segments = store.getActiveSegments(scope, streamName, context, executor).get();
        assertEquals(segments.size(), 4);
        assertTrue(segments.stream().allMatch(x -> Lists.newArrayList(0L, 1L, 2L, five).contains(x.segmentId())));

        // 1 -> 6 = 0.2 -.3, 7 = .3 - .4
        // 2,5 -> 8 = .4 - 1.0
        newRanges = Arrays.asList(
                new AbstractMap.SimpleEntry<>(keyChunk, 0.3),
                new AbstractMap.SimpleEntry<>(0.3, 2 * keyChunk),
                new AbstractMap.SimpleEntry<>(2 * keyChunk, 1.0));

        long scale2 = scale1 + 10000;
        ArrayList<Long> sealedSegments1 = Lists.newArrayList(1L, 2L, five);
        long six = computeSegmentId(6, 2);
        long seven = computeSegmentId(7, 2);
        long eight = computeSegmentId(8, 2);
        versioned = store.submitScale(scope, streamName, sealedSegments1, newRanges, scale2, null, context, executor).get();
        EpochTransitionRecord response = versioned.getObject();
        state = store.getVersionedState(scope, streamName, null, executor).join();
        state = store.updateVersionedState(scope, streamName, State.SCALING, state, null, executor).join();
        versioned = store.startScale(scope, streamName, false, versioned, state, null, executor).join();
        store.scaleCreateNewEpochs(scope, streamName, versioned, context, executor).get();
        store.scaleSegmentsSealed(scope, streamName, sealedSegments1.stream().collect(Collectors.toMap(x -> x, x -> 0L)), versioned,
                context, executor).get();
        store.completeScale(scope, streamName, versioned, null, executor).join();
        store.setState(scope, streamName, State.ACTIVE, null, executor).join();
        state1 = store.getState(scope, streamName, false, null, executor).join();
        assertEquals(state1, State.ACTIVE);

        segments = store.getActiveSegments(scope, streamName, context, executor).get();
        assertEquals(segments.size(), 4);
        assertTrue(segments.stream().allMatch(x -> Lists.newArrayList(0L, six, seven, eight).contains(x.segmentId())));

        // 7 -> 9 = .3 - .35, 10 = .35 - .6
        // 8 -> 10 = .35 - .6, 11 = .6 - 1.0
        newRanges = Arrays.asList(
                new AbstractMap.SimpleEntry<>(0.3, 0.35),
                new AbstractMap.SimpleEntry<>(0.35, 3 * keyChunk),
                new AbstractMap.SimpleEntry<>(3 * keyChunk, 1.0));

        long scale3 = scale2 + 10000;
        ArrayList<Long> sealedSegments2 = Lists.newArrayList(seven, eight);
        versioned = store.submitScale(scope, streamName, sealedSegments2, newRanges, scale3, null, context, executor).get();
        state = store.getVersionedState(scope, streamName, null, executor).join();
        state = store.updateVersionedState(scope, streamName, State.SCALING, state, null, executor).join();
        store.startScale(scope, streamName, false, versioned, state, null, executor).join();
        store.scaleCreateNewEpochs(scope, streamName, versioned, context, executor).get();
        store.scaleSegmentsSealed(scope, streamName, sealedSegments2.stream().collect(Collectors.toMap(x -> x, x -> 0L)), versioned,
                context, executor).get();
        store.completeScale(scope, streamName, versioned, null, executor).join();
        store.setState(scope, streamName, State.ACTIVE, null, executor).join();
        state1 = store.getState(scope, streamName, false, null, executor).join();
        assertEquals(state1, State.ACTIVE);
    }
}
