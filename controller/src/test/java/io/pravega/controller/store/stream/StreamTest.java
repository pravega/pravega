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
import io.pravega.controller.store.stream.tables.State;
import io.pravega.test.common.TestingServerStarter;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;

public class StreamTest {
    private TestingServer zkTestServer;
    private CuratorFramework cli;

    private final ScheduledExecutorService executor = Executors.newScheduledThreadPool(10);

    @Before
    public void setUp() throws Exception {
        zkTestServer = new TestingServerStarter().start();
        cli = CuratorFrameworkFactory.newClient(zkTestServer.getConnectString(), new RetryOneTime(2000));
        cli.start();
    }

    @After
    public void tearDown() throws Exception {
        cli.close();
        zkTestServer.close();
        executor.shutdown();
    }

    @Test(timeout = 10000)
    public void testZkCreateStream() throws ExecutionException, InterruptedException {
        ZKStoreHelper zkStoreHelper = new ZKStoreHelper(cli, executor);
        ZKStream zkStream = new ZKStream("test", "test", zkStoreHelper);
        testStream(zkStream);
    }

    @Test(timeout = 10000)
    public void testInMemoryCreateStream() throws ExecutionException, InterruptedException {
        InMemoryStream stream = new InMemoryStream("test", "test");
        testStream(stream);
    }

    private void testStream(PersistentStreamBase<Integer> stream) throws InterruptedException, ExecutionException {
        long creationTime1 = System.currentTimeMillis();
        long creationTime2 = creationTime1 + 1;
        final ScalingPolicy policy1 = ScalingPolicy.fixed(5);
        final ScalingPolicy policy2 = ScalingPolicy.fixed(6);

        final StreamConfiguration streamConfig1 = StreamConfiguration.builder().scope("test").streamName("test").scalingPolicy(policy1).build();
        final StreamConfiguration streamConfig2 = StreamConfiguration.builder().scope("test").streamName("test").scalingPolicy(policy2).build();

        CreateStreamResponse response = stream.checkStreamExists(streamConfig1, creationTime1).get();
        assertEquals(CreateStreamResponse.CreateStatus.NEW, response.getStatus());
        stream.storeCreationTimeIfAbsent(creationTime1).get();

        response = stream.checkStreamExists(streamConfig1, creationTime1).get();
        assertEquals(CreateStreamResponse.CreateStatus.NEW, response.getStatus());
        response = stream.checkStreamExists(streamConfig2, creationTime1).get();
        assertEquals(CreateStreamResponse.CreateStatus.NEW, response.getStatus());
        response = stream.checkStreamExists(streamConfig2, creationTime2).get();
        assertEquals(CreateStreamResponse.CreateStatus.NEW, response.getStatus());

        stream.createConfigurationIfAbsent(streamConfig1).get();

        response = stream.checkStreamExists(streamConfig1, creationTime1).get();
        assertEquals(CreateStreamResponse.CreateStatus.NEW, response.getStatus());
        response = stream.checkStreamExists(streamConfig2, creationTime1).get();
        assertEquals(CreateStreamResponse.CreateStatus.NEW, response.getStatus());
        response = stream.checkStreamExists(streamConfig2, creationTime2).get();
        assertEquals(CreateStreamResponse.CreateStatus.EXISTS_CREATING, response.getStatus());

        stream.createStateIfAbsent(State.UNKNOWN).get();

        response = stream.checkStreamExists(streamConfig1, creationTime1).get();
        assertEquals(CreateStreamResponse.CreateStatus.NEW, response.getStatus());
        response = stream.checkStreamExists(streamConfig2, creationTime1).get();
        assertEquals(CreateStreamResponse.CreateStatus.NEW, response.getStatus());
        response = stream.checkStreamExists(streamConfig2, creationTime2).get();
        assertEquals(CreateStreamResponse.CreateStatus.EXISTS_CREATING, response.getStatus());

        stream.updateState(State.CREATING).get();

        response = stream.checkStreamExists(streamConfig1, creationTime1).get();
        assertEquals(CreateStreamResponse.CreateStatus.NEW, response.getStatus());
        response = stream.checkStreamExists(streamConfig2, creationTime1).get();
        assertEquals(CreateStreamResponse.CreateStatus.NEW, response.getStatus());
        response = stream.checkStreamExists(streamConfig2, creationTime2).get();
        assertEquals(CreateStreamResponse.CreateStatus.EXISTS_CREATING, response.getStatus());

        stream.updateState(State.ACTIVE).get();

        response = stream.checkStreamExists(streamConfig1, creationTime1).get();
        assertEquals(CreateStreamResponse.CreateStatus.EXISTS_ACTIVE, response.getStatus());
        response = stream.checkStreamExists(streamConfig2, creationTime1).get();
        assertEquals(CreateStreamResponse.CreateStatus.EXISTS_ACTIVE, response.getStatus());
        response = stream.checkStreamExists(streamConfig2, creationTime2).get();
        assertEquals(CreateStreamResponse.CreateStatus.EXISTS_ACTIVE, response.getStatus());

        stream.updateState(State.SEALING).get();

        response = stream.checkStreamExists(streamConfig1, creationTime1).get();
        assertEquals(CreateStreamResponse.CreateStatus.EXISTS_ACTIVE, response.getStatus());
        response = stream.checkStreamExists(streamConfig2, creationTime1).get();
        assertEquals(CreateStreamResponse.CreateStatus.EXISTS_ACTIVE, response.getStatus());
        response = stream.checkStreamExists(streamConfig2, creationTime2).get();
        assertEquals(CreateStreamResponse.CreateStatus.EXISTS_ACTIVE, response.getStatus());
    }

    @Test
    public void testNoValuePresent()  throws Exception {
        final ScalingPolicy policy = ScalingPolicy.fixed(1);

        final StreamMetadataStore store = new ZKStreamMetadataStore(cli, executor);
        final String streamName = "test";
        String scopeName = "test";
        store.createScope(scopeName).get();

        ZKStoreHelper zkStoreHelper = new ZKStoreHelper(cli, executor);

        StreamConfiguration streamConfig = StreamConfiguration.builder()
                .scope(streamName)
                .streamName(streamName)
                .scalingPolicy(policy)
                .build();

        store.createStream(scopeName, streamName, streamConfig, System.currentTimeMillis(), null, executor).get();
        store.setState(scopeName, streamName, State.ACTIVE, null, executor).get();

        ZKStream zkStream = new ZKStream("test", "test", zkStoreHelper);

        List<AbstractMap.SimpleEntry<Double, Double>> newRanges;

        newRanges = Arrays.asList(new AbstractMap.SimpleEntry<>(0.0, 0.5), new AbstractMap.SimpleEntry<>(0.5, 1.0));

        long scale = System.currentTimeMillis();
        ArrayList<Integer> sealedSegments = Lists.newArrayList(0);

        StartScaleResponse response = zkStream.startScale(sealedSegments, newRanges, scale, false).join();
        List<Segment> newSegments = response.getSegmentsCreated();
        zkStream.updateState(State.SCALING).join();

        List<Integer> newSegmentInt = newSegments.stream().map(Segment::getNumber).collect(Collectors.toList());
        zkStream.scaleNewSegmentsCreated(sealedSegments, newSegmentInt,
                response.getActiveEpoch(), scale).get();
        // history table has a partial record at this point.
        // now we could have sealed the segments so get successors could be called.
        Map<Integer, List<Integer>> successors = zkStream.getSuccessorsWithPredecessors(0).get();

        zkStream.scaleOldSegmentsSealed(sealedSegments, newSegmentInt, response.getActiveEpoch(), scale).get();
        // scale is completed, history table also has completed record now.

        successors = zkStream.getSuccessorsWithPredecessors(0).get();
    }

}
