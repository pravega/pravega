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

import io.pravega.client.stream.ScalingPolicy;
import io.pravega.test.common.TestingServerStarter;
import io.pravega.controller.store.stream.tables.State;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.test.common.AssertExtensions;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.TestingServer;
import org.junit.Test;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Zookeeper based stream metadata store tests.
 */
public class ZKStreamMetadataStoreTest extends StreamMetadataStoreTest {

    private TestingServer zkServer;
    private CuratorFramework cli;

    @Override
    public void setupTaskStore() throws Exception {
        zkServer = new TestingServerStarter().start();
        zkServer.start();
        cli = CuratorFrameworkFactory.newClient(zkServer.getConnectString(), new RetryOneTime(2000));
        cli.start();
        store = new ZKStreamMetadataStore(cli, executor);
    }

    @Override
    public void cleanupTaskStore() throws IOException {
        cli.close();
        zkServer.close();
    }

    @Test
    public void listStreamsWithInactiveStream() throws Exception {
        // list stream in scope
        store.createScope("Scope").get();
        store.createStream("Scope", stream1, configuration1, System.currentTimeMillis(), null, executor).get();
        store.setState("Scope", stream1, State.ACTIVE, null, executor).get();

        store.createStream("Scope", stream2, configuration2, System.currentTimeMillis(), null, executor).get();
        store.setState("Scope", stream1, State.CREATING, null, executor).get();

        List<StreamConfiguration> streamInScope = store.listStreamsInScope("Scope").get();
        assertEquals("List streams in scope", 2, streamInScope.size());
        assertEquals("List streams in scope", stream1, streamInScope.get(0).getStreamName());
        assertEquals("List streams in scope", stream2, streamInScope.get(1).getStreamName());
    }

    @Test
    public void testInvalidOperation() throws Exception {
        // Test operation when stream is not in active state
        store.createScope(scope).get();
        store.createStream(scope, stream1, configuration1, System.currentTimeMillis(), null, executor).get();
        store.setState(scope, stream1, State.CREATING, null, executor).get();

        AssertExtensions.assertThrows("Should throw IllegalStateException",
                store.getActiveSegments(scope, stream1, null, executor),
                (Throwable t) -> t instanceof IllegalStateException);
    }

    @Test
    public void testScaleMetadata() throws Exception {
        String scope = "testScopeScale";
        String stream = "testStreamScale";
        ScalingPolicy policy = ScalingPolicy.fixed(3);
        StreamConfiguration configuration = StreamConfiguration.builder().scope(scope).streamName(stream).scalingPolicy(policy).build();

        store.createScope(scope).get();
        store.createStream(scope, stream, configuration, System.currentTimeMillis(), null, executor).get();
        store.setState(scope, stream, State.ACTIVE, null, executor).get();

        List<ScaleMetadata> scaleIncidents = store.getScaleMetadata(scope, stream, null, executor).get();
        assertTrue(scaleIncidents.size() == 1);
        assertTrue(scaleIncidents.get(0).getSegments().size() == 3);
        // scale
        scale(scope, stream, scaleIncidents.get(0).getSegments());
        scaleIncidents = store.getScaleMetadata(scope, stream, null, executor).get();
        assertTrue(scaleIncidents.size() == 2);
        assertTrue(scaleIncidents.get(0).getSegments().size() == 2);
        assertTrue(scaleIncidents.get(1).getSegments().size() == 3);

        // scale again
        scale(scope, stream, scaleIncidents.get(0).getSegments());
        scaleIncidents = store.getScaleMetadata(scope, stream, null, executor).get();
        assertTrue(scaleIncidents.size() == 3);
        assertTrue(scaleIncidents.get(0).getSegments().size() == 2);
        assertTrue(scaleIncidents.get(1).getSegments().size() == 2);

        // scale again
        scale(scope, stream, scaleIncidents.get(0).getSegments());
        scaleIncidents = store.getScaleMetadata(scope, stream, null, executor).get();
        assertTrue(scaleIncidents.size() == 4);
        assertTrue(scaleIncidents.get(0).getSegments().size() == 2);
        assertTrue(scaleIncidents.get(1).getSegments().size() == 2);
    }

    private void scale(String scope, String stream, List<Segment> segments) {

        AbstractMap.SimpleEntry<Double, Double> segment1 = new AbstractMap.SimpleEntry<>(0.0, 0.5);
        AbstractMap.SimpleEntry<Double, Double> segment2 = new AbstractMap.SimpleEntry<>(0.5, 1.0);
        long scaleTimestamp = System.currentTimeMillis();
        List<Integer> existingSegments = segments.stream().map(Segment::getNumber).collect(Collectors.toList());
        List<Segment> segmentsCreated = store.startScale(scope, stream, existingSegments, Arrays.asList(segment1, segment2),
                scaleTimestamp, null, executor).join();
        store.scaleNewSegmentsCreated(scope, stream, existingSegments, segmentsCreated, scaleTimestamp, null, executor).join();
        store.scaleSegmentsSealed(scope, stream, existingSegments, segmentsCreated, scaleTimestamp, null, executor).join();
    }
}
