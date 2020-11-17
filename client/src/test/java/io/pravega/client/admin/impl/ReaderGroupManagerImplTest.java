/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.admin.impl;

import com.google.common.collect.ImmutableMap;
import io.pravega.client.connection.impl.ConnectionPool;
import io.pravega.client.control.impl.Controller;
import io.pravega.client.segment.impl.Segment;
import io.pravega.client.state.InitialUpdate;
import io.pravega.client.state.StateSynchronizer;
import io.pravega.client.state.SynchronizerConfig;
import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.Serializer;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.StreamCut;
import io.pravega.client.stream.impl.AbstractClientFactoryImpl;
import io.pravega.client.stream.impl.ReaderGroupState;
import io.pravega.client.stream.impl.StreamCutImpl;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.IntStream;

import static io.pravega.shared.NameUtils.getStreamForReaderGroup;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class ReaderGroupManagerImplTest {

    private static final String SCOPE = "scope";
    private static final String GROUP_NAME = "readerGroup";
    private ReaderGroupManagerImpl readerGroupManager;
    @Mock
    private AbstractClientFactoryImpl clientFactory;
    @Mock
    private Controller controller;
    @Mock
    private ConnectionPool connectionPool;
    @Mock
    private StateSynchronizer<ReaderGroupState> synchronizer;
    @Mock
    private ReaderGroupState state;

    @Before
    public void setUp() throws Exception {
        when(synchronizer.getState()).thenReturn(state);
        when(clientFactory.getConnectionPool()).thenReturn(connectionPool);

        readerGroupManager = new ReaderGroupManagerImpl(SCOPE, controller, clientFactory);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testCreateSubscriberReaderGroup() {
        // Setup mocks
        when(controller.addSubscriber(SCOPE, "s1", GROUP_NAME)).thenReturn(CompletableFuture.completedFuture(true));
        when(controller.addSubscriber(SCOPE, "s2", GROUP_NAME)).thenReturn(CompletableFuture.completedFuture(true));
        when(controller.createStream(SCOPE, getStreamForReaderGroup(GROUP_NAME), StreamConfiguration.builder()
                .scalingPolicy(ScalingPolicy.fixed(1))
                .build())).thenReturn(CompletableFuture.completedFuture(true));
        when(clientFactory.createStateSynchronizer(anyString(), any(Serializer.class), any(Serializer.class),
                any(SynchronizerConfig.class))).thenReturn(synchronizer);
        when(synchronizer.getState()).thenReturn(state);
        when(state.getConfigState()).thenReturn(ReaderGroupState.ConfigState.INITIALIZING);
        ReaderGroupConfig config = ReaderGroupConfig.builder().startFromStreamCuts(ImmutableMap.<Stream, StreamCut>builder()
                .put(createStream("s1"), createStreamCut("s1", 2))
                .put(createStream("s2"), createStreamCut("s2", 3)).build())
                .retentionConfig(ReaderGroupConfig.RetentionConfig.TRUNCATE_AT_USER_STREAMCUT)
                .build();
        when(state.getConfig()).thenReturn(config);

        // Create a ReaderGroup
        readerGroupManager.createReaderGroup(GROUP_NAME, config);
        verify(controller, times(1)).addSubscriber(SCOPE, "s1", GROUP_NAME);
        verify(controller, times(1)).addSubscriber(SCOPE, "s2", GROUP_NAME);
        verify(controller, times(1)).createStream(SCOPE, getStreamForReaderGroup(GROUP_NAME), StreamConfiguration.builder()
                .scalingPolicy(ScalingPolicy.fixed(1))
                .build());
        verify(clientFactory, times(2)).createStateSynchronizer(anyString(), any(Serializer.class),
                any(Serializer.class), any(SynchronizerConfig.class));
        verify(synchronizer, times(1)).initialize(any(InitialUpdate.class));
        verify(synchronizer, times(1)).updateState(any(StateSynchronizer.UpdateGenerator.class));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testDeleteSubscriberReaderGroupSuccess() {
        ReaderGroupConfig config = ReaderGroupConfig.builder().startFromStreamCuts(ImmutableMap.<Stream, StreamCut>builder()
                .put(createStream("s1"), createStreamCut("s1", 2))
                .put(createStream("s2"), createStreamCut("s2", 3)).build())
                .retentionConfig(ReaderGroupConfig.RetentionConfig.TRUNCATE_AT_USER_STREAMCUT)
                .build();
        // Setup mocks
        when(clientFactory.createStateSynchronizer(anyString(), any(Serializer.class), any(Serializer.class),
                any(SynchronizerConfig.class))).thenReturn(synchronizer);
        when(synchronizer.getState()).thenReturn(state);
        when(state.getConfigState()).thenReturn(ReaderGroupState.ConfigState.READY, ReaderGroupState.ConfigState.DELETING);
        when(state.getConfig()).thenReturn(config);
        when(controller.deleteSubscriber(SCOPE, "s1", GROUP_NAME)).thenReturn(CompletableFuture.completedFuture(true));
        when(controller.deleteSubscriber(SCOPE, "s2", GROUP_NAME)).thenReturn(CompletableFuture.completedFuture(true));
        when(controller.sealStream(SCOPE, getStreamForReaderGroup(GROUP_NAME))).thenReturn(CompletableFuture.completedFuture(true));
        when(controller.deleteStream(SCOPE, getStreamForReaderGroup(GROUP_NAME))).thenReturn(CompletableFuture.completedFuture(true));

        // Delete ReaderGroup
        readerGroupManager.deleteReaderGroup(GROUP_NAME);

        verify(clientFactory, times(1)).createStateSynchronizer(anyString(), any(Serializer.class),
                any(Serializer.class), any(SynchronizerConfig.class));
        verify(synchronizer, times(1)).updateState(any(StateSynchronizer.UpdateGenerator.class));
        verify(controller, times(1)).deleteSubscriber(SCOPE, "s1", GROUP_NAME);
        verify(controller, times(1)).deleteSubscriber(SCOPE, "s2", GROUP_NAME);
        verify(controller, times(1)).sealStream(SCOPE, getStreamForReaderGroup(GROUP_NAME));
        verify(controller, times(1)).deleteStream(SCOPE, getStreamForReaderGroup(GROUP_NAME));
    }

    @Test(expected = IllegalStateException.class)
    @SuppressWarnings("unchecked")
    public void testDeleteSubscriberReaderGroupFailure() {
        // Setup mocks
        when(clientFactory.createStateSynchronizer(anyString(), any(Serializer.class), any(Serializer.class),
                any(SynchronizerConfig.class))).thenReturn(synchronizer);
        when(synchronizer.getState()).thenReturn(state);
        when(state.getConfigState()).thenReturn(ReaderGroupState.ConfigState.REINITIALIZING);

        // Delete ReaderGroup
        readerGroupManager.deleteReaderGroup(GROUP_NAME);
    }

    private StreamCut createStreamCut(String streamName, int numberOfSegments) {
        Map<Segment, Long> positions = new HashMap<>();
        IntStream.of(numberOfSegments).forEach(segNum -> positions.put(new Segment(SCOPE, streamName, segNum), 10L));
        return new StreamCutImpl(createStream(streamName), positions);
    }

    private Stream createStream(String streamName) {
        return Stream.of(SCOPE, streamName);
    }
}
