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
import io.pravega.client.SynchronizerClientFactory;
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
import io.pravega.client.stream.impl.ReaderGroupImpl;
import io.pravega.client.stream.impl.SegmentWithRange;
import io.pravega.client.stream.impl.AbstractClientFactoryImpl;
import io.pravega.client.stream.impl.ClientFactoryImpl;
import io.pravega.client.stream.impl.ReaderGroupState;
import io.pravega.client.stream.impl.StreamCutImpl;
import io.pravega.client.stream.mock.MockConnectionFactoryImpl;
import io.pravega.client.stream.mock.MockController;
import io.pravega.client.stream.mock.MockSegmentStreamFactory;
import io.pravega.shared.NameUtils;
import io.pravega.shared.protocol.netty.PravegaNodeUri;
import lombok.Cleanup;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;


import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.stream.IntStream;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.spy;

@RunWith(MockitoJUnitRunner.class)
public class ReaderGroupManagerImplTest {
    private static final String SCOPE = "scope";
    private static final String GROUP_NAME = "readerGroup";
    private ReaderGroupManagerImpl readerGroupManager;
    @Mock
    private ClientFactoryImpl clientFactory;
    @Mock
    private Controller controller;
    @Mock
    private StateSynchronizer<ReaderGroupState> synchronizer;
    @Mock
    private ReaderGroupState state;

    @Before
    public void setUp() throws Exception {
        readerGroupManager = new ReaderGroupManagerImpl(SCOPE, controller, clientFactory);
    }

    @After
    public void shutDown() {
        synchronizer.close();
        controller.close();
        clientFactory.close();
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testCreateReaderGroup() {
        ReaderGroupConfig config = ReaderGroupConfig.builder().startFromStreamCuts(ImmutableMap.<Stream, StreamCut>builder()
                .put(createStream("s1"), createStreamCut("s1", 2))
                .put(createStream("s2"), createStreamCut("s2", 3)).build())
                .retentionType(ReaderGroupConfig.StreamDataRetention.MANUAL_RELEASE_AT_USER_STREAMCUT)
                .build();
        ReaderGroupConfig expectedConfig = config.toBuilder().readerGroupId(UUID.randomUUID()).generation(0L).build();
        when(controller.createReaderGroup(anyString(), anyString(), any(ReaderGroupConfig.class))).thenReturn(CompletableFuture.completedFuture(expectedConfig));
        when(clientFactory.createStateSynchronizer(anyString(), any(Serializer.class), any(Serializer.class),
                any(SynchronizerConfig.class))).thenReturn(synchronizer);
        when(synchronizer.getState()).thenReturn(state);
        when(state.getConfig()).thenReturn(expectedConfig);
        // Create a ReaderGroup
        readerGroupManager.createReaderGroup(GROUP_NAME, config);
        verify(clientFactory, times(1)).createStateSynchronizer(anyString(), any(Serializer.class),
                any(Serializer.class), any(SynchronizerConfig.class));
        verify(synchronizer, times(1)).initialize(any(InitialUpdate.class));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testCreateReaderGroupWithMigration() {
        String scope = "scope";
        String stream = "stream";
        String groupName = "rg1";
        PravegaNodeUri endpoint = new PravegaNodeUri("localhost", 12345);
        MockConnectionFactoryImpl connectionFactory = new MockConnectionFactoryImpl();
        MockController controller = new MockController(endpoint.getEndpoint(), endpoint.getPort(), connectionFactory, false);
        final StreamConfiguration streamConfig = StreamConfiguration.builder()
                .scalingPolicy(ScalingPolicy.fixed(1))
                .build();
        controller.createScope(scope).join();
        controller.createRGStream(scope, groupName, stream, streamConfig).join();
        MockSegmentStreamFactory streamFactory = new MockSegmentStreamFactory();
        @Cleanup
        SynchronizerClientFactory clientFactory = new ClientFactoryImpl(scope, controller, connectionFactory, streamFactory, streamFactory, streamFactory, streamFactory);

        ReaderGroupConfig rgConfig = ReaderGroupConfig.builder()
                .stream(Stream.of("scope", "stream"))
                .retentionType(ReaderGroupConfig.StreamDataRetention.MANUAL_RELEASE_AT_USER_STREAMCUT)
                .build();

        assertEquals(ReaderGroupConfig.DEFAULT_UUID, rgConfig.getReaderGroupId());
        assertEquals(ReaderGroupConfig.DEFAULT_GENERATION, rgConfig.getGeneration());
        @Cleanup
        StateSynchronizer<ReaderGroupState> stateSynchronizer = clientFactory.createStateSynchronizer(NameUtils.getStreamForReaderGroup(groupName),
                new ReaderGroupManagerImpl.ReaderGroupStateUpdatesSerializer(), new ReaderGroupManagerImpl.ReaderGroupStateInitSerializer(), SynchronizerConfig.builder().build());
        Map<SegmentWithRange, Long> segments = ReaderGroupImpl.getSegmentsForStreams(controller, rgConfig);
        StateSynchronizer<ReaderGroupState> spiedSynchronizer = spy(stateSynchronizer);
        SynchronizerClientFactory spiedClientFactory = spy(clientFactory);
        Mockito.doReturn(spiedSynchronizer).when(spiedClientFactory).createStateSynchronizer(anyString(), any(Serializer.class),
                any(Serializer.class), any(SynchronizerConfig.class));
        stateSynchronizer.initialize(new ReaderGroupState.ReaderGroupStateInit(rgConfig, segments, Collections.emptyMap(), false));
        Mockito.doReturn(state).when(spiedSynchronizer).getState();
        Mockito.doNothing().when(spiedSynchronizer).initialize(any(InitialUpdate.class));
        Mockito.doNothing().when(spiedSynchronizer).updateState(any(StateSynchronizer.UpdateGenerator.class));
        when(state.getConfig()).thenReturn(rgConfig);
        ReaderGroupManagerImpl rgManager = new ReaderGroupManagerImpl("scope", controller, (AbstractClientFactoryImpl) spiedClientFactory);

        rgManager.createReaderGroup(groupName, rgConfig);
        verify(spiedSynchronizer, times(1)).initialize(any(InitialUpdate.class));
        verify(spiedSynchronizer, times(1)).updateState(any(StateSynchronizer.UpdateGenerator.class));

    }

    @Test
    @SuppressWarnings("unchecked")
    public void testDeleteReaderGroup() {
        final UUID rgId = UUID.randomUUID();
        ReaderGroupConfig config = ReaderGroupConfig.builder().startFromStreamCuts(ImmutableMap.<Stream, StreamCut>builder()
                .put(createStream("s1"), createStreamCut("s1", 2))
                .put(createStream("s2"), createStreamCut("s2", 3)).build())
                .readerGroupId(rgId).generation(0L)
                .retentionType(ReaderGroupConfig.StreamDataRetention.MANUAL_RELEASE_AT_USER_STREAMCUT)
                .build();
        when(clientFactory.createStateSynchronizer(anyString(), any(Serializer.class), any(Serializer.class),
                any(SynchronizerConfig.class))).thenReturn(synchronizer);
        when(synchronizer.getState()).thenReturn(state);
        when(state.getConfig()).thenReturn(config);
        when(controller.deleteReaderGroup(SCOPE, GROUP_NAME, config.getReaderGroupId(), config.getGeneration())).thenReturn(CompletableFuture.completedFuture(true));
        // Delete ReaderGroup
        readerGroupManager.deleteReaderGroup(GROUP_NAME);
        verify(controller, times(1)).deleteReaderGroup(SCOPE, GROUP_NAME, config.getReaderGroupId(), config.getGeneration());
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testDeleteReaderGroupWithMigration() {
        ReaderGroupConfig config = ReaderGroupConfig.builder().startFromStreamCuts(ImmutableMap.<Stream, StreamCut>builder()
                .put(createStream("s1"), createStreamCut("s1", 2))
                .put(createStream("s2"), createStreamCut("s2", 3)).build())
                .retentionType(ReaderGroupConfig.StreamDataRetention.MANUAL_RELEASE_AT_USER_STREAMCUT)
                .build();
        when(clientFactory.createStateSynchronizer(anyString(), any(Serializer.class), any(Serializer.class),
                any(SynchronizerConfig.class))).thenReturn(synchronizer);
        when(synchronizer.getState()).thenReturn(state);
        when(state.getConfig()).thenReturn(config);

        ReaderGroupConfig expectedConfig = config.toBuilder().readerGroupId(UUID.randomUUID()).generation(0L).build();
        when(controller.createReaderGroup(anyString(), anyString(), any(ReaderGroupConfig.class)))
               .thenReturn(CompletableFuture.completedFuture(expectedConfig));
        when(controller.deleteReaderGroup(anyString(), anyString(), any(UUID.class), anyLong()))
                .thenReturn(CompletableFuture.completedFuture(true));
        // Delete ReaderGroup
        readerGroupManager.deleteReaderGroup(GROUP_NAME);
        verify(controller, times(1)).deleteReaderGroup(SCOPE, GROUP_NAME, expectedConfig.getReaderGroupId(), expectedConfig.getGeneration());
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
