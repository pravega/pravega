/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.stream.impl;

import com.google.common.collect.ImmutableMap;
import io.pravega.client.ClientFactory;
import io.pravega.client.netty.impl.ConnectionFactory;
import io.pravega.client.segment.impl.Segment;
import io.pravega.client.state.StateSynchronizer;
import io.pravega.client.state.SynchronizerConfig;
import io.pravega.client.state.Update;
import io.pravega.client.stream.Checkpoint;
import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.client.stream.Serializer;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamCut;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.IntStream;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class ReaderGroupImplTest {

    private static final String SCOPE = "scope";
    private static final String GROUP_NAME = "readerGroup";
    private ReaderGroupImpl readerGroup;
    @Mock
    private SynchronizerConfig synchronizerConfig;
    @Mock
    private ClientFactory clientFactory;
    @Mock
    private Controller controller;
    @Mock
    private ConnectionFactory connectionFactory;
    @Mock
    private StateSynchronizer<ReaderGroupState> synchronizer;
    @Mock
    private ReaderGroupState state;

    private Serializer<ReaderGroupState.ReaderGroupStateInit> initSerializer = new JavaSerializer<>();
    private Serializer<ReaderGroupState.ReaderGroupStateUpdate> updateSerializer = new JavaSerializer<>();

    @Before
    public void setUp() throws Exception {
        readerGroup = new ReaderGroupImpl(SCOPE, GROUP_NAME, synchronizerConfig, initSerializer,
                updateSerializer, clientFactory, controller, connectionFactory);
        when(clientFactory.createStateSynchronizer(anyString(), any(Serializer.class), any(Serializer.class),
                any(SynchronizerConfig.class))).thenReturn(synchronizer);
    }

    @Test(expected = IllegalArgumentException.class)
    public void resetReadersToStreamCutDuplicateStreamCut() {
        readerGroup.resetReaderGroup(ReaderGroupConfig.builder().startFromStreamCuts(ImmutableMap.<Stream, StreamCut>builder()
                .put(createStream("s1"), createStreamCut("s1", 2))
                .put(createStream("s2"), createStreamCut("s1", 3)).build())
        .build());
    }

    @Test(expected = IllegalArgumentException.class)
    public void resetReadersToStreamMissingStreamCut() {
        readerGroup.resetReaderGroup(ReaderGroupConfig.builder().startFromStreamCuts(ImmutableMap.<Stream, StreamCut>builder()
                .put(createStream("s1"), createStreamCut("s2", 2)).build()).build());
    }

    @Test
    public void resetReadersToStreamCut() {
        readerGroup.resetReaderGroup(ReaderGroupConfig.builder().startFromStreamCuts(ImmutableMap.<Stream,
                StreamCut>builder()
                .put(createStream("s1"), createStreamCut("s1", 2))
                .put(createStream("s2"), createStreamCut("s2", 3)).build())
                                                      .build());
        verify(synchronizer, times(1)).updateStateUnconditionally(any(Update.class));
    }

    @Test
    public void resetReadersToCheckpoint() {
        Map<Segment, Long> positions = new HashMap<>();
        IntStream.of(2).forEach(segNum -> positions.put(new Segment(SCOPE, "s1", segNum), 10L));
        Checkpoint checkpoint = new CheckpointImpl("testChkPoint", positions);
        readerGroup.resetReaderGroup(ReaderGroupConfig.builder().startFromCheckpoint(checkpoint).build());
        verify(synchronizer, times(1)).updateStateUnconditionally(any(Update.class));
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
