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

import com.google.common.annotations.VisibleForTesting;
import io.pravega.client.ClientFactory;
import io.pravega.client.netty.impl.ConnectionFactory;
import io.pravega.client.segment.impl.Segment;
import io.pravega.client.segment.impl.SegmentMetadataClient;
import io.pravega.client.segment.impl.SegmentMetadataClientFactory;
import io.pravega.client.segment.impl.SegmentMetadataClientFactoryImpl;
import io.pravega.client.state.StateSynchronizer;
import io.pravega.client.state.SynchronizerConfig;
import io.pravega.client.stream.Checkpoint;
import io.pravega.client.stream.InvalidStreamException;
import io.pravega.client.stream.Position;
import io.pravega.client.stream.ReaderGroup;
import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.client.stream.ReaderGroupMetrics;
import io.pravega.client.stream.Serializer;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.impl.ReaderGroupState.ClearCheckpoints;
import io.pravega.client.stream.impl.ReaderGroupState.CreateCheckpoint;
import io.pravega.client.stream.impl.ReaderGroupState.ReaderGroupStateInit;
import io.pravega.client.stream.impl.ReaderGroupState.ReaderGroupStateUpdate;
import io.pravega.client.stream.notifications.NotificationSystem;
import io.pravega.client.stream.notifications.Observable;
import io.pravega.client.stream.notifications.events.CustomEvent;
import io.pravega.client.stream.notifications.events.ScaleEvent;
import io.pravega.common.concurrent.FutureHelpers;
import io.pravega.shared.NameUtils;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import lombok.Cleanup;
import lombok.Data;
import lombok.Getter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import static io.pravega.common.concurrent.FutureHelpers.allOfWithResults;
import static io.pravega.common.concurrent.FutureHelpers.getAndHandleExceptions;

@Slf4j
@Data
public class ReaderGroupImpl implements ReaderGroup, ReaderGroupMetrics {

    private final String scope;
    private final String groupName;
    private final SynchronizerConfig synchronizerConfig;
    private final Serializer<ReaderGroupStateInit> initSerializer;
    private final Serializer<ReaderGroupStateUpdate> updateSerializer;
    private final ClientFactory clientFactory;
    private final Controller controller;
    private final ConnectionFactory connectionFactory;
    @Getter
    private final NotificationSystem notificationSystem = new NotificationSystem();

    /**
     * Called by the StreamManager to provide the streams the group should start reading from.
     * @param  config The configuration for the reader group.
     * @param streams The segments to use and where to start from.
     */
    @VisibleForTesting
    public void initializeGroup(ReaderGroupConfig config, Set<String> streams) {
        @Cleanup
        StateSynchronizer<ReaderGroupState> synchronizer = createSynchronizer();
        Map<Segment, Long> segments = getSegmentsForStreams(streams);
        ReaderGroupStateManager.initializeReaderGroup(synchronizer, config, segments);
    }
    
    private Map<Segment, Long> getSegmentsForStreams(Set<String> streams) {
        List<CompletableFuture<Map<Segment, Long>>> futures = new ArrayList<>(streams.size());
        for (String stream : streams) {
            futures.add(controller.getSegmentsAtTime(new StreamImpl(scope, stream), 0L));
        }
        return getAndHandleExceptions(allOfWithResults(futures).thenApply(listOfMaps -> {
            return listOfMaps.stream()
                             .flatMap(map -> map.entrySet().stream())
                             .collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue()));
        }), InvalidStreamException::new);
    }

    @Override
    public void readerOffline(String readerId, Position lastPosition) {
        @Cleanup
        StateSynchronizer<ReaderGroupState> synchronizer = createSynchronizer();
        ReaderGroupStateManager.readerShutdown(readerId, lastPosition, synchronizer);
    }

    private StateSynchronizer<ReaderGroupState> createSynchronizer() {
        return clientFactory.createStateSynchronizer(NameUtils.getStreamForReaderGroup(groupName),
                updateSerializer, initSerializer, synchronizerConfig);
    }

    @Override
    public Set<String> getOnlineReaders() {
        @Cleanup
        StateSynchronizer<ReaderGroupState> synchronizer = createSynchronizer();
        synchronizer.fetchUpdates();
        return synchronizer.getState().getOnlineReaders();
    }

    @Override
    public Set<String> getStreamNames() {
        @Cleanup
        StateSynchronizer<ReaderGroupState> synchronizer = createSynchronizer();
        synchronizer.fetchUpdates();
        return synchronizer.getState().getStreamNames();
    }

    @Override
    public CompletableFuture<Checkpoint> initiateCheckpoint(String checkpointName, ScheduledExecutorService backgroundExecutor) {
        StateSynchronizer<ReaderGroupState> synchronizer = createSynchronizer();
        synchronizer.updateStateUnconditionally(new CreateCheckpoint(checkpointName));
        AtomicBoolean checkpointPending = new AtomicBoolean(true);

        return FutureHelpers.loop(checkpointPending::get, () -> {
            return FutureHelpers.delayedTask(() -> {
                synchronizer.fetchUpdates();
                checkpointPending.set(!synchronizer.getState().isCheckpointComplete(checkpointName));
                if (checkpointPending.get()) {
                    log.debug("Waiting on checkpoint: {} currentState is: {}", checkpointName, synchronizer.getState());
                }
                return null;
            }, Duration.ofMillis(500), backgroundExecutor);
        }, backgroundExecutor)
                            .thenApply(v -> completeCheckpoint(checkpointName, synchronizer))
                            .whenComplete((v, t) -> synchronizer.close());
    }

    @SneakyThrows(CheckpointFailedException.class)
    private Checkpoint completeCheckpoint(String checkpointName, StateSynchronizer<ReaderGroupState> synchronizer) {
        Map<Segment, Long> map = synchronizer.getState().getPositionsForCompletedCheckpoint(checkpointName);
        synchronizer.updateStateUnconditionally(new ClearCheckpoints(checkpointName));
        if (map == null) {
            throw new CheckpointFailedException("Checkpoint was cleared before results could be read.");
        }
        return new CheckpointImpl(checkpointName, map);
    }

    @Override
    public void resetReadersToCheckpoint(Checkpoint checkpoint) {
        @Cleanup
        StateSynchronizer<ReaderGroupState> synchronizer = createSynchronizer();
        synchronizer.updateState(state -> {
            ReaderGroupConfig config = state.getConfig();
            Map<Segment, Long> positions = new HashMap<>();
            for (StreamCut cut : checkpoint.asImpl().getPositions().values()) {
                positions.putAll(cut.getPositions());
            }
            return Collections.singletonList(new ReaderGroupStateInit(config, positions));
        });
    }

    @Override
    public void updateConfig(ReaderGroupConfig config, Set<String> streamNames) {
        @Cleanup
        StateSynchronizer<ReaderGroupState> synchronizer = createSynchronizer();
        Map<Segment, Long> segments = getSegmentsForStreams(streamNames);
        synchronizer.updateStateUnconditionally(new ReaderGroupStateInit(config, segments));
    }

    @Override
    public ReaderGroupMetrics getMetrics() {
        return this;
    }

    @Override
    public long unreadBytes() {
        @Cleanup
        StateSynchronizer<ReaderGroupState> synchronizer = createSynchronizer();
        Map<Stream, Map<Segment, Long>> positions = synchronizer.getState().getPositions();
        SegmentMetadataClientFactory metaFactory = new SegmentMetadataClientFactoryImpl(controller, connectionFactory);
        
        long totalLength = 0;
        for (Entry<Stream, Map<Segment, Long>> streamPosition : positions.entrySet()) {
            StreamCut position = new StreamCut(streamPosition.getKey(), streamPosition.getValue());
            totalLength += getRemainingBytes(metaFactory, position);
        }
        return totalLength;
    }
    
    private long getRemainingBytes(SegmentMetadataClientFactory metaFactory, StreamCut position) {
        long totalLength = 0;
        CompletableFuture<Set<Segment>> unread = controller.getSuccessors(position);
        for (Segment s : FutureHelpers.getAndHandleExceptions(unread, RuntimeException::new)) {
            @Cleanup
            SegmentMetadataClient metadataClient = metaFactory.createSegmentMetadataClient(s);
            totalLength += metadataClient.fetchCurrentStreamLength();
        }
        for (long bytesRead : position.getPositions().values()) {
            totalLength -= bytesRead;
        }
        return totalLength;
    }

    @Override
    public Observable<ScaleEvent> getScaleEventNotifier() {
        return this.notificationSystem.getFactory().getScaleNotifier(this::getCurrentScaleEvent);
    }

    private ScaleEvent getCurrentScaleEvent() {
        @Cleanup
        StateSynchronizer<ReaderGroupState> synchronizer = createSynchronizer();
        synchronizer.fetchUpdates();
        ReaderGroupState state = synchronizer.getState();
        return ScaleEvent.builder().numOfSegments(state.getNumberOfSegments())
                         .numOfReaders(state.getOnlineReaders().size())
                         .build();
    }

    @Override
    public Observable<CustomEvent> getCustomEventNotifier() {
        return this.notificationSystem.getFactory().getCustomNotifier();
    }
}
