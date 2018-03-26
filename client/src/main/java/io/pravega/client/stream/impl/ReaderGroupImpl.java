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
import io.pravega.client.stream.StreamCut;
import io.pravega.client.stream.impl.ReaderGroupState.ClearCheckpoints;
import io.pravega.client.stream.impl.ReaderGroupState.CreateCheckpoint;
import io.pravega.client.stream.impl.ReaderGroupState.ReaderGroupStateInit;
import io.pravega.client.stream.impl.ReaderGroupState.ReaderGroupStateUpdate;
import io.pravega.client.stream.notifications.EndOfDataNotification;
import io.pravega.client.stream.notifications.NotificationSystem;
import io.pravega.client.stream.notifications.NotifierFactory;
import io.pravega.client.stream.notifications.Observable;
import io.pravega.client.stream.notifications.SegmentNotification;
import io.pravega.common.concurrent.Futures;
import io.pravega.shared.NameUtils;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import lombok.Cleanup;
import lombok.Data;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import static com.google.common.base.Preconditions.checkNotNull;
import static io.pravega.common.concurrent.Futures.allOfWithResults;
import static io.pravega.common.concurrent.Futures.getAndHandleExceptions;

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
    private final NotificationSystem notificationSystem = new NotificationSystem();
    private final NotifierFactory notifierFactory = new NotifierFactory(notificationSystem, this::createSynchronizer);

    /**
     * Called by the StreamManager to provide the streams the group should start reading from.
     * @param  config The configuration for the reader group.
     */
    @VisibleForTesting
    public void initializeGroup(ReaderGroupConfig config) {
        @Cleanup
        StateSynchronizer<ReaderGroupState> synchronizer = createSynchronizer();
        ReaderGroupStateManager.initializeReaderGroup(synchronizer, config, getSegmentsForStreams(config), getEndSegmentsForStreams(config));
    }

    private Map<Segment, Long> getSegmentsForStreams(ReaderGroupConfig config) {
        Map<Stream, StreamCut> streamToStreamCuts = config.getStartingStreamCuts();
        final List<CompletableFuture<Map<Segment, Long>>> futures = new ArrayList<>(streamToStreamCuts.size());
        streamToStreamCuts.entrySet().forEach(e -> {
                  if (e.getValue().equals(StreamCut.UNBOUNDED)) {
                      futures.add(controller.getSegmentsAtTime(e.getKey(), 0L));
                  } else {
                      futures.add(CompletableFuture.completedFuture(e.getValue().asImpl().getPositions()));
                  }
              });
        return getAndHandleExceptions(allOfWithResults(futures).thenApply(listOfMaps -> {
            return listOfMaps.stream()
                             .flatMap(map -> map.entrySet().stream())
                             .collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue()));
        }), InvalidStreamException::new);
    }

    private Map<Segment, Long> getEndSegmentsForStreams(ReaderGroupConfig config) {

        final List<Map<Segment, Long>> listOfMaps = config.getEndingStreamCuts().entrySet().stream()
                                                          .filter(e -> !e.getValue().equals(StreamCut.UNBOUNDED))
                                                          .map(e -> e.getValue().asImpl().getPositions())
                                                          .collect(Collectors.toList());

        return listOfMaps.stream()
                                                     .flatMap(map -> map.entrySet().stream())
                                                     .collect(Collectors.toMap(Entry::getKey, Entry::getValue));
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

        return Futures.loop(checkpointPending::get, () -> {
            return Futures.delayedTask(() -> {
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
        ReaderGroupState state = synchronizer.getState();
        Map<Segment, Long> map = state.getPositionsForCompletedCheckpoint(checkpointName);
        synchronizer.updateStateUnconditionally(new ClearCheckpoints(checkpointName));
        if (map == null) {
            throw new CheckpointFailedException("Checkpoint was cleared before results could be read.");
        }
        return new CheckpointImpl(checkpointName, map);
    }

    @SuppressWarnings( "deprecation" )
    @Override
    public void resetReadersToCheckpoint(Checkpoint checkpoint) {
        @Cleanup
        StateSynchronizer<ReaderGroupState> synchronizer = createSynchronizer();
        synchronizer.updateState(state -> {
            ReaderGroupConfig config = state.getConfig();
            Map<Segment, Long> positions = new HashMap<>();
            for (StreamCut cut : checkpoint.asImpl().getPositions().values()) {
                positions.putAll(cut.asImpl().getPositions());
            }
            return Collections.singletonList(new ReaderGroupStateInit(config, positions, getEndSegmentsForStreams(config)));
        });
    }

    @Override
    public void resetReaderGroup(ReaderGroupConfig config) {
        @Cleanup
        StateSynchronizer<ReaderGroupState> synchronizer = createSynchronizer();
        Map<Segment, Long> segments = getSegmentsForStreams(config);
        synchronizer.updateStateUnconditionally(new ReaderGroupStateInit(config, segments, getEndSegmentsForStreams(config)));
    }

    @Override
    public ReaderGroupMetrics getMetrics() {
        return this;
    }

    @Override
    public long unreadBytes() {
        @Cleanup
        StateSynchronizer<ReaderGroupState> synchronizer = createSynchronizer();
        synchronizer.fetchUpdates();

        Optional<Map<Stream, Map<Segment, Long>>> checkPointedPositions =
                synchronizer.getState().getPositionsForLastCompletedCheckpoint();
        SegmentMetadataClientFactory metaFactory = new SegmentMetadataClientFactoryImpl(controller, connectionFactory);
        if (checkPointedPositions.isPresent()) {
            log.debug("Computing unread bytes based on the last checkPoint position");
            return getUnreadBytes(checkPointedPositions.get(), metaFactory);
        } else {
            log.info("No checkpoints found, using the last known offset to compute unread bytes");
            return getUnreadBytes(synchronizer.getState().getPositions(), metaFactory);
        }
    }

    private long getUnreadBytes(Map<Stream, Map<Segment, Long>> positions, SegmentMetadataClientFactory metaFactory) {
        log.debug("Compute unread bytes from position {}", positions);
        long totalLength = 0;
        for (Entry<Stream, Map<Segment, Long>> streamPosition : positions.entrySet()) {
            StreamCut position = new StreamCutImpl(streamPosition.getKey(), streamPosition.getValue());
            totalLength += getRemainingBytes(metaFactory, position);
        }
        return totalLength;
    }

    private long getRemainingBytes(SegmentMetadataClientFactory metaFactory, StreamCut position) {
        long totalLength = 0;
        CompletableFuture<StreamSegmentSuccessors> unread = controller.getSuccessors(position);
        StreamSegmentSuccessors unreadVal = Futures.getAndHandleExceptions(unread, RuntimeException::new);
        for (Segment s : unreadVal.getSegments()) {
            @Cleanup
            SegmentMetadataClient metadataClient = metaFactory.createSegmentMetadataClient(s, unreadVal.getDelegationToken());
            totalLength += metadataClient.fetchCurrentSegmentLength();
        }
        for (long bytesRead : position.asImpl().getPositions().values()) {
            totalLength -= bytesRead;
        }
        log.debug("Remaining bytes after position: {} is {}", position, totalLength);
        return totalLength;
    }

    @Override
    public Observable<SegmentNotification> getSegmentNotifier(ScheduledExecutorService executor) {
        checkNotNull(executor, "executor");
        return this.notifierFactory.getSegmentNotifier(executor);
    }

    @Override
    public Observable<EndOfDataNotification> getEndOfDataNotifier(ScheduledExecutorService executor) {
        checkNotNull(executor, "executor");
        return this.notifierFactory.getEndOfDataNotifier(executor);
    }

    @Override
    public Map<Stream, StreamCut> getStreamCuts() {
        @Cleanup
        StateSynchronizer<ReaderGroupState> synchronizer = createSynchronizer();
        synchronizer.fetchUpdates();
        ReaderGroupState state = synchronizer.getState();
        Map<Stream, Map<Segment, Long>> positions = state.getPositions();
        HashMap<Stream, StreamCut> cuts = new HashMap<>();

        for (Entry<Stream, Map<Segment, Long>> streamPosition : positions.entrySet()) {
            StreamCut position = new StreamCutImpl(streamPosition.getKey(), streamPosition.getValue());
            cuts.put(streamPosition.getKey(), position);
        }

        return cuts;
    }
}
