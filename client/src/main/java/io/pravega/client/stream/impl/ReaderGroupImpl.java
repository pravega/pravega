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

import com.google.common.base.Preconditions;
import io.pravega.client.ClientFactory;
import io.pravega.client.netty.impl.ConnectionFactory;
import io.pravega.client.segment.impl.Segment;
import io.pravega.client.segment.impl.SegmentMetadataClient;
import io.pravega.client.segment.impl.SegmentMetadataClientFactory;
import io.pravega.client.segment.impl.SegmentMetadataClientFactoryImpl;
import io.pravega.client.state.InitialUpdate;
import io.pravega.client.state.StateSynchronizer;
import io.pravega.client.state.SynchronizerConfig;
import io.pravega.client.state.Update;
import io.pravega.client.stream.Checkpoint;
import io.pravega.client.stream.InvalidStreamException;
import io.pravega.client.stream.Position;
import io.pravega.client.stream.ReaderGroup;
import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.client.stream.ReaderGroupMetrics;
import io.pravega.client.stream.Serializer;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamCut;
import io.pravega.client.stream.impl.ReaderGroupState.ClearCheckpointsBefore;
import io.pravega.client.stream.impl.ReaderGroupState.CreateCheckpoint;
import io.pravega.client.stream.impl.ReaderGroupState.ReaderGroupStateInit;
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
    private final Controller controller;
    private final SegmentMetadataClientFactory metaFactory;
    private final StateSynchronizer<ReaderGroupState> synchronizer;
    private final NotifierFactory notifierFactory;

    public ReaderGroupImpl(String scope, String groupName, SynchronizerConfig synchronizerConfig,
                           Serializer<InitialUpdate<ReaderGroupState>> initSerializer, Serializer<Update<ReaderGroupState>> updateSerializer,
                           ClientFactory clientFactory, Controller controller, ConnectionFactory connectionFactory) {
        Preconditions.checkNotNull(synchronizerConfig);
        Preconditions.checkNotNull(initSerializer);
        Preconditions.checkNotNull(updateSerializer);
        Preconditions.checkNotNull(clientFactory);
        Preconditions.checkNotNull(connectionFactory);
        this.scope = Preconditions.checkNotNull(scope);
        this.groupName = Preconditions.checkNotNull(groupName);
        this.controller = Preconditions.checkNotNull(controller);
        this.metaFactory = new SegmentMetadataClientFactoryImpl(controller, connectionFactory);
        this.synchronizer = clientFactory.createStateSynchronizer(NameUtils.getStreamForReaderGroup(groupName),
                                                                  updateSerializer, initSerializer, synchronizerConfig);
        this.notifierFactory = new NotifierFactory(new NotificationSystem(), synchronizer);
    }

    @Override
    public void readerOffline(String readerId, Position lastPosition) {
        ReaderGroupStateManager.readerShutdown(readerId, lastPosition, synchronizer);
    }

    @Override
    public Set<String> getOnlineReaders() {
        synchronizer.fetchUpdates();
        return synchronizer.getState().getOnlineReaders();
    }

    @Override
    public Set<String> getStreamNames() {
        synchronizer.fetchUpdates();
        return synchronizer.getState().getStreamNames();
    }

    @Override
    public CompletableFuture<Checkpoint> initiateCheckpoint(String checkpointName, ScheduledExecutorService backgroundExecutor) {

        synchronizer.fetchUpdates();
        int maxOutstandingCheckpointRequest = this.synchronizer.getState().getConfig().getMaxOutstandingCheckpointRequest();
        int currentOutstandingCheckpointRequest = synchronizer.getState().getCheckpointState().getUnCheckpointedHostsSize();
        if (currentOutstandingCheckpointRequest == maxOutstandingCheckpointRequest) {
            String errorMessage = "rejecting checkpoint request since pending checkpoint reaches max allowed limit: " + maxOutstandingCheckpointRequest;
            log.warn("maxOutstandingCheckpointRequest: {}, currentOutstandingCheckpointRequest: {}, errorMessage: {}",
                    maxOutstandingCheckpointRequest, currentOutstandingCheckpointRequest, errorMessage);
            return Futures.failedFuture(new CheckpointFailedException(errorMessage));
        }

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
                      .thenApply(v -> completeCheckpoint(checkpointName));
    }

    @SneakyThrows(CheckpointFailedException.class)
    private Checkpoint completeCheckpoint(String checkpointName) {
        ReaderGroupState state = synchronizer.getState();
        Map<Segment, Long> map = state.getPositionsForCompletedCheckpoint(checkpointName);
        synchronizer.updateStateUnconditionally(new ClearCheckpointsBefore(checkpointName));
        if (map == null) {
            throw new CheckpointFailedException("Checkpoint was cleared before results could be read.");
        }
        return new CheckpointImpl(checkpointName, map);
    }

    /**
     * Used to reset a reset a reader group to a checkpoint. This should be removed in time.
     * @deprecated Use {@link ReaderGroup#resetReaderGroup(ReaderGroupConfig)} to reset readers to a given Checkpoint.
     */
    @Override
    @Deprecated
    public void resetReadersToCheckpoint(Checkpoint checkpoint) {
        synchronizer.updateState((state, updates) -> {
            ReaderGroupConfig config = state.getConfig();
            Map<Segment, Long> positions = new HashMap<>();
            for (StreamCut cut : checkpoint.asImpl().getPositions().values()) {
                positions.putAll(cut.asImpl().getPositions());
            }
            updates.add(new ReaderGroupStateInit(config, positions, getEndSegmentsForStreams(config)));
        });
    }

    @Override
    public void resetReaderGroup(ReaderGroupConfig config) {
        Map<Segment, Long> segments = getSegmentsForStreams(controller, config);
        synchronizer.updateStateUnconditionally(new ReaderGroupStateInit(config, segments, getEndSegmentsForStreams(config)));
    }

    public static Map<Segment, Long> getSegmentsForStreams(Controller controller, ReaderGroupConfig config) {
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

    public static Map<Segment, Long> getEndSegmentsForStreams(ReaderGroupConfig config) {

        final List<Map<Segment, Long>> listOfMaps = config.getEndingStreamCuts().entrySet().stream()
                                                          .filter(e -> !e.getValue().equals(StreamCut.UNBOUNDED))
                                                          .map(e -> e.getValue().asImpl().getPositions())
                                                          .collect(Collectors.toList());

        return listOfMaps.stream()
                         .flatMap(map -> map.entrySet().stream())
                         .collect(Collectors.toMap(Entry::getKey, Entry::getValue));
    }

    @Override
    public ReaderGroupMetrics getMetrics() {
        return this;
    }

    @Override
    public long unreadBytes() {
        synchronizer.fetchUpdates();

        Optional<Map<Stream, Map<Segment, Long>>> checkPointedPositions =
                synchronizer.getState().getPositionsForLastCompletedCheckpoint();

        if (checkPointedPositions.isPresent()) {
            log.debug("Computing unread bytes based on the last checkPoint position");
            return getUnreadBytes(checkPointedPositions.get(), synchronizer.getState().getEndSegments(), metaFactory);
        } else {
            log.info("No checkpoints found, using the last known offset to compute unread bytes");
            return getUnreadBytes(synchronizer.getState().getPositions(), synchronizer.getState().getEndSegments(), metaFactory);
        }
    }

    private long getUnreadBytes(Map<Stream, Map<Segment, Long>> positions, Map<Segment, Long> endSegments, SegmentMetadataClientFactory metaFactory) {
        log.debug("Compute unread bytes from position {}", positions);
        long totalLength = 0;
        for (Entry<Stream, Map<Segment, Long>> streamPosition : positions.entrySet()) {
            StreamCut fromStreamCut = new StreamCutImpl(streamPosition.getKey(), streamPosition.getValue());
            StreamCut toStreamCut = computeEndStreamCut(streamPosition.getKey(), endSegments);
            totalLength += getRemainingBytes(metaFactory, fromStreamCut, toStreamCut);
        }
        return totalLength;
    }

    private StreamCut computeEndStreamCut(Stream stream, Map<Segment, Long> endSegments) {
        final Map<Segment, Long> toPositions = endSegments.entrySet().stream()
                                                          .filter(e -> e.getKey().getStream().equals(stream))
                                                          .collect(Collectors.toMap(Entry::getKey, Entry::getValue));
        return toPositions.isEmpty() ? StreamCut.UNBOUNDED : new StreamCutImpl(stream, toPositions);
    }

    private long getRemainingBytes(SegmentMetadataClientFactory metaFactory, StreamCut fromStreamCut, StreamCut toStreamCut) {
        long totalLength = 0;

        //fetch StreamSegmentSuccessors
        final CompletableFuture<StreamSegmentSuccessors> unread;
        final Map<Segment, Long> endPositions;
        if (toStreamCut.equals(StreamCut.UNBOUNDED)) {
            unread = controller.getSuccessors(fromStreamCut);
            endPositions = Collections.emptyMap();
        } else {
            unread = controller.getSegments(fromStreamCut, toStreamCut);
            endPositions = toStreamCut.asImpl().getPositions();
        }
        StreamSegmentSuccessors unreadVal = Futures.getAndHandleExceptions(unread, RuntimeException::new);
        //compute remaining bytes.
        for (Segment s : unreadVal.getSegments()) {
            if (endPositions.containsKey(s)) {
                totalLength += endPositions.get(s);
            } else {
                @Cleanup
                SegmentMetadataClient metadataClient = metaFactory.createSegmentMetadataClient(s, unreadVal.getDelegationToken());
                totalLength += metadataClient.fetchCurrentSegmentLength();
            }
        }
        for (long bytesRead : fromStreamCut.asImpl().getPositions().values()) {
            totalLength -= bytesRead;
        }
        log.debug("Remaining bytes from position: {} to position: {} is {}", fromStreamCut, toStreamCut, totalLength);
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

    @Override
    public void close() {
        synchronizer.close();
    }
}
