/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.stream.impl;

import com.emc.pravega.ClientFactory;
import com.emc.pravega.common.concurrent.FutureHelpers;
import com.emc.pravega.state.StateSynchronizer;
import com.emc.pravega.state.SynchronizerConfig;
import com.emc.pravega.stream.Checkpoint;
import com.emc.pravega.stream.InvalidStreamException;
import com.emc.pravega.stream.Position;
import com.emc.pravega.stream.ReaderGroup;
import com.emc.pravega.stream.ReaderGroupConfig;
import com.emc.pravega.stream.Segment;
import com.emc.pravega.stream.Serializer;
import com.emc.pravega.stream.impl.ReaderGroupState.ClearCheckpoints;
import com.emc.pravega.stream.impl.ReaderGroupState.CreateCheckpoint;
import com.emc.pravega.stream.impl.ReaderGroupState.ReaderGroupStateInit;
import com.emc.pravega.stream.impl.ReaderGroupState.ReaderGroupStateUpdate;
import com.google.common.annotations.VisibleForTesting;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import lombok.Data;
import lombok.SneakyThrows;

import static com.emc.pravega.common.concurrent.FutureHelpers.allOfWithResults;
import static com.emc.pravega.common.concurrent.FutureHelpers.getAndHandleExceptions;

@Data
public class ReaderGroupImpl implements ReaderGroup {

    private final String scope;
    private final String groupName;
    private final Set<String> streamNames;
    private final SynchronizerConfig synchronizerConfig;
    private final Serializer<ReaderGroupStateInit> initSerializer;
    private final Serializer<ReaderGroupStateUpdate> updateSerializer;
    private final ClientFactory clientFactory;
    private final Controller controller;

    /**
     * Called by the StreamManager to provide the streams the group should start reading from.
     * @param  config The configuration for the reader group.
     * @param streams The streams to use.
     */
    @VisibleForTesting
    public void initializeGroup(ReaderGroupConfig config, Set<String> streams) {
        Map<Segment, Long> segments = getSegmentsForStreams(streams);
        StateSynchronizer<ReaderGroupState> synchronizer = createSynchronizer();
        ReaderGroupStateManager.initializeReaderGroup(synchronizer, config, segments);
    }

    private Map<Segment, Long> getSegmentsForStreams(Set<String> streams) {
        List<CompletableFuture<Map<Segment, Long>>> futures = new ArrayList<>(streams.size());
        for (String stream : streams) {
            CompletableFuture<List<PositionInternal>> future = controller.getPositions(new StreamImpl(scope, stream), 0, 1);
            futures.add(future.thenApply(list -> list.stream()
                                                     .flatMap(pos -> pos.getOwnedSegmentsWithOffsets().entrySet().stream())
                                                     .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue))));
        }
        return getAndHandleExceptions(allOfWithResults(futures).thenApply(listOfMaps -> {
            return listOfMaps.stream()
                             .flatMap(map -> map.entrySet().stream())
                             .collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue()));
        }), InvalidStreamException::new);
    }

    @Override
    public void readerOffline(String readerId, Position lastPosition) {
        StateSynchronizer<ReaderGroupState> synchronizer = createSynchronizer();
        ReaderGroupStateManager.readerShutdown(readerId, lastPosition.asImpl(), synchronizer);
    }

    private StateSynchronizer<ReaderGroupState> createSynchronizer() {
        return clientFactory.createStateSynchronizer(groupName, updateSerializer, initSerializer, synchronizerConfig);
    }

    @Override
    public Set<String> getOnlineReaders() {
        StateSynchronizer<ReaderGroupState> synchronizer = createSynchronizer();
        synchronizer.fetchUpdates();
        return synchronizer.getState().getOnlineReaders();
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
                return null;
            }, Duration.ofSeconds(2), backgroundExecutor);
        }, backgroundExecutor).thenApply(v ->  completeCheckpoint(checkpointName, synchronizer)
        );
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
        StateSynchronizer<ReaderGroupState> synchronizer = createSynchronizer();
        synchronizer.updateState(state -> {
            ReaderGroupConfig config = state.getConfig();
            return Collections.singletonList(new ReaderGroupStateInit(config, checkpoint.asImpl().getPositions()));
        });
    }

    @Override
    public void alterConfig(ReaderGroupConfig config, Set<String> streamNames) {
        StateSynchronizer<ReaderGroupState> synchronizer = createSynchronizer();
        Map<Segment, Long> segments = getSegmentsForStreams(streamNames);
        synchronizer.updateStateUnconditionally(new ReaderGroupStateInit(config, segments));
    }

}
