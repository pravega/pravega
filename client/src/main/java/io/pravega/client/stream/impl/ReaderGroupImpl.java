/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.pravega.client.stream.impl;

import com.google.common.annotations.VisibleForTesting;
import io.pravega.client.ClientFactory;
import io.pravega.client.segment.impl.Segment;
import io.pravega.client.state.StateSynchronizer;
import io.pravega.client.state.SynchronizerConfig;
import io.pravega.client.stream.Checkpoint;
import io.pravega.client.stream.InvalidStreamException;
import io.pravega.client.stream.Position;
import io.pravega.client.stream.ReaderGroup;
import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.client.stream.Serializer;
import io.pravega.client.stream.impl.ReaderGroupState.ClearCheckpoints;
import io.pravega.client.stream.impl.ReaderGroupState.CreateCheckpoint;
import io.pravega.client.stream.impl.ReaderGroupState.ReaderGroupStateInit;
import io.pravega.client.stream.impl.ReaderGroupState.ReaderGroupStateUpdate;
import io.pravega.common.concurrent.FutureHelpers;
import io.pravega.shared.NameUtils;
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
import lombok.extern.slf4j.Slf4j;

import static io.pravega.common.concurrent.FutureHelpers.allOfWithResults;
import static io.pravega.common.concurrent.FutureHelpers.getAndHandleExceptions;

@Slf4j
@Data
public class ReaderGroupImpl implements ReaderGroup {

    private final String scope;
    private final String groupName;
    private final SynchronizerConfig synchronizerConfig;
    private final Serializer<ReaderGroupStateInit> initSerializer;
    private final Serializer<ReaderGroupStateUpdate> updateSerializer;
    private final ClientFactory clientFactory;
    private final Controller controller;

    /**
     * Called by the StreamManager to provide the streams the group should start reading from.
     * @param  config The configuration for the reader group.
     * @param streams The segments to use and where to start from.
     */
    @VisibleForTesting
    public void initializeGroup(ReaderGroupConfig config, Set<String> streams) {
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
        StateSynchronizer<ReaderGroupState> synchronizer = createSynchronizer();
        ReaderGroupStateManager.readerShutdown(readerId, lastPosition, synchronizer);
    }

    private StateSynchronizer<ReaderGroupState> createSynchronizer() {
        return clientFactory.createStateSynchronizer(NameUtils.getStreamForReaderGroup(groupName),
                updateSerializer, initSerializer, synchronizerConfig);
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
                if (checkpointPending.get()) {
                    log.debug("Waiting on checkpoint: {} currentState is: {}", checkpointName, synchronizer.getState());                    
                }
                return null;
            }, Duration.ofMillis(500), backgroundExecutor);
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
