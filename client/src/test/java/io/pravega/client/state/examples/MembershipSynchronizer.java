/**
 * Copyright Pravega Authors.
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
package io.pravega.client.state.examples;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.AbstractService;
import io.pravega.client.SynchronizerClientFactory;
import io.pravega.client.state.InitialUpdate;
import io.pravega.client.state.Revision;
import io.pravega.client.state.Revisioned;
import io.pravega.client.state.StateSynchronizer;
import io.pravega.client.state.Update;
import io.pravega.client.stream.impl.JavaSerializer;
import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MembershipSynchronizer extends AbstractService {

    /**
     * How frequently to update the segment using a heartbeat.
     */
    private static final int UPDATE_INTERVAL_MILLIS = 1000;

    /**
     * Number of intervals behind before switching to unconditional updates.
     */
    private static final int UNCONDITIONAL_UPDATE_THRESHOLD = 3;

    /**
     * Number of intervals behind before we should stop executing for safety.
     */
    private static final int UNHEALTHY_THRESHOLD = 5;

    /**
     * Number of intervals behind before another host should be considered dead.
     */
    private static final int DEATH_THRESHOLD = 10;

    private final String instanceId = UUID.randomUUID().toString();

    private final AtomicBoolean healthy = new AtomicBoolean();

    private final ScheduledExecutorService executor;

    private final StateSynchronizer<LiveInstances> stateSync;
    private final MembershipListener listener;
    private ScheduledFuture<?> task;

    MembershipSynchronizer(String streamName, SynchronizerClientFactory clientFactory, ScheduledExecutorService executor,
                           MembershipListener listener) {
        Preconditions.checkNotNull(streamName);
        Preconditions.checkNotNull(clientFactory);
        Preconditions.checkNotNull(listener);
        this.executor = executor;
        this.listener = listener;
        stateSync = clientFactory.createStateSynchronizer(streamName,
                                                    new JavaSerializer<HeartbeatUpdate>(),
                                                    new JavaSerializer<LiveInstances>(),
                                                    null);
    }

    @Data
    private static class LiveInstances
            implements Revisioned, Comparable<LiveInstances>, Serializable, InitialUpdate<LiveInstances> {

        private static final long serialVersionUID = 1L;
        private final String scopedStreamName;
        private final Revision revision;
        private final Map<String, Long> liveInstances;
        private final long vectorTime;

        @Override
        public int compareTo(LiveInstances o) {
            return revision.compareTo(o.revision);
        }

        @Override
        public LiveInstances create(String scopedStreamName, Revision revision) {
            return new LiveInstances(scopedStreamName, revision, liveInstances, vectorTime);
        }

        public boolean isHealthy(String instance) {
            long unhealthyThreshold = vectorTime - UNHEALTHY_THRESHOLD * liveInstances.size();
            Long time = liveInstances.get(instance);
            return time == null || time >= unhealthyThreshold;
        }

        /**
         * If a host is behind in it's heartbeating it does not want to become unhealthy as it might
         * need to halt execution. So it can use unconditional writes to updates itself more quickly
         * by avoiding contention.
         */
        public boolean isOverUnconditionalThreshold(String instance) {
            long updateThreshold = vectorTime - UNCONDITIONAL_UPDATE_THRESHOLD * liveInstances.size();
            Long time = liveInstances.get(instance);
            return time == null || time < updateThreshold;
        }

        public List<String> findInstancesThatWillDieBy(long vectorTime) {
            long deathThreshold = vectorTime - DEATH_THRESHOLD * liveInstances.size();
            return liveInstances.entrySet()
                                .stream()
                                .filter(entry -> entry.getValue() < deathThreshold)
                                .map(entry -> entry.getKey())
                                .collect(Collectors.toList());
        }

        public Set<String> getLiveInstances() {
            return liveInstances.keySet();
        }
    }

    private class HeartBeater implements Runnable {
        @Override
        public void run() {
            try {
                stateSync.fetchUpdates();
                notifyListener();
                if (stateSync.getState().isOverUnconditionalThreshold(instanceId)) {
                    stateSync.updateState((state, updates) -> {
                        long vectorTime = state.getVectorTime() + 1;
                        updates.add(new HeartBeat(instanceId, vectorTime));
                        for (String id : state.findInstancesThatWillDieBy(vectorTime)) {
                            if (!id.equals(instanceId)) {
                                updates.add(new DeclareDead(id));
                            }
                        }
                    });
                } else {
                    stateSync.updateStateUnconditionally(new HeartBeat(instanceId, stateSync.getState().vectorTime));
                    stateSync.fetchUpdates();
                }
                notifyListener();
            } catch (Exception e) {
                log.warn("Encountered an error while heartbeating: " + e);
                if (healthy.compareAndSet(true, false)) {
                    listener.unhealthy();
                }
            }
        }
    }

    private abstract class HeartbeatUpdate implements Update<LiveInstances>, Serializable {
        private static final long serialVersionUID = 1L;
    }

    @RequiredArgsConstructor
    private final class HeartBeat extends HeartbeatUpdate {
        private static final long serialVersionUID = 1L;
        private final String name;
        private final long timestamp;

        @Override
        public LiveInstances applyTo(LiveInstances state, Revision newRevision) {
            Map<String, Long> timestamps = new HashMap<>(state.liveInstances);
            long vectorTime = Long.max(timestamps.values().stream().max(Long::compare).get(), timestamp);
            timestamps.put(name, timestamp);
            return new LiveInstances(state.scopedStreamName,
                    newRevision,
                    Collections.unmodifiableMap(timestamps),
                    vectorTime);
        }
    }

    @RequiredArgsConstructor
    private final class DeclareDead extends HeartbeatUpdate {
        private static final long serialVersionUID = 1L;
        private final String name;

        @Override
        public LiveInstances applyTo(LiveInstances state, Revision newRevision) {
            Map<String, Long> timestamps = new HashMap<>(state.liveInstances);
            timestamps.remove(name);
            return new LiveInstances(state.scopedStreamName,
                    newRevision,
                    Collections.unmodifiableMap(timestamps),
                    state.vectorTime);
        }
    }

    public void notifyListener() {
        LiveInstances currentState = stateSync.getState();
        if (currentState.isHealthy(instanceId)) {
            if (healthy.compareAndSet(false, true)) {
                listener.healthy();
            }
        } else {
            if (healthy.compareAndSet(true, false)) {
                listener.unhealthy();
            }
        }
    }

    public boolean isCurrentlyHealthy() {
        return healthy.get();
    }

    public Set<String> getCurrentMembers() {
        return stateSync.getState().getLiveInstances();
    }

    public interface MembershipListener {
        void healthy();

        void unhealthy();
    }

    @Override
    protected void doStart() {
        task = executor.scheduleAtFixedRate(new HeartBeater(),
                                            UPDATE_INTERVAL_MILLIS,
                                            UPDATE_INTERVAL_MILLIS,
                                            TimeUnit.MILLISECONDS);
    }

    @Override
    protected void doStop() {
        task.cancel(false);
    }

}
