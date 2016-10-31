package com.emc.pravega.state.examples;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.stream.Collectors;

import com.emc.pravega.common.concurrent.NewestRefrence;
import com.emc.pravega.state.Revision;
import com.emc.pravega.state.Revisioned;
import com.emc.pravega.state.Synchronizer;
import com.emc.pravega.state.Update;
import com.emc.pravega.stream.Stream;
import com.emc.pravega.stream.impl.JavaSerializer;
import com.google.common.util.concurrent.AbstractService;

import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class HeartBeatSynchronizer extends AbstractService {

    private static final int MIN_HEARTBEAT_INTERVAL = 1000;
    private static final int MAX_HEARTBEAT_INTERVAL = 4000;
    private static final int DEATH_THREASHOLD = 12000;

    private final String instanceId = UUID.randomUUID().toString();
    private final Random rand = new Random(instanceId.hashCode());

    private final Thread heartbeatThread = new Thread(new HeartBeater());
    private final Thread updateThread = new Thread(new Updater());

    private final NewestRefrence<LiveInstances> liveInstances = new NewestRefrence<>();
    private final Synchronizer<LiveInstances, HeartbeatUpdate> sync;

    HeartBeatSynchronizer(Stream stream) {
        sync = stream.createSynchronizer(new JavaSerializer<LiveInstances>(),
                                         new JavaSerializer<HeartbeatUpdate>(),
                                         null);
    }

    @Data
    private static class LiveInstances implements Revisioned, Comparable<LiveInstances>, Serializable {
        private final String qualifiedStreamName;
        private final Revision revision;
        private final Map<String, Long> liveInstances;

        @Override
        public int compareTo(LiveInstances o) {
            return revision.compareTo(o.revision);
        }
    }

    private class HeartBeater implements Runnable {
        @Override
        public void run() {
            while (true) {
                int delay = MIN_HEARTBEAT_INTERVAL + rand.nextInt(MAX_HEARTBEAT_INTERVAL - MIN_HEARTBEAT_INTERVAL);
                try {
                    Thread.sleep(delay);
                } catch (InterruptedException e) {
                    log.warn("Exiting due to interupt", e);
                    break;
                }
                long currentTime = System.currentTimeMillis();
                LiveInstances state = liveInstances.get();
                HeartBeat h = new HeartBeat(instanceId, currentTime);
                state = sync.updateState(state, h, false);
                liveInstances.update(state);
            }
        }
    }

    private List<DeclareDead> findDeadInstances(LiveInstances state, long currentTime) {
        long threashold = currentTime - DEATH_THREASHOLD;
        return state.liveInstances.entrySet()
            .stream()
            .filter(entry -> (entry.getValue() < threashold))
            .map(entry -> new DeclareDead(entry.getKey()))
            .collect(Collectors.toList());
    }

    private class Updater implements Runnable {
        @Override
        public void run() {
            while (true) {
                LiveInstances current = sync.getLatestState(liveInstances.get()); // blocking
                long currentTime = System.currentTimeMillis();
                liveInstances.update(current);
                List<DeclareDead> dead = findDeadInstances(current, currentTime);
                if (!dead.isEmpty()) {
                    sync.updateState(current, dead, true);
                }
            }
        }
    }

    private abstract class HeartbeatUpdate implements Update<LiveInstances>, Serializable {
    }

    @RequiredArgsConstructor
    private final class HeartBeat extends HeartbeatUpdate {
        private final String name;
        private final long timestamp;

        @Override
        public LiveInstances applyTo(LiveInstances state, Revision newRevision) {
            Map<String, Long> timestamps = new HashMap<>(state.getLiveInstances());
            timestamps.put(name, timestamp);
            return new LiveInstances(state.qualifiedStreamName, newRevision, Collections.unmodifiableMap(timestamps));
        }
    }

    @RequiredArgsConstructor
    private final class DeclareDead extends HeartbeatUpdate {
        private final String name;

        @Override
        public LiveInstances applyTo(LiveInstances state, Revision newRevision) {
            Map<String, Long> timestamps = new HashMap<>(state.getLiveInstances());
            timestamps.remove(name);
            return new LiveInstances(state.qualifiedStreamName, newRevision, Collections.unmodifiableMap(timestamps));
        }
    }

    @Override
    protected void doStart() {
        liveInstances.update(sync.getLatestState());
        heartbeatThread.start();
        updateThread.start();
    }

    @Override
    protected void doStop() {
        heartbeatThread.interrupt();
        updateThread.interrupt();
        try {
            heartbeatThread.join();
            updateThread.join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

}
