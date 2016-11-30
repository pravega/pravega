/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.emc.pravega.state.examples;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.stream.Collectors;

import com.emc.pravega.common.Exceptions;
import com.emc.pravega.common.concurrent.NewestReference;
import com.emc.pravega.state.InitialUpdate;
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
import net.bytebuddy.implementation.bytecode.member.HandleInvocation;

@Slf4j
public class HeartBeatSynchronizer extends AbstractService {

    private static final int MIN_HEARTBEAT_INTERVAL_MILLIS = 1000;
    private static final int MAX_HEARTBEAT_INTERVAL_MILLIS = 4000;
    private static final int DEATH_THREASHOLD_MILLIS = 12000;

    private final String instanceId = UUID.randomUUID().toString();
    private final Random rand = new Random(instanceId.hashCode());

    private final Thread heartbeatThread = new Thread(new HeartBeater());
    private final Thread updateThread = new Thread(new Updater());

    private final NewestReference<LiveInstances> liveInstances = new NewestReference<>();
    private final Synchronizer<LiveInstances, HeartbeatUpdate, LiveInstances> sync;

    HeartBeatSynchronizer(Stream stream) {
        sync = stream.createSynchronizer(new JavaSerializer<HeartbeatUpdate>(),
                                         new JavaSerializer<LiveInstances>(),
                                         null);
    }

    @Data
    private static class LiveInstances implements Revisioned, Comparable<LiveInstances>, Serializable, InitialUpdate<LiveInstances> {
        private final String qualifiedStreamName;
        private final Revision revision;
        private final Map<String, Long> liveInstances;

        @Override
        public int compareTo(LiveInstances o) {
            return revision.compareTo(o.revision);
        }

        @Override
        public LiveInstances create(Revision revision) {
            return new LiveInstances(qualifiedStreamName, revision, liveInstances);
        }
    }

    private class HeartBeater implements Runnable {
        @Override
        public void run() {
            while (true) {
                int delay = MIN_HEARTBEAT_INTERVAL_MILLIS + rand.nextInt(MAX_HEARTBEAT_INTERVAL_MILLIS - MIN_HEARTBEAT_INTERVAL_MILLIS);
                try {
                    Thread.sleep(delay);
                } catch (InterruptedException e) {
                    log.warn("Exiting due to interupt", e);
                    break;
                }
                long currentTime = System.currentTimeMillis();
                LiveInstances state = liveInstances.get();
                HeartBeat h = new HeartBeat(instanceId, currentTime);
                state = sync.unconditionallyUpdateState(state, h);
                liveInstances.update(state);
            }
        }
    }

    private List<DeclareDead> findDeadInstances(LiveInstances state, long currentTime) {
        long threashold = currentTime - DEATH_THREASHOLD_MILLIS;
        return state.liveInstances.entrySet()
            .stream()
            .filter(entry -> entry.getValue() < threashold)
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
                    sync.conditionallyUpdateState(current, dead);
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
        Exceptions.handleInterrupted(() -> {
            heartbeatThread.join();
            updateThread.join();
        });
    }

}
