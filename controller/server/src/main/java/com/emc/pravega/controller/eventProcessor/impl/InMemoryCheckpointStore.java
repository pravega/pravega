/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.emc.pravega.controller.eventProcessor.impl;

import com.emc.pravega.controller.eventProcessor.CheckpointStore;
import com.emc.pravega.controller.eventProcessor.CheckpointStoreException;
import com.emc.pravega.stream.Position;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Synchronized;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * In memory checkpoint store.
 */
class InMemoryCheckpointStore implements CheckpointStore {

    private final static String SEPARATOR = ":::";
    private final Map<String, ReaderGroupData> map;

    InMemoryCheckpointStore() {
        this.map = new HashMap<>();
    }

    @Data
    @AllArgsConstructor
    static class ReaderGroupData {
        enum State {
            Active,
            Sealed,
        }

        private State state;
        private Map<String, Position> map;

        void update(String readerId, Position position) {
            if (this.map.containsKey(readerId)) {
                this.map.put(readerId, position);
            } else {
                throw new CheckpointStoreException(CheckpointStoreException.Type.NoNode, "Reader does not exist");
            }
        }
    }

    @Override
    @Synchronized
    public void setPosition(String process, String readerGroup, String readerId, Position position) {
        String key = getKey(process, readerGroup);
        if (map.containsKey(key)) {
            map.get(key).update(readerId, position);
        } else {
            throw new CheckpointStoreException(CheckpointStoreException.Type.NoNode, "ReaderGroup does not exist");
        }
    }

    @Override
    @Synchronized
    public Map<String, Position> getPositions(String process, String readerGroup) {
        return Collections.unmodifiableMap(map.get(getKey(process, readerGroup)).getMap());
    }

    @Override
    @Synchronized
    public void addReaderGroup(String process, String readerGroup) {
        String key = getKey(process, readerGroup);
        if (!map.containsKey(key)) {
            ReaderGroupData groupData = new ReaderGroupData(ReaderGroupData.State.Active, new HashMap<>());
            map.put(key, groupData);
        } else {
            throw new CheckpointStoreException(CheckpointStoreException.Type.NodeExists, "ReaderGroup exists");
        }
    }

    @Override
    @Synchronized
    public Map<String, Position> sealReaderGroup(String process, String readerGroup) {
        String key = getKey(process, readerGroup);
        if (map.containsKey(key)) {
            ReaderGroupData groupData = map.get(key);
            groupData.setState(ReaderGroupData.State.Sealed);
            map.put(key, groupData);
            return Collections.unmodifiableMap(groupData.getMap());
        } else {
            throw new CheckpointStoreException(CheckpointStoreException.Type.NoNode, "ReaderGroup does not exist");
        }
    }

    @Override
    @Synchronized
    public void removeReaderGroup(String process, String readerGroup) {
        String key = getKey(process, readerGroup);
        if (map.containsKey(key)) {
            // Remove the reader group only if it has no active readers.
            ReaderGroupData groupData = map.get(key);
            if (groupData.getState() == ReaderGroupData.State.Active) {
                throw new CheckpointStoreException(CheckpointStoreException.Type.Active, "ReaderGroup is active.");
            }
            if (!groupData.getMap().isEmpty()) {
                throw new CheckpointStoreException(CheckpointStoreException.Type.NodeNotEmpty, "ReaderGroup is not empty.");
            }
            map.remove(key);
        }
    }

    @Override
    @Synchronized
    public List<String> getReaderGroups(String process) {
        List<String> list = new ArrayList<>();
        map.entrySet().stream().forEach(pair -> {
            String readerGroup = getMatchingReaderGroup(pair.getKey(), process);
            if (readerGroup != null) {
                list.add(readerGroup);
            }
        });
        return list;
    }

    @Override
    @Synchronized
    public void addReader(String process, String readerGroup, String readerId) {
        String key = getKey(process, readerGroup);
        if (map.containsKey(key)) {
            ReaderGroupData groupData = map.get(key);
            if (groupData.getState() == ReaderGroupData.State.Sealed) {
                throw new CheckpointStoreException(CheckpointStoreException.Type.Sealed, "ReaderGroup is sealed");
            }
            if (groupData.getMap().containsKey(readerId)) {
                throw new CheckpointStoreException(CheckpointStoreException.Type.NodeExists, "Duplicate readerId");
            }
            groupData.getMap().put(readerId, null);
        }

        setPosition(process, readerGroup, readerId, null);
    }

    @Override
    @Synchronized
    public void removeReader(String process, String readerGroup, String readerId) {
        String key = getKey(process, readerGroup);
        if (map.containsKey(key)) {
            ReaderGroupData groupData = map.get(key);
            groupData.getMap().remove(readerId);
        }
    }

    private String getKey(String process, String readerGroup) {
        return process + SEPARATOR + readerGroup;
    }

    private String getMatchingReaderGroup(String key, String process) {
        String[] splits = key.split(SEPARATOR);
        if (process.equals(splits[0])) {
            return splits[1];
        } else {
            return null;
        }
    }
}
