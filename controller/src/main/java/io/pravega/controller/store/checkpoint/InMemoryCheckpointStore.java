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
package io.pravega.controller.store.checkpoint;

import io.pravega.client.stream.Position;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Synchronized;

import javax.annotation.concurrent.GuardedBy;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * In memory checkpoint store.
 */
class InMemoryCheckpointStore implements CheckpointStore {

    private final static String SEPARATOR = ":::";

    @GuardedBy("$lock")
    private final Map<String, ReaderGroupData> map;

    InMemoryCheckpointStore() {
        this.map = new HashMap<>();
    }

    @Data
    @AllArgsConstructor
    private static class ReaderGroupData {
        enum State {
            Active,
            Sealed,
        }

        private State state;
        private Map<String, Position> map;

        void update(String readerId, Position position) throws CheckpointStoreException {
            if (this.map.containsKey(readerId)) {
                this.map.put(readerId, position);
            } else {
                throw new CheckpointStoreException(CheckpointStoreException.Type.NoNode, "Reader does not exist");
            }
        }
    }

    @Override
    @Synchronized
    public void setPosition(final String process, final String readerGroup, final String readerId,
                            final Position position) throws CheckpointStoreException {
        String key = getKey(process, readerGroup);
        if (map.containsKey(key)) {
            map.get(key).update(readerId, position);
        } else {
            throw new CheckpointStoreException(CheckpointStoreException.Type.NoNode, "ReaderGroup does not exist");
        }
    }

    @Override
    @Synchronized
    public Map<String, Position> getPositions(final String process, final String readerGroup) {
        return Collections.unmodifiableMap(map.get(getKey(process, readerGroup)).getMap());
    }

    @Override
    @Synchronized
    public void addReaderGroup(final String process, final String readerGroup) throws CheckpointStoreException {
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
    public Map<String, Position> sealReaderGroup(final String process,
                                                 final String readerGroup) throws CheckpointStoreException {
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
    public void removeReaderGroup(final String process, final String readerGroup) throws CheckpointStoreException {
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
    public List<String> getReaderGroups(final String process) {
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
    public void addReader(final String process, final String readerGroup,
                          final String readerId) throws CheckpointStoreException {
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
    public void removeReader(final String process, final String readerGroup, final String readerId) {
        String key = getKey(process, readerGroup);
        if (map.containsKey(key)) {
            ReaderGroupData groupData = map.get(key);
            groupData.getMap().remove(readerId);
        }
    }

    @Override
    public Set<String> getProcesses() throws CheckpointStoreException {
        return map.keySet().stream().map(this::getProcess).collect(Collectors.toSet());
    }

    /**
     * Get the health status.
     *
     * @return true by deafult.
     */
    @Override
    public boolean isHealthy() {
        return true;
    }

    private String getKey(final String process, final String readerGroup) {
        return process + SEPARATOR + readerGroup;
    }

    private String getMatchingReaderGroup(final String key, final String process) {
        String[] splits = key.split(SEPARATOR);
        if (process.equals(splits[0])) {
            return splits[1];
        } else {
            return null;
        }
    }

    private String getProcess(final String key) {
        String[] splits = key.split(SEPARATOR);
        return splits[0];
    }
}
