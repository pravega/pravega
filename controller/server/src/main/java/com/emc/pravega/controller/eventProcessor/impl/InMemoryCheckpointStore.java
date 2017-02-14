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
import com.emc.pravega.stream.Position;
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
    private final Map<String, Map<String, Position>> map;

    InMemoryCheckpointStore() {
        this.map = new HashMap<>();
    }

    @Override
    @Synchronized
    public void setPosition(String process, String readerGroup, String readerId, Position position) {
        String key = getKey(process, readerGroup);
        if (map.containsKey(key)) {
            map.get(key).put(readerId, position);
        } else {
            Map<String, Position> inner = new HashMap<>();
            inner.put(readerId, position);
            map.put(key, inner);
        }
    }

    @Override
    @Synchronized
    public Map<String, Position> getPositions(String process, String readerGroup) {
        return Collections.unmodifiableMap(map.get(getKey(process, readerGroup)));
    }

    @Override
    @Synchronized
    public void addReaderGroup(String process, String readerGroup) {
        String key = getKey(process, readerGroup);
        if (!map.containsKey(key)) {
            map.put(key, new HashMap<>());
        }
    }

    @Override
    @Synchronized
    public void removeReaderGroup(String process, String readerGroup) {
        String key = getKey(process, readerGroup);
        if (map.containsKey(key) && map.get(key).isEmpty()) {
            // Remove the reader group only if it has no active readers.
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
        setPosition(process, readerGroup, readerId, null);
    }

    @Override
    @Synchronized
    public void removeReader(String process, String readerGroup, String readerId) {
        String key = getKey(process, readerGroup);
        if (map.containsKey(key)) {
            map.get(key).remove(readerId);
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
