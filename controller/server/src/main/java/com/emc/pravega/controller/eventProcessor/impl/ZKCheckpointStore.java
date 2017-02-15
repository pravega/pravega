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

import com.emc.pravega.common.util.Retry;
import com.emc.pravega.controller.eventProcessor.CheckpointStore;
import com.emc.pravega.controller.eventProcessor.CheckpointStoreException;
import com.emc.pravega.stream.Position;
import com.emc.pravega.stream.impl.JavaSerializer;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Zookeeper based checkpoint store.
 */
@Slf4j
class ZKCheckpointStore implements CheckpointStore {

    private static long initialMillis = 100L;
    private static int multiplier = 2;
    private static int attempts = 10;
    private static long maxDelay = 2000;

    private final CuratorFramework client;
    private final JavaSerializer<Position> positionSerializer;
    private final JavaSerializer<ReaderGroupData> groupDataSerializer;

    ZKCheckpointStore(CuratorFramework client) {
        this.client = client;
        this.positionSerializer = new JavaSerializer<>();
        this.groupDataSerializer = new JavaSerializer<>();
    }

    @Data
    @AllArgsConstructor
    static class ReaderGroupData implements Serializable {
        enum State {
            Active,
            Sealed,
        }

        private State state;
        private List<String> readerIds;
    }

    @Override
    public void setPosition(String process, String readerGroup, String readerId, Position position) {
        updateNode(getReaderPath(process, readerGroup, readerId), positionSerializer.serialize(position).array());
    }

    @Override
    public Map<String, Position> getPositions(String process, String readerGroup) {
        Map<String, Position> map = new HashMap<>();
        String path = getReaderGroupPath(process, readerGroup);
        for (String child : getChildren(path)) {
            Position position = null;
            byte[] data = getData(path + "/" + child);
            if (data != null && data.length > 0) {
                position = positionSerializer.deserialize(ByteBuffer.wrap(data));
            }
            map.put(child, position);
        }
        return map;
    }

    @Override
    public void addReaderGroup(String process, String readerGroup) {
        ReaderGroupData data = new ReaderGroupData(ReaderGroupData.State.Active, new ArrayList<>());
        addNode(getReaderGroupPath(process, readerGroup), groupDataSerializer.serialize(data).array());
    }

    @Override
    public Map<String, Position> sealReaderGroup(String process, String readerGroup) {
        String path = getReaderGroupPath(process, readerGroup);
        Stat stat = new Stat();

        try {
            return Retry.withExpBackoff(initialMillis, multiplier, attempts, maxDelay)
                    .retryingOn(KeeperException.BadVersionException.class)
                    .throwingOn(Exception.class)
                    .run(() -> {

                        byte[] data = client.getData().storingStatIn(stat).forPath(path);
                        ReaderGroupData groupData = groupDataSerializer.deserialize(ByteBuffer.wrap(data));
                        groupData.setState(ReaderGroupData.State.Sealed);

                        client.setData()
                                .withVersion(stat.getVersion())
                                .forPath(path, groupDataSerializer.serialize(groupData).array());
                        return getPositions(process, readerGroup);

                    });
        } catch (KeeperException.NoNodeException e) {
            throw new CheckpointStoreException(CheckpointStoreException.Type.NoNode, e);
        } catch (Exception e) {
            throw new CheckpointStoreException(e);
        }
    }

    @Override
    public void removeReaderGroup(String process, String readerGroup) {
        String path = getReaderGroupPath(process, readerGroup);
        byte[] data = getData(path);

        ReaderGroupData groupData = groupDataSerializer.deserialize(ByteBuffer.wrap(data));

        if (groupData.getState() == ReaderGroupData.State.Active) {
            throw new CheckpointStoreException(CheckpointStoreException.Type.Active, "ReaderGroup is active.");
        }
        if (!groupData.getReaderIds().isEmpty()) {
            throw new CheckpointStoreException(CheckpointStoreException.Type.NodeNotEmpty, "ReaderGroup is not empty.");
        }
        removeEmptyNode(path);
    }

    @Override
    public List<String> getReaderGroups(String process) {
        return getChildren(getProcessPath(process));
    }

    @Override
    public void addReader(String process, String readerGroup, String readerId) {
        String path = getReaderGroupPath(process, readerGroup);
        Stat stat = new Stat();

        try {
            Retry.withExpBackoff(initialMillis, multiplier, attempts, maxDelay)
                    .retryingOn(KeeperException.BadVersionException.class)
                    .throwingOn(Exception.class)
                    .run(() -> {

                        byte[] data = client.getData().storingStatIn(stat).forPath(path);
                        ReaderGroupData groupData = groupDataSerializer.deserialize(ByteBuffer.wrap(data));
                        if (groupData.getState() == ReaderGroupData.State.Sealed) {
                            throw new CheckpointStoreException(CheckpointStoreException.Type.Sealed,
                                    "ReaderGroup is sealed");
                        }
                        List<String> list = groupData.getReaderIds();
                        if (list.contains(readerId)) {
                            throw new CheckpointStoreException(CheckpointStoreException.Type.NodeExists,
                                    "Duplicate readerId");
                        }

                        list.add(readerId);
                        groupData.setReaderIds(list);

                        client.setData()
                                .withVersion(stat.getVersion())
                                .forPath(path, groupDataSerializer.serialize(groupData).array());

                        addNode(getReaderPath(process, readerGroup, readerId));
                        return null;
                    });
        } catch (KeeperException.NoNodeException e) {
            throw new CheckpointStoreException(CheckpointStoreException.Type.NoNode, e);
        } catch (CheckpointStoreException e) {
            throw e;
        } catch (Exception e) {
            throw new CheckpointStoreException(e);
        }

    }

    @Override
    public void removeReader(String process, String readerGroup, String readerId) {
        String path = getReaderGroupPath(process, readerGroup);
        Stat stat = new Stat();

        try {
            Retry.withExpBackoff(initialMillis, multiplier, attempts, maxDelay)
                    .retryingOn(KeeperException.BadVersionException.class)
                    .throwingOn(Exception.class)
                    .run(() -> {

                        removeEmptyNode(getReaderPath(process, readerGroup, readerId));

                        byte[] data = client.getData().storingStatIn(stat).forPath(path);
                        ReaderGroupData groupData = groupDataSerializer.deserialize(ByteBuffer.wrap(data));

                        List<String> list = groupData.getReaderIds();
                        if (list.contains(readerId)) {
                            list.remove(readerId);
                            groupData.setReaderIds(list);

                            client.setData()
                                    .withVersion(stat.getVersion())
                                    .forPath(path, groupDataSerializer.serialize(groupData).array());
                        }

                        return null;
                    });
        } catch (KeeperException.NoNodeException e) {
            throw new CheckpointStoreException(CheckpointStoreException.Type.NoNode, e);
        } catch (Exception e) {
            throw new CheckpointStoreException(e);
        }
    }

    private String getReaderPath(String process, String readerGroup, String readerId) {
        return String.format("/%s/%s/%s", process, readerGroup, readerId);
    }

    private String getReaderGroupPath(String process, String readerGroup) {
        return String.format("/%s/%s", process, readerGroup);
    }

    private String getProcessPath(String process) {
        return String.format("/%s", process);
    }

    private void addNode(String path) {
        addNode(path, new byte[0]);
    }

    private void addNode(String path, byte[] data) {
        try {

            client.create()
                    .creatingParentsIfNeeded()
                    .withMode(CreateMode.PERSISTENT)
                    .forPath(path, data);

        } catch (KeeperException.NodeExistsException e) {
            throw new CheckpointStoreException(CheckpointStoreException.Type.NodeExists, e);
        } catch (Exception e) {
            throw new CheckpointStoreException(e);
        }
    }

    private void removeEmptyNode(String path) {
        try {

            client.delete().forPath(path);

        } catch (KeeperException.NoNodeException e) {
            // Its ok if the node is already deleted, mask this exception.
        } catch (KeeperException.NotEmptyException e) {
            throw new CheckpointStoreException(CheckpointStoreException.Type.NodeNotEmpty, e);
        } catch (Exception e) {
            throw new CheckpointStoreException(e);
        }
    }

    private void updateNode(String path, byte[] data) {
        try {

            client.setData().forPath(path, data);

        } catch (KeeperException.NoNodeException e) {
            throw new CheckpointStoreException(CheckpointStoreException.Type.NoNode, e);
        } catch (Exception e) {
            throw new CheckpointStoreException(e);
        }
    }

    private List<String> getChildren(String path) {
        try {

            return client.getChildren().forPath(path);

        } catch (KeeperException.NoNodeException e) {
            // Return empty list for KeeperException.NoNodeException.
            return Collections.emptyList();
        } catch (Exception e) {
            throw new CheckpointStoreException(e);
        }
    }

    private byte[] getData(String path) {
        try {

            return client.getData().forPath(path);

        } catch (KeeperException.NoNodeException e) {
            throw new CheckpointStoreException(CheckpointStoreException.Type.NoNode, e);
        } catch (Exception e) {
            throw new CheckpointStoreException(e);
        }
    }
}
