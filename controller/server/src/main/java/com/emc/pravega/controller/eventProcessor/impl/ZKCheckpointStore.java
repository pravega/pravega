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
import com.emc.pravega.stream.impl.JavaSerializer;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Zookeeper based checkpoint store.
 */
@Slf4j
class ZKCheckpointStore implements CheckpointStore {

    private final CuratorFramework client;
    private final JavaSerializer<Position> serializer;

    ZKCheckpointStore(CuratorFramework client) {
        this.client = client;
        this.serializer = new JavaSerializer<>();
    }

    @Override
    public void setPosition(String process, String readerGroup, String readerId, Position position) {
        updateNode(getReaderPath(process, readerGroup, readerId), serializer.serialize(position).array());
    }

    @Override
    public Map<String, Position> getPositions(String process, String readerGroup) {
        Map<String, Position> map = new HashMap<>();
        String path = getReaderGroupPath(process, readerGroup);
        for (String child : getChildren(path)) {
            Position position = null;
            byte[] data = getData(path + "/" + child);
            if (data != null && data.length > 0) {
                position = serializer.deserialize(ByteBuffer.wrap(data));
            }
            map.put(child, position);
        }
        return map;
    }

    @Override
    public void addReaderGroup(String process, String readerGroup) {
        addNode(getReaderGroupPath(process, readerGroup));
    }

    @Override
    public void removeReaderGroup(String process, String readerGroup) {
        removeEmptyNode(getReaderGroupPath(process, readerGroup));
    }

    @Override
    public List<String> getReaderGroups(String process) {
        return getChildren(getProcessPath(process));
    }

    @Override
    public void addReader(String process, String readerGroup, String readerId) {
        addNode(getReaderPath(process, readerGroup, readerId));
    }

    @Override
    public void removeReader(String process, String readerGroup, String readerId) {
        removeEmptyNode(getReaderPath(process, readerGroup, readerId));
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
            // Its ok if the node already exists, mask this exception.
        } catch (Exception e) {
            throw new CheckpointStoreException(e);
        }
    }

    private void removeEmptyNode(String path) {
        try {
            client.delete().forPath(path);
        } catch (KeeperException.NoNodeException e) {
            // Its ok if the node is already deleted, mask this exception.
        } catch (Exception e) {
            // else throw exception in case of KeeperException.NotEmptyException or any other exception.
            throw new CheckpointStoreException(e);
        }
    }

    private void updateNode(String path, byte[] data) {
        try {
            client.setData().forPath(path, data);
        } catch (Exception e) {
            // return false for KeeperException.NoNodeException as well
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
            return null;
        } catch (Exception e) {
            throw new CheckpointStoreException(e);
        }
    }
}
