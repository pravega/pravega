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

    ZKCheckpointStore(CuratorFramework client) {
        this.client = client;
    }

    @Override
    public boolean setPosition(String process, String readerGroup, String readerId, Position position) {
        return updateNode(getReaderPath(process, readerGroup, readerId),
                new JavaSerializer<Position>().serialize(position).array());
    }

    @Override
    public Map<String, Position> getPositions(String process, String readerGroup) {
        Map<String, Position> map = new HashMap<>();
        String path = getReaderGroupPath(process, readerGroup);
        for (String child : getChildren(path)) {
            Position position = null;
            byte[] data = getData(path + "/" + child);
            if (data != null && data.length > 0) {
                position = new JavaSerializer<Position>().deserialize(ByteBuffer.wrap(data));
            }
            map.put(child, position);
        }
        return map;
    }

    @Override
    public boolean addReaderGroup(String process, String readerGroup) {
        return addNode(getReaderGroupPath(process, readerGroup));
    }

    @Override
    public boolean removeReaderGroup(String process, String readerGroup) {
        return removeEmptyNode(getReaderGroupPath(process, readerGroup));
    }

    @Override
    public List<String> getReaderGroups(String process) {
        return getChildren(getProcessPath(process));
    }

    @Override
    public boolean addReader(String process, String readerGroup, String readerId) {
        return addNode(getReaderPath(process, readerGroup, readerId));
    }

    @Override
    public boolean removeReader(String process, String readerGroup, String readerId) {
        return removeEmptyNode(getReaderPath(process, readerGroup, readerId));
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

    private boolean addNode(String path) {
        return addNode(path, new byte[0]);
    }

    private boolean addNode(String path, byte[] data) {
        try {
            client.create()
                    .creatingParentsIfNeeded()
                    .withMode(CreateMode.PERSISTENT)
                    .forPath(path, data);
            return true;
        } catch (Exception e) {
            if (e instanceof KeeperException.NodeExistsException) {
                return true;
            } else {
                // log this exception and return false
                log.warn("Error creating node for path " + path, e);
                return false;
            }
        }
    }

    private boolean removeEmptyNode(String path) {
        try {
            client.delete()
                    .forPath(path);
            return true;
        } catch (Exception e) {
            // Return true for KeeperException.NoNodeException,
            // else return false for KeeperException.NotEmptyException or any other exception.
            return e instanceof KeeperException.NoNodeException;
        }
    }

    private boolean updateNode(String path, byte[] data) {
        try {
            client.setData().forPath(path, data);
            return true;
        } catch (Exception e) {
            // return false for KeeperException.NoNodeException as well
            return false;
        }
    }

    private List<String> getChildren(String path) {
        try {
            return client.getChildren().forPath(path);
        } catch (Exception e) {
            // Return empty list for KeeperException.NoNodeException or any other exception.
            return Collections.emptyList();
        }
    }

    private byte[] getData(String path) {
        try {
            return client.getData().forPath(path);
        } catch (Exception e) {
            return null;
        }
    }
}
