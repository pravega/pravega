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
import io.pravega.client.stream.Serializer;
import io.pravega.client.stream.impl.JavaSerializer;
import io.pravega.common.Exceptions;
import io.pravega.common.util.Retry;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;

/**
 * Zookeeper based checkpoint store.
 */
@Slf4j
public class ZKCheckpointStore implements CheckpointStore {

    private static final String ROOT = "eventProcessors";
    private final CuratorFramework client;
    private final Serializer<Position> positionSerializer;
    private final JavaSerializer<ReaderGroupData> groupDataSerializer;
    private final AtomicBoolean isZKConnected = new AtomicBoolean(false);

    ZKCheckpointStore(CuratorFramework client) {
        this.client = client;
        this.positionSerializer = new Serializer<Position>() {
            @Override
            public ByteBuffer serialize(Position value) {
                return value.toBytes();
            }
            
            @Override
            public Position deserialize(ByteBuffer serializedValue) {
                return Position.fromBytes(serializedValue);
            }
        };
        this.groupDataSerializer = new JavaSerializer<>();
        this.isZKConnected.set(client.getZookeeperClient().isConnected());
        //Listen for any zookeeper connection state changes
        client.getConnectionStateListenable().addListener(
                (curatorClient, newState) -> {
                    this.isZKConnected.set(newState.isConnected());
                });
    }

    @Data
    @AllArgsConstructor
    private static class ReaderGroupData implements Serializable {
        enum State {
            Active,
            Sealed,
        }

        private final State state;
        private final List<String> readerIds;
    }

    /**
     * Get the zookeeper health status.
     *
     * @return true if zookeeper is connected.
     */
    @Override
    public boolean isHealthy() {
        return isZKConnected.get();
     }

    @Override
    public void setPosition(String process, String readerGroup, String readerId, Position position) throws CheckpointStoreException {
        updateNode(getReaderPath(process, readerGroup, readerId), positionSerializer.serialize(position).array());
    }

    @Override
    public Map<String, Position> getPositions(String process, String readerGroup) throws CheckpointStoreException {
        Map<String, Position> map = new HashMap<>();
        String path = getReaderGroupPath(process, readerGroup);
        ReaderGroupData rgData = groupDataSerializer.deserialize(ByteBuffer.wrap(getData(path)));
        rgData.getReaderIds().forEach(x -> map.put(x, null));
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
    public void addReaderGroup(String process, String readerGroup) throws CheckpointStoreException {
        ReaderGroupData data = new ReaderGroupData(ReaderGroupData.State.Active, new ArrayList<>());
        addNode(getReaderGroupPath(process, readerGroup), groupDataSerializer.serialize(data).array());
    }

    @Override
    public Map<String, Position> sealReaderGroup(String process, String readerGroup) throws CheckpointStoreException {
        String path = getReaderGroupPath(process, readerGroup);

        try {
            updateReaderGroupData(path, groupData ->
                    new ReaderGroupData(ReaderGroupData.State.Sealed, groupData.getReaderIds()));

            return getPositions(process, readerGroup);

        } catch (KeeperException.NoNodeException e) {
            throw new CheckpointStoreException(CheckpointStoreException.Type.NoNode, e);
        } catch (KeeperException.ConnectionLossException | KeeperException.OperationTimeoutException
                | KeeperException.SessionExpiredException e) {
            throw new CheckpointStoreException(CheckpointStoreException.Type.Connectivity, e);
        } catch (Exception e) {
            throw new CheckpointStoreException(e);
        }
    }

    @Override
    public void removeReaderGroup(String process, String readerGroup) throws CheckpointStoreException {
        String path = getReaderGroupPath(process, readerGroup);

        byte[] data;
        try {
            data = getData(path);
        } catch (CheckpointStoreException e) {
            if (e.getType().equals(CheckpointStoreException.Type.NoNode)) {
                return;
            } else {
                throw e;
            }
        }

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
    public List<String> getReaderGroups(String process) throws CheckpointStoreException {
        return getChildren(getProcessPath(process));
    }

    @Override
    public void addReader(String process, String readerGroup, String readerId) throws CheckpointStoreException {
        String path = getReaderGroupPath(process, readerGroup);

        try {

            updateReaderGroupData(path, groupData -> {
                if (groupData.getState() == ReaderGroupData.State.Sealed) {
                    throw Exceptions.sneakyThrow(new CheckpointStoreException(CheckpointStoreException.Type.Sealed,
                            "ReaderGroup is sealed"));
                }
                List<String> list = groupData.getReaderIds();
                if (list.contains(readerId)) {
                    throw Exceptions.sneakyThrow(new CheckpointStoreException(CheckpointStoreException.Type.NodeExists,
                            "Duplicate readerId"));
                }

                list.add(readerId);
                return new ReaderGroupData(groupData.getState(), list);
            });

            addNode(getReaderPath(process, readerGroup, readerId));

        } catch (KeeperException.NoNodeException e) {
            throw new CheckpointStoreException(CheckpointStoreException.Type.NoNode, e);
        } catch (KeeperException.ConnectionLossException | KeeperException.OperationTimeoutException
                | KeeperException.SessionExpiredException e) {
            throw new CheckpointStoreException(CheckpointStoreException.Type.Connectivity, e);
        } catch (CheckpointStoreException e) {
            throw e;
        } catch (Exception e) {
            throw new CheckpointStoreException(e);
        }
    }

    @Override
    public void removeReader(String process, String readerGroup, String readerId) throws CheckpointStoreException {
        String path = getReaderGroupPath(process, readerGroup);

        try {
            removeEmptyNode(getReaderPath(process, readerGroup, readerId));

            updateReaderGroupData(path, groupData -> {
                List<String> list = groupData.getReaderIds();
                if (list.contains(readerId)) {
                    list.remove(readerId);
                    return new ReaderGroupData(groupData.getState(), list);
                } else {
                    return groupData;
                }
            });

        } catch (KeeperException.NoNodeException e) {
            log.debug("ZNode for reader {} in Reader Group {} for Controller {} not found.", readerId, readerGroup, process);
        } catch (KeeperException.ConnectionLossException | KeeperException.OperationTimeoutException
                | KeeperException.SessionExpiredException e) {
            throw new CheckpointStoreException(CheckpointStoreException.Type.Connectivity, e);
        } catch (CheckpointStoreException e) {
            throw e;
        } catch (Exception e) {
            throw new CheckpointStoreException(e);
        }
    }

    @Override
    public Set<String> getProcesses() throws CheckpointStoreException {
        return getChildren(getRootPath()).stream().collect(Collectors.toSet());
    }

    private String getReaderPath(String process, String readerGroup, String readerId) {
        return String.format("/%s/%s/%s/%s", ROOT, process, readerGroup, readerId);
    }

    private String getReaderGroupPath(String process, String readerGroup) {
        return String.format("/%s/%s/%s", ROOT, process, readerGroup);
    }

    private String getProcessPath(String process) {
        return String.format("/%s/%s", ROOT, process);
    }

    private String getRootPath() {
        return String.format("/%s", ROOT);
    }

    /**
     * Updates the reader group data at specified path by applying the updater method on the existing data.
     * It repeatedly invokes conditional update on specified path until is succeeds or max attempts (10) are exhausted.
     *
     * @param path Reader group node path.
     * @param updater Function to obtain the new data value from existing data value.
     * @throws Exception Throws exception thrown from Curator, or from application of updater method.
     */
    private void updateReaderGroupData(String path, Function<ReaderGroupData, ReaderGroupData> updater) throws Exception {
        final long initialMillis = 100L;
        final int multiplier = 2;
        final int attempts = 10;
        final long maxDelay = 2000;

        Stat stat = new Stat();

        Retry.withExpBackoff(initialMillis, multiplier, attempts, maxDelay)
                .retryingOn(KeeperException.BadVersionException.class)
                .throwingOn(Exception.class)
                .run(() -> {
                    byte[] data = client.getData().storingStatIn(stat).forPath(path);
                    ReaderGroupData groupData = groupDataSerializer.deserialize(ByteBuffer.wrap(data));
                    groupData = updater.apply(groupData);
                    byte[] newData = groupDataSerializer.serialize(groupData).array();

                    client.setData()
                            .withVersion(stat.getVersion())
                            .forPath(path, newData);
                    return null;
                });
    }

    private void addNode(String path) throws CheckpointStoreException {
        addNode(path, new byte[0]);
    }

    private void addNode(String path, byte[] data) throws CheckpointStoreException {
        try {

            client.create()
                    .creatingParentsIfNeeded()
                    .withMode(CreateMode.PERSISTENT)
                    .forPath(path, data);

        } catch (KeeperException.NodeExistsException e) {
            throw new CheckpointStoreException(CheckpointStoreException.Type.NodeExists, e);
        } catch (KeeperException.ConnectionLossException | KeeperException.OperationTimeoutException
                | KeeperException.SessionExpiredException e) {
            throw new CheckpointStoreException(CheckpointStoreException.Type.Connectivity, e);
        } catch (Exception e) {
            throw new CheckpointStoreException(e);
        }
    }

    private void removeEmptyNode(String path) throws CheckpointStoreException {
        try {

            client.delete().forPath(path);

        } catch (KeeperException.NoNodeException e) {
            // Its ok if the node is already deleted, mask this exception.
        } catch (KeeperException.NotEmptyException e) {
            throw new CheckpointStoreException(CheckpointStoreException.Type.NodeNotEmpty, e);
        } catch (KeeperException.ConnectionLossException | KeeperException.OperationTimeoutException
                | KeeperException.SessionExpiredException e) {
            throw new CheckpointStoreException(CheckpointStoreException.Type.Connectivity, e);
        } catch (Exception e) {
            throw new CheckpointStoreException(e);
        }
    }

    private void updateNode(String path, byte[] data) throws CheckpointStoreException {
        try {

            client.setData().forPath(path, data);

        } catch (KeeperException.NoNodeException e) {
            throw new CheckpointStoreException(CheckpointStoreException.Type.NoNode, e);
        } catch (KeeperException.ConnectionLossException | KeeperException.OperationTimeoutException
                | KeeperException.SessionExpiredException e) {
            throw new CheckpointStoreException(CheckpointStoreException.Type.Connectivity, e);
        } catch (Exception e) {
            throw new CheckpointStoreException(e);
        }
    }

    private List<String> getChildren(String path) throws CheckpointStoreException {
        try {

            return client.getChildren().forPath(path);

        } catch (KeeperException.NoNodeException e) {
            // Return empty list for KeeperException.NoNodeException.
            return Collections.emptyList();
        } catch (KeeperException.ConnectionLossException | KeeperException.OperationTimeoutException
                | KeeperException.SessionExpiredException e) {
            throw new CheckpointStoreException(CheckpointStoreException.Type.Connectivity, e);
        } catch (Exception e) {
            throw new CheckpointStoreException(e);
        }
    }

    private byte[] getData(String path) throws CheckpointStoreException {
        try {

            return client.getData().forPath(path);

        } catch (KeeperException.NoNodeException e) {
            throw new CheckpointStoreException(CheckpointStoreException.Type.NoNode, e);
        } catch (KeeperException.ConnectionLossException | KeeperException.OperationTimeoutException
                | KeeperException.SessionExpiredException e) {
            throw new CheckpointStoreException(CheckpointStoreException.Type.Connectivity, e);
        } catch (Exception e) {
            throw new CheckpointStoreException(e);
        }
    }
}
