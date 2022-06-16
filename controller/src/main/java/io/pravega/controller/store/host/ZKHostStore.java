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
package io.pravega.controller.store.host;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import io.pravega.common.cluster.Host;
import io.pravega.common.cluster.HostContainerMap;
import io.pravega.controller.util.ZKUtils;
import io.pravega.shared.NameUtils;
import io.pravega.shared.segment.SegmentToContainerMapper;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import lombok.SneakyThrows;
import lombok.Synchronized;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.utils.ZKPaths;

import javax.annotation.concurrent.GuardedBy;

/**
 * Zookeeper based implementation of the HostControllerStore.
 */
@SuppressWarnings("deprecation")
@Slf4j
public class ZKHostStore implements HostControllerStore {

    //The path used to store the segment container mapping.
    private final String zkPath;

    //The supplied curator framework instance.
    private final CuratorFramework zkClient;

    private final Object lock = new Object();

    @GuardedBy("$lock")
    //To bootstrap zookeeper on first use.
    private boolean zkInit = false;

    private final SegmentToContainerMapper segmentMapper;

    private final NodeCache hostContainerMapNode;

    private final AtomicReference<HostContainerMap> hostContainerMap;
    /**
     * The tests can add listeners to get notification when the update has happened in the store.
     */
    private final AtomicReference<Listener> listenerRef;
    /**
     * Zookeeper based host store implementation.
     *
     * @param client                    The curator client instance.
     * @param containerCount            Number of containers in the system.
     */
    @VisibleForTesting
    public ZKHostStore(CuratorFramework client, int containerCount) {
        Preconditions.checkNotNull(client, "client");

        zkClient = client;
        zkPath = ZKPaths.makePath("cluster", "segmentContainerHostMapping");
        segmentMapper = new SegmentToContainerMapper(containerCount, true);
        hostContainerMapNode = new NodeCache(zkClient, zkPath);
        hostContainerMap = new AtomicReference<>(HostContainerMap.EMPTY);
        listenerRef = new AtomicReference<>();
    }

    //Ensure required zk node is present in zookeeper.
    @Synchronized
    @SneakyThrows(Exception.class)
    private void tryInit() {
        if (!zkInit) {
            // we are making remote calls under a lock but this is only done for initialization at
            // the start of controller process.
            ZKUtils.createPathIfNotExists(zkClient, zkPath, HostContainerMap.EMPTY.toBytes());
            hostContainerMapNode.getListenable().addListener(this::updateMap);
            hostContainerMapNode.start(true);
            updateMap();
            zkInit = true;
        }
    }

    @Synchronized
    private void updateMap() {
        hostContainerMap.set(HostContainerMap.fromBytes(hostContainerMapNode.getCurrentData().getData()));
        // Following signal is meant only for testing
        Listener consumer = listenerRef.get();
        if (consumer != null) {
            consumer.signal();
        }
    }

    @Override
    public Map<Host, Set<Integer>> getHostContainersMap() {
        tryInit();

        return hostContainerMap.get().getHostContainerMap();
    }

    @Override
    public void updateHostContainersMap(Map<Host, Set<Integer>> newMapping) {
        Preconditions.checkNotNull(newMapping, "newMapping");
        tryInit();
        byte[] serializedMap = HostContainerMap.createHostContainerMap(newMapping).toBytes();
        try {
            zkClient.setData().forPath(zkPath, serializedMap);
            log.info("Successfully updated segment container map");
        } catch (Exception e) {
            throw new HostStoreException("Failed to persist segment container map to zookeeper", e);
        }
    }

    private Host getHostForContainer(int containerId) {
        tryInit();

        // Note: the reference for hostContainerMap may be updated as we are accessing it. However, the map is immutable.
        Optional<Host> host = hostContainerMap.get().getHostContainerMap().entrySet().stream()
                                     .filter(x -> x.getValue().contains(containerId)).map(Map.Entry::getKey).findAny();
        if (host.isPresent()) {
            log.debug("Found owning host: {} for containerId: {}", host.get(), containerId);
            return host.get();
        } else {
            throw new HostStoreException("Could not find host for container id: " + containerId);
        }
    }

    @Override
    public int getContainerCount() {
        return segmentMapper.getTotalContainerCount();
    }

    @Override
    public Host getHostForSegment(String scope, String stream, long segmentId) {
        String qualifiedName = NameUtils.getQualifiedStreamSegmentName(scope, stream, segmentId);
        return getHostForContainer(segmentMapper.getContainerId(qualifiedName));
    }

    @Override
    public Host getHostForTableSegment(String tableName) {
        return getHostForContainer(segmentMapper.getContainerId(tableName));
    }

    @VisibleForTesting
    public void addListener(Listener listener) {
        this.listenerRef.set(listener);
    }

    /**
     * Functional interface to notify tests about changes to the map as they occur.
     */
    @VisibleForTesting
    @FunctionalInterface
    public interface Listener {
        void signal();
    }
}
