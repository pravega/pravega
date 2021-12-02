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
package io.pravega.common.cluster.zkImpl;

import io.pravega.common.Exceptions;
import io.pravega.common.cluster.Cluster;
import io.pravega.common.cluster.ClusterException;
import io.pravega.common.cluster.ClusterListener;
import io.pravega.common.cluster.Host;
import com.google.common.base.Preconditions;
import lombok.Synchronized;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.framework.recipes.nodes.PersistentNode;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.CreateMode;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static io.pravega.common.cluster.ClusterListener.EventType.ERROR;
import static io.pravega.common.cluster.ClusterListener.EventType.HOST_ADDED;
import static io.pravega.common.cluster.ClusterListener.EventType.HOST_REMOVED;

/**
 * Zookeeper based implementation of Cluster.
 * - It uses persistent ephemeral node which is an ephemeral node that attempts to stay present in ZooKeeper, even through
 * connection and session interruptions.
 * - Ephemeral Node is valid until a session timeout, default session timeout is 60 seconds.
 * System property "curator-default-session-timeout" can be used to change it.
 */
@SuppressWarnings("deprecation")
@Slf4j
public class ClusterZKImpl implements Cluster {

    private final static String PATH_CLUSTER = "/cluster/";

    private final static int INIT_SIZE = 3;
    private final String clusterName;

    private final CuratorFramework client;

    private final AtomicBoolean isZKConnected = new AtomicBoolean(false);

    private final Map<Host, PersistentNode> entryMap = new HashMap<>(INIT_SIZE);
    private Optional<PathChildrenCache> cache = Optional.empty();

    public ClusterZKImpl(CuratorFramework zkClient, String clusterName) {
        this.client = zkClient;
        this.clusterName = clusterName;
        if (client.getState().equals(CuratorFrameworkState.LATENT)) {
            client.start();
        }
        this.isZKConnected.set(client.getZookeeperClient().isConnected());
        //Listen for any zookeeper connection state changes
        client.getConnectionStateListenable().addListener(
                (curatorClient, newState) -> this.isZKConnected.set(newState.isConnected()));
    }

    /**
     * Register Host to cluster.
     *
     * @param host Host to be part of cluster.
     */
    @Override
    @Synchronized
    public void registerHost(Host host) {
        Preconditions.checkNotNull(host, "host");
        Exceptions.checkArgument(!entryMap.containsKey(host), "host", "host is already registered to cluster.");

        String hostPath = ZKPaths.makePath(getPathPrefix(), host.toString());
        PersistentNode node = new PersistentNode(client, CreateMode.EPHEMERAL, false, hostPath,
                host.toBytes());

        node.start(); //start creation of ephemeral node in background.
        entryMap.put(host, node);
    }

    /**
     * Remove Host from cluster.
     *
     * @param host Host to be removed from cluster.
     */
    @Override
    @Synchronized
    public void deregisterHost(Host host) {
        Preconditions.checkNotNull(host, "host");
        PersistentNode node = entryMap.get(host);
        Preconditions.checkNotNull(node, "Host is not present in cluster.");
        entryMap.remove(host);
        close(node);
    }

    @Override
    public boolean isHealthy() {
        return isZKConnected.get();
    }

    /**
     * Add Listener to the cluster.
     *
     * @param listener Cluster event Listener.
     */
    @Override
    @Synchronized
    public void addListener(ClusterListener listener) {
        Preconditions.checkNotNull(listener, "listener");
        if (!cache.isPresent()) {
            initializeCache();
        }
        cache.get().getListenable().addListener(pathChildrenCacheListener(listener));
    }

    /**
     * Add Listener to the cluster.
     *
     * @param listener Cluster event Listener.
     * @param executor Executor to run the listener on.
     */
    @Override
    @Synchronized
    public void addListener(final ClusterListener listener, final Executor executor) {
        Preconditions.checkNotNull(listener, "listener");
        Preconditions.checkNotNull(executor, "executor");
        if (!cache.isPresent()) {
            initializeCache();
        }
        cache.get().getListenable().addListener(pathChildrenCacheListener(listener), executor);
    }

    /**
     * Get the current cluster members.
     *
     * @return List of cluster members.
     */
    @Override
    @Synchronized
    public Set<Host> getClusterMembers() {
        if (!cache.isPresent()) {
            initializeCache();
        }
        List<ChildData> data = cache.get().getCurrentData();
        return data.stream()
                .map(d -> Host.fromBytes(d.getData()))
                .collect(Collectors.toSet());
    }

    @Override
    public void close() throws Exception {
        synchronized (entryMap) {
            entryMap.values().forEach(this::close);
            cache.ifPresent(this::close);
        }
    }

    private void close(Closeable c) {
        if (c == null) {
            return;
        }
        try {
            c.close();
        } catch (IOException e) {
            log.error("Error while closing resource", e);
        }
    }

    private void initializeCache() throws ClusterException {
        cache = Optional.of(new PathChildrenCache(client, getPathPrefix(), true));
        try {
            cache.get().start(PathChildrenCache.StartMode.BUILD_INITIAL_CACHE);
        } catch (Exception e) {
            throw ClusterException.create(ClusterException.Type.METASTORE,
                                          "Failed to initialize ZooKeeper cache: " + e.getMessage());
        }
    }

    private PathChildrenCacheListener pathChildrenCacheListener(final ClusterListener listener) {
        return (client, event) -> {
            log.debug("Event {} generated on cluster", event);
            switch (event.getType()) {
                case CHILD_ADDED:
                    log.info("Node {} added to cluster", getServerName(event));
                    listener.onEvent(HOST_ADDED, Host.fromBytes(event.getData().getData()));
                    break;
                case CHILD_REMOVED:
                    log.info("Node {} removed from cluster", getServerName(event));
                    listener.onEvent(HOST_REMOVED, Host.fromBytes(event.getData().getData()));
                    break;
                case CHILD_UPDATED:
                    log.warn("Invalid usage: Node {} updated externally for cluster", getServerName(event));
                    break;
                case CONNECTION_LOST:
                    log.error("Connection lost with Zookeeper");
                    listener.onEvent(ERROR, null);
                    break;
                //$CASES-OMITTED$
                default:
                    log.warn("Received unknown event {}", event.getType());
            }
        };
    }

    private String getServerName(final PathChildrenCacheEvent event) {
        String path = event.getData().getPath();
        return path.substring(path.lastIndexOf("/") + 1);
    }

    private String getPathPrefix() {
        return ZKPaths.makePath(PATH_CLUSTER, clusterName);
    }
}
