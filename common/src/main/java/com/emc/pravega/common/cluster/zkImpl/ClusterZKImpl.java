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
package com.emc.pravega.common.cluster.zkImpl;

import com.emc.pravega.common.cluster.Cluster;
import com.emc.pravega.common.cluster.ClusterListener;
import com.emc.pravega.common.cluster.Host;
import com.emc.pravega.common.util.CollectionHelpers;
import lombok.Synchronized;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.SerializationUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.framework.recipes.nodes.PersistentNode;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.emc.pravega.common.cluster.ClusterListener.EventType.HOST_ADDED;
import static com.emc.pravega.common.cluster.ClusterListener.EventType.HOST_REMOVED;

/**
 * Zookeeper based implementation of Cluster
 * - It uses persistent ephemeral node which is an ephemeral node that attempts to stay present in ZooKeeper, even through
 * connection and session interruptions.
 * - Ephemeral Node is valid until a session timeout, default session timeout is 60 seconds.
 * System property "curator-default-session-timeout" can be used to change it.
 */
@Slf4j
public class ClusterZKImpl implements Cluster {

    private final static String PATH_CLUSTER = "/cluster/";
    private final static String HOSTS = "hosts";
    private final static int INIT_SIZE = 3;

    private final CuratorFramework client;
    private final String clusterName;

    private Map<Host, PersistentNode> entryMap = new HashMap<>(INIT_SIZE);
    private Optional<PathChildrenCache> cache = Optional.empty();

    public ClusterZKImpl(CuratorFramework zkClient, String clusterName) {
        this.clusterName = clusterName;
        this.client = zkClient;
        if (client.getState().equals(CuratorFrameworkState.LATENT))
            client.start();
    }

    /**
     * Register Host to cluster.
     *
     * @param host
     * @throws Exception
     */
    @Override
    @Synchronized
    public void registerHost(Host host) throws Exception {
        if (entryMap.containsKey(host))
            throw new IllegalArgumentException("Host already registered to cluster");

        String basePath = ZKPaths.makePath(PATH_CLUSTER, clusterName, HOSTS);
        createPathIfExists(basePath);
        String hostPath = ZKPaths.makePath(basePath, host.getIpAddr());

        PersistentNode node = new PersistentNode(client, CreateMode.EPHEMERAL, false, hostPath, SerializationUtils.serialize(host));

        node.start(); //start creation of ephemeral node in background.
        entryMap.put(host, node);
    }

    /**
     * Remove Host from cluster.
     *
     * @param host
     * @throws Exception
     */
    @Override
    @Synchronized
    public void deregisterHost(Host host) throws Exception {
        PersistentNode node = entryMap.get(host);
        if (node == null) {
            throw new IllegalArgumentException("Host not present inside cluster: " + clusterName + " Host: " + host);
        } else {
            entryMap.remove(host);
            close(node);
        }
    }

    /**
     * Add Listener to the cluster.
     *
     * @param listener - Cluster event Listener
     * @throws Exception
     */
    @Override
    public void addListener(ClusterListener listener) throws Exception {
        if (!cache.isPresent()) {
            initializeCache();
        }
        cache.get().getListenable().addListener(pathChildrenCacheListener(listener));
    }

    private void initializeCache() throws Exception {
        cache = Optional.of(new PathChildrenCache(client, ZKPaths.makePath(PATH_CLUSTER, clusterName, HOSTS), true));
        cache.get().start(PathChildrenCache.StartMode.BUILD_INITIAL_CACHE);
    }

    private PathChildrenCacheListener pathChildrenCacheListener(final ClusterListener listener) {
        return (client, event) -> {
            log.debug("Event {} generated on cluster:{}", event, clusterName);
            switch (event.getType()) {
                case CHILD_ADDED:
                    log.info("Node {} added to cluster:{}", getServerName(event), clusterName);
                    listener.onEvent(HOST_ADDED, (Host) SerializationUtils.deserialize(event.getData().getData()));
                    break;
                case CHILD_REMOVED:
                    log.info("Node {} removed from cluster:{}", getServerName(event), clusterName);
                    listener.onEvent(HOST_REMOVED, (Host) SerializationUtils.deserialize(event.getData().getData()));
                    break;
                case CHILD_UPDATED:
                    log.warn("Invalid usage: Node {} updated externally for cluster:{}", getServerName(event), clusterName);
                    break;
            }
        };
    }

    /**
     * Get the current cluster members.
     *
     * @return List<Host> list of cluster members
     */
    @Override
    public List<Host> getClusterMembers() throws Exception {
        if (!cache.isPresent()) {
            initializeCache();
        }
        List<ChildData> data = cache.get().getCurrentData();
        return data.stream()
                .map(d -> (Host) SerializationUtils.deserialize(d.getData()))
                .collect(Collectors.toList());
    }

    @Override
    public void close() throws Exception {
        CollectionHelpers.forEach(entryMap.values(), this::close);
        if (cache.isPresent())
            close(cache.get());
    }

    private void close(Closeable c) {
        if (c == null) return;
        try {
            c.close();
        } catch (IOException e) {
            log.error("Error while closing resource", e);
        }
    }

    private String getServerName(final PathChildrenCacheEvent event) {
        String path = event.getData().getPath();
        return path.substring(path.lastIndexOf("/") + 1);
    }

    private void createPathIfExists(String basePath) throws Exception {
        try {
            if (client.checkExists().forPath(basePath) == null) {
                client.create().creatingParentsIfNeeded().forPath(basePath);
            }
        } catch (KeeperException.NodeExistsException e) {
            log.debug("Path exists {} , ignoring exception", basePath, e);
        }
    }
}
