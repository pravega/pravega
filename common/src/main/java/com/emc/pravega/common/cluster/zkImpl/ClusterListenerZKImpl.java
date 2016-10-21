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

import com.emc.pravega.common.cluster.ClusterListener;
import com.emc.pravega.common.cluster.Host;
import com.emc.pravega.common.cluster.NodeType;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.SerializationUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.utils.ZKPaths;

import java.util.List;
import java.util.concurrent.Executor;
import java.util.stream.Collectors;

/**
 * ZK based Cluster Listener implementation.
 */
@Slf4j
public abstract class ClusterListenerZKImpl implements ClusterListener, AutoCloseable {

    private final static String PATH_CLUSTER = "/cluster/";

    @Getter
    private String clusterName;
    private NodeType nodeType;

    private final CuratorFramework client;
    private final PathChildrenCache cache;

    private final PathChildrenCacheListener pathChildrenCacheListener = (client, event) -> {
        log.debug("Event {} generated on cluster:{}", event, clusterName);
        switch (event.getType()) {
            case CHILD_ADDED:
                log.info("Node {} added to cluster:{}", getServerName(event), clusterName);
                nodeAdded((Host) SerializationUtils.deserialize(event.getData().getData()));
                break;
            case CHILD_REMOVED:
                log.info("Node {} removed from cluster:{}", getServerName(event), clusterName);
                nodeRemoved((Host) SerializationUtils.deserialize(event.getData().getData()));
                break;
            case CHILD_UPDATED:
                log.error("Invalid usage: Node {} updated externally for cluster:{}", getServerName(event), clusterName);
                break;
        }
    };

    public ClusterListenerZKImpl(final CuratorFramework zkClient, final String clusterName, final NodeType nodeType) {
        this.clusterName = clusterName;
        this.nodeType = nodeType;
        this.client = zkClient;
        if (client.getState().equals(CuratorFrameworkState.LATENT))
            client.start();

        cache = new PathChildrenCache(client, ZKPaths.makePath(PATH_CLUSTER, clusterName, nodeType.name()), true);
    }

    /**
     * Start listener for a given cluster
     */
    @Override
    public void start() throws Exception {
        cache.getListenable().addListener(pathChildrenCacheListener);
        cache.start(PathChildrenCache.StartMode.POST_INITIALIZED_EVENT);
    }

    /**
     * Start listener on a custom executor.
     *
     * @param executor custom executor on which the listener should run.
     */
    @Override
    public void start(Executor executor) throws Exception {
        cache.getListenable().addListener(pathChildrenCacheListener, executor);
        cache.start(PathChildrenCache.StartMode.POST_INITIALIZED_EVENT);
    }

    @Override
    public void close() throws Exception {
        cache.close();
    }

    /**
     * Get the current list of ClusterMembers.
     *
     * @return
     */
    public List<Host> getClusterMembers() {
        List<ChildData> data = cache.getCurrentData();
        return data.stream()
                .map(d -> (Host) SerializationUtils.deserialize(d.getData()))
                .collect(Collectors.toList());
    }

    private String getServerName(final PathChildrenCacheEvent event) {
        String path = event.getData().getPath();
        return path.substring(path.lastIndexOf("/") + 1);
    }

}
