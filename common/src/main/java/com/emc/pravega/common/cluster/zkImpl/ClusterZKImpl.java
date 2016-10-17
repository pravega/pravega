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
import com.emc.pravega.common.cluster.Host;
import com.emc.pravega.common.cluster.NodeType;
import com.emc.pravega.common.util.CollectionHelpers;
import com.google.common.annotations.VisibleForTesting;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.SerializationUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.framework.recipes.nodes.PersistentNode;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Zookeeper based implementation of Cluster
 * - It uses persistent ephemeral node which is an ephemeral node that attempts to stay present in ZooKeeper, even through
 * connection and session interruptions.
 * - Ephemeral Node is valid until a session timeout, default session timeout is 60 seconds.
 * System property "curator-default-session-timeout" can be used to change it.
 */
@Slf4j
public class ClusterZKImpl implements Cluster, AutoCloseable {

    private final static String PATH_CLUSTER = "/cluster/";
    private final static int INIT_SIZE = 3;

    private final CuratorFramework client;
    private final String clusterName;
    private final NodeType nodeType;

    private Map<String, PersistentNode> entryMap = new HashMap<>(INIT_SIZE);

    public ClusterZKImpl(CuratorFramework zkClient, String clusterName, NodeType nodeType) {
        this.clusterName = clusterName;
        this.nodeType = nodeType;
        this.client = zkClient;
        if (client.getState().equals(CuratorFrameworkState.LATENT))
            client.start();
    }

    @Override
    public void registerNode(Host endPoint) throws Exception {

        String basePath = ZKPaths.makePath(PATH_CLUSTER, clusterName, nodeType.name());
        createPathIfExists(basePath);
        String nodePath = ZKPaths.makePath(basePath, endPoint.getIpAddr());

        PersistentNode node = new PersistentNode(client, CreateMode.EPHEMERAL, false, nodePath, SerializationUtils.serialize(endPoint));

        node.start(); //start creation of ephemeral node in background.
        entryMap.put(endPoint.getIpAddr(), node);
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

    @Override
    public void deregisterNode(Host endPoint) throws Exception {
        PersistentNode node = entryMap.get(endPoint.getIpAddr());
        try {
            if (node == null) {
                throw new IllegalArgumentException("Host not present inside cluster: " + clusterName + " Host: " + endPoint);
            } else
                node.close();
        } catch (IOException ex) {
            log.error("Error while removing node from cluster", ex);
        }
    }

    @VisibleForTesting
    public Map<String, PersistentNode> getEntries() {
        return Collections.unmodifiableMap(entryMap);
    }

    @Override
    public void close() throws Exception {
        CollectionHelpers.forEach(entryMap.values(), PersistentNode::close);
    }
}
