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
import com.emc.pravega.common.cluster.EndPoint;
import com.emc.pravega.common.cluster.NodeType;
import com.google.common.annotations.VisibleForTesting;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.nodes.PersistentNode;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.CreateMode;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Zookeeper based implementation of Cluster
 */
@Slf4j
public class ClusterZKImpl implements Cluster, AutoCloseable {

    private final static int RETRY_SLEEP_MS = 100;
    private final static int MAX_RETRY = 5;
    private final static int INIT_SIZE = 3;
    private final static String PATH_CLUSTER = "/cluster/";

    private final CuratorFramework client;
    private final String name;
    private final NodeType type;

    private Map<String, PersistentNode> entryMap = new HashMap<>(INIT_SIZE);

    public ClusterZKImpl(String connectionString, String clusterName, NodeType nodeType) {
        this.name = clusterName;
        this.type = nodeType;
        this.client = CuratorFrameworkFactory.newClient(connectionString, new ExponentialBackoffRetry(RETRY_SLEEP_MS, MAX_RETRY));
        client.start(); // start zk Client
    }

    @Override
    public void registerNode(EndPoint endPoint) throws Exception {

        String basePath = ZKPaths.makePath(PATH_CLUSTER, name, type.name());
        if (client.checkExists().forPath(basePath) == null) {
            client.create().creatingParentsIfNeeded().forPath(basePath);
        }
        String nodePath = ZKPaths.makePath(basePath, endPoint.getHost());
        PersistentNode node = new PersistentNode(client, CreateMode.EPHEMERAL, false, nodePath, endPoint.toString().getBytes());

        node.start(); //start creation of ephemeral node in background.
        entryMap.put(endPoint.getHost(), node);
    }

    @Override
    public void deregisterNode(EndPoint endPoint) throws Exception {
        PersistentNode node = entryMap.get(endPoint.getHost());
        try {
            if (node == null) {
                throw new IllegalArgumentException("No endpoint present inside cluster: " + name + " EndPoint: " + endPoint);
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
        client.close();
    }
}
