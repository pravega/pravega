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
package com.emc.pravega.controller.store.host;

import com.emc.pravega.common.cluster.Host;
import com.emc.pravega.controller.util.Config;
import com.emc.pravega.controller.util.ZKUtils;
import com.google.common.base.Preconditions;
import lombok.Synchronized;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.SerializationUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.utils.ZKPaths;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Zookeeper based implementation of the HostControllerStore.
 */
@Slf4j
public class ZKHostStore implements HostControllerStore {

    //The path used to store the segment container mapping.
    private final String zkPath;

    //The supplied curator framework instance.
    private final CuratorFramework zkClient;

    //To bootstrap zookeeper on first use.
    private volatile boolean zkInit = false;

    public ZKHostStore(CuratorFramework client, String clusterName) {
        Preconditions.checkNotNull(client, "Curator Client");
        Preconditions.checkNotNull(clusterName, "clusterName");

        zkClient = client;
        if (zkClient.getState().equals(CuratorFrameworkState.LATENT)) {
            zkClient.start();
        }
        zkPath = ZKPaths.makePath("cluster", clusterName, "segmentContainerHostMapping");
    }

    //Ensure required zk node is present in zookeeper.
    @Synchronized
    private void tryInit() throws Exception {
        if (!zkInit) {
            ZKUtils.createPathIfNotExists(zkClient, zkPath, SerializationUtils.serialize(new HashMap<Host,
                    Set<Integer>>()));
            zkInit = true;
        }
    }

    @Override
    public Map<Host, Set<Integer>> getHostContainersMap() throws Exception {
        tryInit();

        return getCurrentHostMap();
    }

    private Map<Host, Set<Integer>> getCurrentHostMap() throws Exception {
        try {
            return (Map<Host, Set<Integer>>) SerializationUtils.deserialize(zkClient.getData().forPath(zkPath));
        } catch (Exception e) {
            log.warn("Failed to fetch segment container map from zookeeper. Error: " + e.getMessage());
            throw e;
        }
    }

    @Override
    public void updateHostContainersMap(Map<Host, Set<Integer>> newMapping) throws Exception {
        Preconditions.checkNotNull(newMapping, "newMapping");
        tryInit();

        try {
            zkClient.setData().forPath(zkPath, SerializationUtils.serialize((HashMap) newMapping));
            log.debug("Successfully updated segment container map");
        } catch (Exception e) {
            log.warn("Failed to persist segment container map to zookeeper. Error: " + e.getMessage());
            throw e;
        }
    }

    @Override
    public Host getHostForContainer(int containerId) throws Exception {
        tryInit();

        Map<Host, Set<Integer>> mapping = getCurrentHostMap();
        Optional<Host> hosts = mapping.entrySet().stream()
                .filter(x -> x.getValue().contains(containerId)).map(x -> x.getKey()).findAny();
        if (hosts.isPresent()) {
            log.debug("Found owning host: {} for containerId: {}", hosts.get(), containerId);
            return hosts.get();
        } else {
            log.warn("No owning host found for containerId: " + containerId);
            throw new ContainerNotFoundException(containerId);
        }
    }

    @Override
    public int getContainerCount() {
        return Config.HOST_STORE_CONTAINER_COUNT;
    }
}
