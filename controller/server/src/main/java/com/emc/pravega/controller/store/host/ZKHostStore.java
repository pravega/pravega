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
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.SerializationUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.utils.ZKPaths;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.emc.pravega.controller.util.Config.HOST_STORE_CONTAINER_COUNT;
import static com.emc.pravega.controller.util.ZKUtils.createPathIfNotExists;

@Slf4j
public class ZKHostStore implements HostControllerStore {

    protected final NodeCache segContainerHostMapping;
    private final CuratorFramework zkClient;
    private final String path = ZKPaths.makePath("cluster", "segmentContainerHostMapping");

    public ZKHostStore(CuratorFramework client) {
        zkClient = client;
        if (zkClient.getState().equals(CuratorFrameworkState.LATENT)) {
            zkClient.start();
        }

        createPathIfNotExists(zkClient, path, SerializationUtils.serialize(new HashMap<Integer, Host>()));
        segContainerHostMapping = new NodeCache(zkClient, path);
        try {
            segContainerHostMapping.start(true);
        } catch (Exception e) {
            log.error("Error while fetching Segment container to Host mapping from container", e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public Set<Host> getHosts() {
        return getCurrentData().values().stream().collect(Collectors.toSet());
    }

    @Override
    public Set<Integer> getContainersForHost(Host host) {
        Map<Integer, Host> containerHostMap = getCurrentData();
        if (containerHostMap.containsValue(host)) {
            return containerHostMap.entrySet().stream()
                    .filter(ep -> ep.getValue().equals(host))
                    .map(Map.Entry::getKey)
                    .collect(Collectors.toSet());
        } else {
            throw new HostNotFoundException(host);
        }
    }

    @Override
    public Host getHostForContainer(int containerId) {
        Map<Integer, Host> mapping = getCurrentData();
        return mapping.get(containerId);
    }

    private Map<Integer, Host> getCurrentData() {
        Optional<ChildData> mappingSer = Optional.of(segContainerHostMapping.getCurrentData());
        if (mappingSer.isPresent()) {
            return (Map<Integer, Host>) SerializationUtils.deserialize(mappingSer.get().getData());
        } else {
            throw new HostControllerException("Error: Segment Container to HostMapping not initialized.");
        }
    }

    @Override
    public Integer getContainerCount() {
        return HOST_STORE_CONTAINER_COUNT;
    }
}
