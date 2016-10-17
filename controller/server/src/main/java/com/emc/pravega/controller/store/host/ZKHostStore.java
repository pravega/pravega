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
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.utils.ZKPaths;

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@Slf4j
public class ZKHostStore implements HostControllerStore {

    private static final int SEGMENT_CONTAINER_COUNT = 128;

    protected final NodeCache segContainerHostMapping;
    private final CuratorFramework zkClient;
    private final String path = ZKPaths.makePath("cluster", "segmentContainerHostMapping");

    public ZKHostStore(CuratorFramework client) {
        zkClient = client;
        segContainerHostMapping = new NodeCache(zkClient, path);

        try {
            segContainerHostMapping.start(true);
        } catch (Exception e) {
            log.error("Error while fetching Segment container to Host mapping from container", e);
            throw new RuntimeException(e);
        }
    }

    private void initStore() {

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
        } else
            throw new HostNotFoundException(host);
    }

    @Override
    public Host getHostForContainer(int containerId) {
        Map<Integer, Host> mapping = getCurrentData();
        return mapping.get(containerId);
    }

    private Map<Integer, Host> getCurrentData() {
        return (Map<Integer, Host>) SerializationUtils.deserialize(segContainerHostMapping.getCurrentData().getData());
    }

    @Override
    public Integer getContainerCount() {
        return SEGMENT_CONTAINER_COUNT;
    }
}
