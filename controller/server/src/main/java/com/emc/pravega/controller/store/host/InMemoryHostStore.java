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
import com.google.common.base.Preconditions;
import lombok.Synchronized;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

@Slf4j
public class InMemoryHostStore implements HostControllerStore {
    private Map<Host, Set<Integer>> hostContainerMap;

    /**
     * Creates an in memory based host store. The data is not persisted across restarts. Useful for dev and single node
     * deployment purposes.
     *
     * @param hostContainerMap      The initial Host to container ownership information.
     * @throws NullPointerException If hostContainerMap is null.
     */
    public InMemoryHostStore(Map<Host, Set<Integer>> hostContainerMap) {
        Preconditions.checkNotNull(hostContainerMap, "hostContainerMap");

        this.hostContainerMap = hostContainerMap;
    }

    @Override
    @Synchronized
    public Map<Host, Set<Integer>> getHostContainersMap() {
        return new HashMap<>(hostContainerMap);
    }

    @Override
    @Synchronized
    public void updateHostContainersMap(Map<Host, Set<Integer>> newMapping) {
        Preconditions.checkNotNull(newMapping, "newMapping");

        hostContainerMap = new HashMap<>(newMapping);
    }

    @Override
    @Synchronized
    public Host getHostForContainer(int containerId) {
        Optional<Host> host = hostContainerMap.entrySet().stream()
                .filter(x -> x.getValue().contains(containerId)).map(x -> x.getKey()).findAny();
        if (host.isPresent()) {
            log.debug("Found owning host: {} for containerId: {}", host.get(), containerId);
            return host.get();
        } else {
            throw new HostStoreException("Could not find host for container id: " + String.valueOf(containerId));
        }
    }

    @Override
    @Synchronized
    public int getContainerCount() {
        return (int) hostContainerMap.values().stream().flatMap(f -> f.stream()).count();
    }
}
