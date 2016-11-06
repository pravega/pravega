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

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class InMemoryHostStore implements HostControllerStore {
    private final Map<Host, Set<Integer>> hostContainerMap;


    public InMemoryHostStore(Map<Host, Set<Integer>> hostContainerMap) {
        this.hostContainerMap = hostContainerMap;
    }

    @Override
    public Set<Host> getHosts() {
        return Collections.unmodifiableSet(hostContainerMap.keySet());
    }

    @Override
    public Set<Integer> getContainersForHost(Host host) {
        if (hostContainerMap.containsKey(host)) {
            return Collections.unmodifiableSet(hostContainerMap.get(host));
        } else {
            throw new HostNotFoundException(host);
        }
    }

    @Override
    public Host getHostForContainer(int containerId) {
        Optional<Host> hosts = hostContainerMap.entrySet().stream()
                .filter(x -> x.getValue().contains(containerId)).map(x -> x.getKey()).findAny();
        if (hosts.isPresent()) {
            return hosts.get();
        } else {
            throw new ContainerNotFoundException(containerId);
        }
    }

    @Override
    public Integer getContainerCount() {
        return hostContainerMap.values().stream().flatMap(f -> f.stream()).collect(Collectors.toList()).size();
    }
}
