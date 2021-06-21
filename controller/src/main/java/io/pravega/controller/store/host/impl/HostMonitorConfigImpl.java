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
package io.pravega.controller.store.host.impl;

import io.pravega.common.Exceptions;
import io.pravega.common.cluster.Host;
import io.pravega.controller.store.host.HostMonitorConfig;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import lombok.Builder;
import lombok.Getter;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Host monitor config.
 */
@Getter
public class HostMonitorConfigImpl implements HostMonitorConfig {
    private final boolean hostMonitorEnabled;
    private final int hostMonitorMinRebalanceInterval;
    private final int containerCount;
    private final Map<Host, Set<Integer>> hostContainerMap;

    @Builder
    HostMonitorConfigImpl(final boolean hostMonitorEnabled,
                          final int hostMonitorMinRebalanceInterval,
                          final int containerCount,
                          final Map<Host, Set<Integer>> hostContainerMap) {
        Exceptions.checkArgument(hostMonitorMinRebalanceInterval > 0, "hostMonitorMinRebalanceInterval",
                "Should be positive integer");
        Preconditions.checkArgument(containerCount > 0, "containerCount should be positive integer");
        if (!hostMonitorEnabled) {
            Preconditions.checkNotNull(hostContainerMap, "hostContainerMap");
            int containerCountInMap = hostContainerMap.values().stream().map(x -> x.size()).reduce(0, (x, y) -> x + y);
            Preconditions.checkArgument(containerCount == containerCountInMap,
                    "containerCount should equal the containers present in hostContainerMap");
        }
        this.hostMonitorEnabled = hostMonitorEnabled;
        this.hostMonitorMinRebalanceInterval = hostMonitorMinRebalanceInterval;
        this.containerCount = containerCount;
        this.hostContainerMap = hostContainerMap;
    }

    /**
     * This method should only be used for test purposes where the segment store service is either mocked. or
     * started at port 12345 on local host. For any other purposes, it is best to use HostMonitorConfigImpl builder.
     *
     * @return HostMonitorConfig with dummy values.
     */
    @VisibleForTesting
    public static HostMonitorConfig dummyConfig() {
        return new HostMonitorConfigImpl(false, 10, 4, getHostContainerMap("localhost", 12345, 4));
    }

    public static Map<Host, Set<Integer>> getHostContainerMap(String host, int port, int containerCount) {
        Exceptions.checkNotNullOrEmpty(host, "host");
        Preconditions.checkArgument(port > 0, "port");
        Preconditions.checkArgument(containerCount > 0, "containerCount");
        Map<Host, Set<Integer>> hostContainerMap = new HashMap<>();
        hostContainerMap.put(new Host(host, port, null), IntStream.range(0, containerCount).boxed().collect(Collectors.toSet()));
        return hostContainerMap;
    }
}
