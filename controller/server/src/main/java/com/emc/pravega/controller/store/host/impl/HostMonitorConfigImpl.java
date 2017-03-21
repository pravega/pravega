/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package com.emc.pravega.controller.store.host.impl;

import com.emc.pravega.common.Exceptions;
import com.emc.pravega.common.cluster.Host;
import com.emc.pravega.controller.store.host.HostMonitorConfig;
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
    private final Map<Host, Set<Integer>> hostContainerMap;

    @Builder
    HostMonitorConfigImpl(final boolean hostMonitorEnabled,
                          final int hostMonitorMinRebalanceInterval,
                          final Map<Host, Set<Integer>> hostContainerMap) {
        Exceptions.checkArgument(hostMonitorMinRebalanceInterval > 0, "hostMonitorMinRebalanceInterval",
                "Should be positive integer");
        if (!hostMonitorEnabled) {
            Preconditions.checkNotNull(hostContainerMap, "hostContainerMap");
        }
        this.hostMonitorEnabled = hostMonitorEnabled;
        this.hostMonitorMinRebalanceInterval = hostMonitorMinRebalanceInterval;
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
        Map<Host, Set<Integer>> hostContainerMap = new HashMap<>();
        hostContainerMap.put(new Host("localhost", 12345), IntStream.range(0, 4).boxed().collect(Collectors.toSet()));
        return new HostMonitorConfigImpl(false, 10, hostContainerMap);
    }
}
