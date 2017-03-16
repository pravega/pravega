/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package com.emc.pravega.controller.store.host;

import com.emc.pravega.common.Exceptions;
import com.google.common.annotations.VisibleForTesting;
import lombok.Builder;
import lombok.Getter;

/**
 * Host monitor config.
 */
@Getter
public class HostMonitorConfig {
    private final boolean hostMonitorEnabled;
    private final int hostMonitorMinRebalanceInterval;
    private final String sssHost;
    private final int sssPort;
    private final int containerCount;

    @Builder
    public HostMonitorConfig(final boolean hostMonitorEnabled,
                             final int hostMonitorMinRebalanceInterval,
                             final String sssHost,
                             final int sssPort,
                             final int containerCount) {
        Exceptions.checkArgument(hostMonitorMinRebalanceInterval > 0, "hostMonitorMinRebalanceInterval",
                "Should be positive integer");
        if (!hostMonitorEnabled) {
            Exceptions.checkNotNullOrEmpty(sssHost, "ssshost");
            Exceptions.checkArgument(sssPort > 0, "sssPort", "Should be positive integer");
            Exceptions.checkArgument(containerCount > 0, "containerCount", "Should be positive integer");
        }
        this.hostMonitorEnabled = hostMonitorEnabled;
        this.hostMonitorMinRebalanceInterval = hostMonitorMinRebalanceInterval;
        this.sssHost = sssHost;
        this.sssPort = sssPort;
        this.containerCount = containerCount;
    }

    @VisibleForTesting
    public static HostMonitorConfig defaultConfig() {
        return new HostMonitorConfig(false, 10, "localhost", 12345, 4);
    }
}
