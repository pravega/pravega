/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package com.emc.pravega.service.monitor;

import lombok.Data;

import java.time.Duration;

@Data
public class AutoScalerConfig {
    static final AutoScalerConfig DEFAULT = new AutoScalerConfig(Duration.ofMinutes(20),
            Duration.ofMinutes(10), Duration.ofMinutes(10), Duration.ofMinutes(20));

    private final Duration cooldownPeriod;
    private final Duration muteDuration;
    private final Duration cacheCleanup;
    private final Duration cacheExpiry;
}
