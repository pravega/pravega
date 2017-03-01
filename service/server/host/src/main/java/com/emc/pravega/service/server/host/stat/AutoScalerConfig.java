/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package com.emc.pravega.service.server.host.stat;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.net.URI;
import java.time.Duration;

@Data
@AllArgsConstructor
public class AutoScalerConfig {
    static final Duration DEFAULT_COOL_DOWN = Duration.ofMinutes(20);
    static final Duration DEFAULT_MUTE_DURATION = Duration.ofMinutes(20);
    static final Duration DEFAULT_CACHE_CLEANUP = Duration.ofMinutes(20);
    static final Duration DEFAULT_CACHE_EXPIRY = Duration.ofMinutes(20);

    private final Duration cooldownPeriod;
    private final Duration muteDuration;
    private final Duration cacheCleanup;
    private final Duration cacheExpiry;

    private final String internalScope;
    private final String internalStream;
    private final URI conrollerUri;

    AutoScalerConfig(String scope, String requestStream, URI controllerUri) {
        this(DEFAULT_COOL_DOWN, DEFAULT_MUTE_DURATION, DEFAULT_CACHE_CLEANUP, DEFAULT_CACHE_EXPIRY,
                scope, requestStream, controllerUri);
    }
}
