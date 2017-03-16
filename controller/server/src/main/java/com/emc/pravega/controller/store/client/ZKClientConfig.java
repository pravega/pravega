/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package com.emc.pravega.controller.store.client;

import com.emc.pravega.common.Exceptions;
import lombok.Builder;
import lombok.Getter;

/**
 * Zookeeper Curator framework client config.
 */
@Getter
public class ZKClientConfig {
    private String connectionString;
    private String namespace;
    private int initialSleepInterval;
    private int maxRetries;

    @Builder
    ZKClientConfig(final String connectionString,
                   final String namespace,
                   final int initialSleepInterval,
                   final int maxRetries) {
        Exceptions.checkNotNullOrEmpty(connectionString, "connectionString");
        Exceptions.checkNotNullOrEmpty(namespace, "namespace");
        Exceptions.checkArgument(initialSleepInterval > 0, "retryInterval", "Should be a positive integer");
        Exceptions.checkArgument(maxRetries > 0, "maxRetries", "Should be a positive integer");

        this.connectionString = connectionString;
        this.namespace = namespace;
        this.initialSleepInterval = initialSleepInterval;
        this.maxRetries = maxRetries;
    }
}
