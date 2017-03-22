/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package com.emc.pravega.controller.store.client;

/**
 * Configuration of Zookeeper's Curator Framework client.
 */
public interface ZKClientConfig {
    /**
     * Fetches the ZK server connection string.
     *
     * @return The ZK server connection string.
     */
    String getConnectionString();

    /**
     * Fetches the ZK client namespace.
     *
     * @return The ZK client namespace.
     */
    String getNamespace();

    /**
     * Fetches the amount of sleep time before first retry.
     *
     * @return The amount of sleep time before first retry.
     */
    int getInitialSleepInterval();

    /**
     * Fetches the maximum nnumber of retries the client should make while attempting to connect to ZK servers.
     *
     * @return The maximum nnumber of retries the client should make while attempting to connect to ZK servers.
     */
    int getMaxRetries();
}
