/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.store.client;

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
     * Fetches the maximum number of retries the client should make while attempting to connect to ZK servers.
     *
     * @return The maximum number of retries the client should make while attempting to connect to ZK servers.
     */
    int getMaxRetries();

    /**
     * Fetches the session timeout in milli seconds.
     * @return session timeout in milli seconds.
     */
    int getSessionTimeoutMs();

    /**
     * Fetches whether the connection is secure.
     */
    boolean isSecureConnectionToZooKeeper();

    /**
     * Fetches the trust store location.
     */
    String getTrustStorePath();

    /**
     * Fetches the trust store password path.
     */
    String getTrustStorePasswordPath();
}
