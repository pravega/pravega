/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package io.pravega.server.controller.service.store.client;

import java.util.Optional;

/**
 * Configuration of the metadata store client.
 */
public interface StoreClientConfig {
    /**
     * Fetches the type of the store the client connects to.
     * @return The type of the store the client connects to.
     */
    StoreType getStoreType();

    /**
     * Fetches whether the ZK base store is enabled, and its configuration if it is enabled.
     * @return Whether the ZK base store is enabled, and its configuration if it is enabled.
     */
    Optional<ZKClientConfig> getZkClientConfig();
}
