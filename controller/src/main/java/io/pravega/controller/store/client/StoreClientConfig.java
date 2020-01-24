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
