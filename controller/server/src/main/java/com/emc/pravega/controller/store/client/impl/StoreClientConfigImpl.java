/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package com.emc.pravega.controller.store.client.impl;

import com.emc.pravega.common.Exceptions;
import com.emc.pravega.controller.store.client.StoreClientConfig;
import com.emc.pravega.controller.store.client.StoreType;
import com.emc.pravega.controller.store.client.ZKClientConfig;
import com.google.common.base.Preconditions;
import lombok.Getter;

import java.util.Optional;

/**
 * Store client configuration.
 */
@Getter
public class StoreClientConfigImpl implements StoreClientConfig {

    private final StoreType storeType;
    private final Optional<ZKClientConfig> zkClientConfig;

    StoreClientConfigImpl(final StoreType storeType, final Optional<ZKClientConfig> zkClientConfig) {
        Preconditions.checkNotNull(storeType, "storeType");
        Preconditions.checkNotNull(zkClientConfig, "zkClientConfig");
        if (storeType == StoreType.Zookeeper) {
            Exceptions.checkArgument(zkClientConfig.isPresent(), "zkClientConfig", "Should be non-empty");
        }

        this.storeType = storeType;
        this.zkClientConfig = zkClientConfig;
    }

    public static StoreClientConfig withInMemoryClient() {
        return new StoreClientConfigImpl(StoreType.InMemory, Optional.<ZKClientConfig>empty());
    }

    public static StoreClientConfig withZKClient(ZKClientConfig zkClientConfig) {
        Preconditions.checkNotNull(zkClientConfig, "zkClientConfig");
        return new StoreClientConfigImpl(StoreType.Zookeeper, Optional.of(zkClientConfig));
    }
}
