/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.store.client.impl;

import io.pravega.common.Exceptions;
import io.pravega.controller.store.client.StoreClientConfig;
import io.pravega.controller.store.client.StoreType;
import io.pravega.controller.store.client.ZKClientConfig;
import com.google.common.base.Preconditions;
import lombok.Getter;
import lombok.ToString;

import java.util.Optional;

/**
 * Store client configuration.
 */
@ToString
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
    
    public static StoreClientConfig withPravegaTablesClient(ZKClientConfig zkClientConfig) {
        Preconditions.checkNotNull(zkClientConfig, "zkClientConfig");
        return new StoreClientConfigImpl(StoreType.PravegaTable, Optional.of(zkClientConfig));
    }
}
