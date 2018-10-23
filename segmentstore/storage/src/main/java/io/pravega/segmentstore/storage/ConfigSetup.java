/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.storage;

import io.pravega.common.util.ConfigBuilder;
import java.util.function.Supplier;

/**
 * Interface used to pass the configuration to storage plugin implementation.
 */
public interface ConfigSetup {
    /**
     * Gets the Configuration with specified constructor from the ServiceBuilder's config.
     *
     * @param builderConstructor A Supplier that creates a ConfigBuilder for the desired configuration type.
     * @param <T>                The type of the Configuration to instantiate.
     */
    <T> T getConfig(Supplier<? extends ConfigBuilder<T>> builderConstructor);
}
