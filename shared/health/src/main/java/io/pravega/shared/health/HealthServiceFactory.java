/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.shared.health;

import com.google.common.base.Preconditions;
import io.pravega.shared.health.impl.HealthConfigImpl;
import io.pravega.shared.health.impl.HealthServiceImpl;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Provides instances of a {@link HealthService} and optionally starts the {@link HealthServiceUpdater}.
 */
public class HealthServiceFactory implements AutoCloseable {
    private final HealthConfig config;
    private final AtomicBoolean closed;

    /**
     * Creates a new instances of the {@link HealthServiceFactory} with an empty {@link HealthConfig}.
     */
    public HealthServiceFactory() {
        this(HealthConfigImpl.builder().empty());
    }

    /**
     * Creates a new instance of the {@link HealthServiceFactory} using a specified {@link HealthConfig}.
     * @param config The {@link HealthConfig} definition.
     */
    public HealthServiceFactory(HealthConfig config) {
        this.config = Objects.isNull(config) ? HealthConfigImpl.builder().empty() : config;
        this.closed = new AtomicBoolean();
    }

    /**
     * Provides an instance of the {@link HealthService} and may optionally start its {@link HealthServiceUpdater}.
     * @param start Defines whether or not to start its {@link  HealthServiceUpdater}.
     *
     * @return The created {@link HealthService} instance.
     */
    public HealthService createHealthService(boolean start) {
        Preconditions.checkState(!this.closed.get(), "HealthServiceFactory has already been closed.");
        HealthService service = new HealthServiceImpl(config);
        if (start) {
            service.getHealthServiceUpdater().startAsync();
        }
        return service;
    }

    /**
     * Closes the {@link HealthServiceFactory} instance, making it unable to create further {@link HealthService} instances.
     */
    @Override
    public void close() {
        this.closed.set(true);
    }
}
