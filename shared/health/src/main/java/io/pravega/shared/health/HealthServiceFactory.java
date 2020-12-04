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

    public HealthServiceFactory() {
        this(HealthConfigImpl.builder().empty());
    }

    public HealthServiceFactory(HealthConfig config) {
        this.config = Objects.isNull(config) ? HealthConfigImpl.builder().empty() : config;
        this.closed = new AtomicBoolean();
    }

    public HealthService createHealthService(boolean start) {
        HealthService service = new HealthServiceImpl(config);
        if (start) {
            service.getHealthServiceUpdater().startAsync();
        }
        return service;
    }

    @Override
    public void close() {
        this.closed.set(true);
    }
}
