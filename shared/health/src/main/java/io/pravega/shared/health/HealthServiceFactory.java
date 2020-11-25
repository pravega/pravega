/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.shared.health;

import io.pravega.shared.health.impl.HealthConfigImpl;
import io.pravega.shared.health.impl.HealthServiceImpl;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

public class HealthServiceFactory implements AutoCloseable {
    private final HealthConfig config;
    private final AtomicBoolean closed;

    public HealthServiceFactory() {
        this(Optional.of(HealthConfigImpl.builder().empty()));
    }

    public HealthServiceFactory(Optional<HealthConfig> config) {
        this.config = config.orElse(HealthConfigImpl.builder().empty());
        this.closed = new AtomicBoolean();
    }

    public HealthService createHealthService(boolean start) {
        HealthService service = new HealthServiceImpl(config);
        if (start) {
            service.daemon().start();
        }
        return service;
    }

    @Override
    public void close() {
        this.closed.set(true);
    }
}
