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

import io.pravega.shared.health.impl.HealthServiceImpl;

import java.util.concurrent.atomic.AtomicReference;

public class HealthProvider {
    /**
     * The singleton {@link HealthService} INSTANCE.
     */
    private static final AtomicReference<HealthService> HEALTH_SERVICE = new AtomicReference<>(new HealthServiceImpl(io.pravega.shared.health.impl.HealthConfigImpl.empty()));

    public synchronized static void initialize(HealthServer.HealthConfig config) {
        setHealthService(new HealthServiceImpl(config));
    }

    public synchronized static HealthService getHealthService() {
        return HEALTH_SERVICE.get();
    }

    private static void setHealthService(HealthService service) {
        HEALTH_SERVICE.set(service);
    }
}
