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
import io.pravega.shared.health.impl.HealthDaemonImpl;
import io.pravega.shared.health.impl.HealthServiceImpl;
import io.pravega.shared.health.impl.NullHealthService;

import java.util.concurrent.atomic.AtomicReference;

public class HealthProvider {

    public static final int HEALTH_DAEMON_INTERVAL_SECONDS = 10;

    /**
     * The singleton {@link HealthService} INSTANCE.
     */
    private static final AtomicReference<HealthService> HEALTH_SERVICE = new AtomicReference<>(new NullHealthService());

    private static final AtomicReference<HealthDaemon> HEALTH_DAEMON = new AtomicReference<>();

    public synchronized static void initialize() {
        initialize(HealthConfigImpl.builder().empty());
    }

    public synchronized static void initialize(HealthConfig config) {
        setHealthService(new HealthServiceImpl(config));
        setHealthDaemon(HEALTH_SERVICE.get());
    }

    public synchronized static HealthService getHealthService() {
        return HEALTH_SERVICE.get();
    }

    public synchronized static HealthDaemon getHealthDaemon() {
        return getHealthDaemon(true);
    }

    public synchronized static HealthDaemon getHealthDaemon(boolean start) {
        HealthDaemon daemon = HEALTH_DAEMON.get();
        if (start) {
            daemon.start();
        }
        return daemon;
    }

    private static void setHealthDaemon(HealthService service) {
        HEALTH_DAEMON.set(new HealthDaemonImpl(service, HEALTH_DAEMON_INTERVAL_SECONDS));
    }

    private static void setHealthService(HealthService service) {
        HEALTH_SERVICE.set(service);
    }
}
