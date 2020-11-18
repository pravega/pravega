/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.shared.health.impl;

import io.pravega.shared.health.ContributorRegistry;
import io.pravega.shared.health.Health;
import io.pravega.shared.health.HealthEndpoint;
import io.pravega.shared.health.HealthService;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class NullHealthService implements HealthService  {

    public Health health(String name, boolean includeDetails) {
        log.warn("Requesting Health of HealthContributor::{} from NullHealthService", name);
        return null;
    }

    public Health health(boolean includeDetails) {
        return health(null, includeDetails);
    }

    @Override
    public ContributorRegistry registry() {
        log.warn("Requesting the ContributorRegistry belonging to a NullHealthService");
        return null;
    }

    @Override
    public HealthEndpoint endpoint() {
        log.warn("Requesting the HealthEndpoint belonging to a NullHealthService");
        return null;
    }

    @Override
    public void clear() {
        log.warn("Clearing a NullHealthService.");
    }

}
