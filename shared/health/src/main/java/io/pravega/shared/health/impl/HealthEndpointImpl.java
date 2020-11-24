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

import io.pravega.shared.health.ContributorNotFoundException;
import io.pravega.shared.health.ContributorRegistry;
import io.pravega.shared.health.Details;
import io.pravega.shared.health.Health;
import io.pravega.shared.health.HealthContributor;
import io.pravega.shared.health.HealthEndpoint;
import io.pravega.shared.health.Status;
import lombok.extern.slf4j.Slf4j;

import java.util.Optional;

@Slf4j
public class HealthEndpointImpl implements HealthEndpoint {

    private final ContributorRegistry registry;

    HealthEndpointImpl(ContributorRegistry registry) {
        this.registry = registry;
    }

    @Override
    public Health health(String name, boolean includeDetails) throws ContributorNotFoundException {
        Optional<HealthContributor> result = registry.get();
        if (result.isEmpty()) {
            throw new ContributorNotFoundException();
        }
        return result.get().health(includeDetails);
    }

    @Override
    public Status status(String id) throws ContributorNotFoundException {
        return health(id).getStatus();
    }

    @Override
    public boolean readiness(String id) throws ContributorNotFoundException {
        return health(id).isReady();
    }

    @Override
    public boolean liveness(String id) throws ContributorNotFoundException {
        return health(id).isAlive();
    }

    @Override
    public Details details(String id) throws ContributorNotFoundException {
        return health(id, true).getDetails();
    }

}

