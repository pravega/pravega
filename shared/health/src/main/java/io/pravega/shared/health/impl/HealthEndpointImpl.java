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
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

@Slf4j
public class HealthEndpointImpl implements HealthEndpoint {

    private final ContributorRegistry registry;

    HealthEndpointImpl(ContributorRegistry registry) {
        this.registry = registry;
    }

    @NonNull
    @Override
    public Health health(String name, boolean includeDetails) {
        Optional<HealthContributor> result = registry.get(name);
        if (result.isEmpty()) {
            throw new ContributorNotFoundException();
        }
        return result.get().health(includeDetails);
    }

    @Override
    public List<String> dependencies(String id) {
        return health(id, false).getChildren().stream()
                .map(child -> child.getName())
                .collect(Collectors.toList());
    }

    @Override
    public Status status(String id) {
        return health(id).getStatus();
    }

    @Override
    public boolean readiness(String id) {
        return health(id).isReady();
    }

    @Override
    public boolean liveness(String id) {
        return health(id).isAlive();
    }

    @Override
    public Details details(String id) {
        return health(id, true).getDetails();
    }

}

