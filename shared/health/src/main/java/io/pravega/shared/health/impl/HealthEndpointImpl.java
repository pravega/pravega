/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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
import java.util.Objects;
import java.util.stream.Collectors;

@Slf4j
public class HealthEndpointImpl implements HealthEndpoint {

    private final ContributorRegistry registry;

    HealthEndpointImpl(ContributorRegistry registry) {
        this.registry = registry;
    }

    @NonNull
    @Override
    public Health getHealth(String id, boolean includeDetails) {
        HealthContributor result = registry.get(id);
        if (Objects.isNull(result)) {
            throw new ContributorNotFoundException();
        }
        return result.getHealthSnapshot(includeDetails);
    }

    @Override
    public List<String> getDependencies(String id) {
        return getHealth(id, false).getChildren().stream()
                .map(child -> child.getName())
                .collect(Collectors.toList());
    }

    @Override
    public Status getStatus(String id) {
        return getHealth(id).getStatus();
    }

    @Override
    public boolean isReady(String id) {
        return getHealth(id).isReady();
    }

    @Override
    public boolean isAlive(String id) {
        return getHealth(id).isAlive();
    }

    @Override
    public Details getDetails(String id) {
        return getHealth(id, true).getDetails();
    }

}

