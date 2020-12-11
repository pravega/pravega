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
import io.pravega.shared.health.Health;
import io.pravega.shared.health.HealthContributor;
import io.pravega.shared.health.HealthEndpoint;
import io.pravega.shared.health.Status;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

@Slf4j
public class HealthEndpointImpl implements HealthEndpoint {

    private final ContributorRegistry registry;

    /**
     * Creates a new instance of the {@link HealthEndpointImpl} class.
     * @param registry The {@link ContributorRegistry} to query.
     */
    HealthEndpointImpl(ContributorRegistry registry) {
        this.registry = registry;
    }

    /**
     * Validates that the {@link HealthContributor} exists and requests it's {@link Health}.
     *
     * @param id  The id/name of the {@link HealthComponent} to check the {@link Health} of.
     * @param includeDetails Whether or not to include detailed information provided by said {@link HealthComponent}.
     * @return The {@link Health} result of the {@link HealthContributor} with name 'id'.
     */
    @NonNull
    @Override
    public Health getHealth(String id, boolean includeDetails) {
        HealthContributor result = registry.get(id);
        if (Objects.isNull(result)) {
            throw new ContributorNotFoundException();
        }
        return result.getHealthSnapshot(includeDetails);
    }

    /**
     * Provides a {@link List} of ids (names) that the {@link HealthContributor} depends on.
     *
     * @param id The id of the {@link HealthContributor} to request from the {@link ContributorRegistry}.
     * @return The {@link List} of dependencies.
     */
    @Override
    public List<String> getDependencies(String id) {
        return getHealth(id, true).getChildren().stream()
                .map(child -> child.getName())
                .collect(Collectors.toList());
    }

    /**
     * Provides the health {@link Status} for the {@link HealthContributor} with name 'id'.
     *
     * @param id The id of some {@link HealthContributor} to search for.
     * @return The {@link Status} result.
     */
    @Override
    public Status getStatus(String id) {
        return getHealth(id).getStatus();
    }

    /**
     * Provides the readiness status of the {@link HealthContributor} with name 'id'.
     *
     * @param id The id of some {@link HealthContributor} to search for.
     * @return The readiness result.
     */
    @Override
    public boolean isReady(String id) {
        return getHealth(id).isReady();
    }

    /**
     * Provides the liveness status of the {@link HealthContributor} with name 'id'.
     *
     * @param id The id of some {@link HealthContributor} to search for.
     * @return The liveness result.
     */
    @Override
    public boolean isAlive(String id) {
        return getHealth(id).isAlive();
    }

    /**
     * Provides a {@link Map} of the {@link Objects} used to create a {@link  HealthContributor} details response.
     *
     * @param id The id of some {@link HealthContributor} to search for.
     * @return The {@link Map}.
     */
    @Override
    public Map<String, Object> getDetails(String id) {
        return getHealth(id, true).getDetails();
    }

}

