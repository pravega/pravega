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

import io.pravega.shared.health.impl.HealthComponent;

import java.util.Map;

/**
 * The {@link HealthEndpoint} interface defines all the types of requests a {@link HealthService} should expect to serve.
 */
public interface HealthEndpoint {

    default String getDefaultContributorName() {
        return ContributorRegistry.DEFAULT_CONTRIBUTOR_NAME;
    }

    default String errorMessage() {
        return String.format("%s (root) contributor not found severe malfunction occurred.", getDefaultContributorName());
    }


    default Status status() throws ContributorNotFoundException {
        return status(getDefaultContributorName());
    }

    /**
     * Fetches the {@link Status} of the {@link HealthContributor} mapped to by {@param id}.
     *
     * @param id The id of some {@link HealthContributor} to search for.
     * @return The current {@link Status} of the {@link HealthContributor}.
     * @throws ContributorNotFoundException If a contributor does not map to {@param id}.
     */
    Status status(String id) throws ContributorNotFoundException;

    default boolean readiness() {
        try {
            return readiness(getDefaultContributorName());
        } catch (ContributorNotFoundException e) {
            throw new RuntimeException(errorMessage());
        }
    }

    /**
     * Determine if some {@link HealthContributor} object is currently in a 'ready' state.
     *
     * @param id The id of some {@link HealthContributor} to search for.
     * @return The 'readiness' result.
     * @throws ContributorNotFoundException If a contributor does not map to {@param id}.
     */
    boolean readiness(String id) throws ContributorNotFoundException;

    default boolean liveness() {
        try {
            return liveness(getDefaultContributorName());
        } catch (ContributorNotFoundException e) {
            throw new RuntimeException(errorMessage());
        }
    }

    /**
     * Determine if some {@link HealthContributor} object is currently in an 'alive' state.
     * @param id The id of some {@link HealthContributor} to search for.
     *
     * @return The 'liveness' result.
     * @throws ContributorNotFoundException If a contributor does not map to {@param id}.
     */
    boolean liveness(String id) throws ContributorNotFoundException;

    default Details details() {
        try {
            return details(getDefaultContributorName());
        } catch (ContributorNotFoundException e) {
            throw new RuntimeException(errorMessage());
        }
    }

    /**
     * Fetches the results from the list of {@link java.util.function.Supplier} provided during {@link HealthContributor}
     * construction.
     *
     * @param id The id of some {@link HealthContributor} to search for.
     * @return The {@link Map} of details results.
     * @throws ContributorNotFoundException If a contributor does not map to {@param id}.
     */
    Details details(String id) throws ContributorNotFoundException;

    default Health health(String id) throws ContributorNotFoundException {
        return health(id, false);
    }

    default Health health(boolean includeDetails) {
        try {
            return health(getDefaultContributorName(), includeDetails);
        } catch (ContributorNotFoundException e) {
            throw new RuntimeException(errorMessage());
        }
    }

    /**
     * Similar to a {@link HealthContributor}, a {@link HealthService} should also provide some way to access the {@link Health}
     * of the service. The difference is a {@link HealthService} is concerned with one or many {@link HealthComponent},
     * where as a {@link HealthContributor} should just be concerned with it's own {@link Health}.
     *
     * @param name  The name of the {@link HealthComponent} to check the {@link Health} of.
     * @param includeDetails Whether or not to include detailed information provided by said {@link HealthComponent}.
     * @return The {@link Health} object of the component.
     * @throws ContributorNotFoundException If a contributor does not map to {@param id}.
     */
    Health health(String name, boolean includeDetails) throws ContributorNotFoundException;
}
