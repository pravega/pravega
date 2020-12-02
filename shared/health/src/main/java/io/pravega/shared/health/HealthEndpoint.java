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

import java.util.List;
import java.util.Map;

/**
 * The {@link HealthEndpoint} interface defines all the types of requests a {@link HealthService} should expect to serve.
 */
public interface HealthEndpoint {

    default String getDefaultContributorName() {
        return ContributorRegistry.DEFAULT_CONTRIBUTOR_NAME;
    }

    default Status status() {
        return status(getDefaultContributorName());
    }

    /**
     * Fetches the {@link Status} of the {@link HealthContributor} mapped to by 'id'.
     *
     * @param id The id of some {@link HealthContributor} to search for.
     * @return The current {@link Status} of the {@link HealthContributor}.
     */
    Status status(String id);

    default boolean readiness() {
        return readiness(getDefaultContributorName());
    }

    /**
     * Determine if some {@link HealthContributor} object is currently in a 'ready' state.
     *
     * @param id The id of some {@link HealthContributor} to search for.
     * @return The 'readiness' result.
     */
    boolean readiness(String id);

    default boolean liveness() {
        return liveness(getDefaultContributorName());
    }

    /**
     * Determine if some {@link HealthContributor} object is currently in an 'alive' state.
     * @param id The id of some {@link HealthContributor} to search for.
     *
     * @return The 'liveness' result.
     */
    boolean liveness(String id);

    default Details details() {
        return details(getDefaultContributorName());
    }

    /**
     * Fetches the results from the list of {@link java.util.function.Supplier} provided during {@link HealthContributor}
     * construction.
     *
     * @param id The id of some {@link HealthContributor} to search for.
     * @return The {@link Map} of details results.
     */
    Details details(String id);

    default Health health(String id) {
        return health(id, false);
    }

    default Health health(boolean includeDetails) {
        return health(getDefaultContributorName(), includeDetails);
    }

    /**
     * Similar to a {@link HealthContributor}, a {@link HealthService} should also provide some way to access the {@link Health}
     * of the service. The difference is a {@link HealthService} is concerned with one or many {@link HealthComponent},
     * where as a {@link HealthContributor} should just be concerned with it's own {@link Health}.
     *
     * @param name  The name of the {@link HealthComponent} to check the {@link Health} of.
     * @param includeDetails Whether or not to include detailed information provided by said {@link HealthComponent}.
     * @return The {@link Health} object of the component.
     */
    Health health(String name, boolean includeDetails);

    default List<String> dependencies() {
        return dependencies(getDefaultContributorName());
    }

    /**
     * Retrieve the list of {@link HealthContributor} names that are used to determine the {@link Health} result
     * for a particular {@link HealthContributor} mapped by 'id'.
     *
     * @param id The id of the {@link HealthContributor} to request from the {@link ContributorRegistry}.
     * @return The list of names (ids) that the {@link HealthContributor} relies on.
     */
    List<String> dependencies(String id);
}
