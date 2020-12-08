/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.shared.health;

import io.pravega.shared.health.impl.HealthComponent;

import java.util.List;
import java.util.Map;

/**
 * The {@link HealthEndpoint} interface defines all the types of requests a {@link HealthService} should expect to serve.
 */
public interface HealthEndpoint {

    /**
     * Provides the name of the default {@link HealthContributor} that all other {@link HealthContributor} objects are
     * reachable from.
     *
     * @return The name/id of the default {@link HealthContributor}.
     */
    default String getDefaultContributorName() {
        return ContributorRegistry.DEFAULT_CONTRIBUTOR_NAME;
    }

    /**
     * Provides the {@link Status} of the {@link HealthContributor} with name {@link #getDefaultContributorName()}.
     *
     * @return The {@link Status} of the default {@link HealthContributor}.
     */
    default Status getStatus() {
        return getStatus(getDefaultContributorName());
    }

    /**
     * Fetches the {@link Status} of the {@link HealthContributor} mapped to by 'id'.
     *
     * @param id The id of some {@link HealthContributor} to search for.
     * @return The current {@link Status} of the {@link HealthContributor}.
     */
    Status getStatus(String id);

    /**
     * Fetches the {@link Status} of the 'root' {@link HealthContributor}.
     *
     * @return The current {@link Status} of the 'root' {@link HealthContributor}.
     */
    default boolean isReady() {
        return isReady(getDefaultContributorName());
    }

    /**
     * Determine if some {@link HealthContributor} object is currently in a 'ready' state.
     *
     * @param id The id of some {@link HealthContributor} to search for.
     * @return The 'readiness' result.
     */
    boolean isReady(String id);

    /**
     * Determine if the root {@link HealthContributor} object is currently in an 'alive' state.
     *
     * @return The 'liveness' result.
     */
    default boolean isAlive() {
        return isAlive(getDefaultContributorName());
    }

    /**
     * Determine if some {@link HealthContributor} object is currently in an 'alive' state.
     * @param id The id of some {@link HealthContributor} to search for.
     *
     * @return The 'liveness' result.
     */
    boolean isAlive(String id);

    /**
     * Fetches the details from the root {@link HealthContributor}. This will always return a {@link Map}
     * object with no entries.
     *
     * @return The {@link Map} of details results.
     */
    default Map<String, Object> getDetails() {
        return getDetails(getDefaultContributorName());
    }

    /**
     * Fetches the results from the list of {@link java.util.function.Supplier} provided during {@link HealthContributor}
     * construction. This operation is essentially a NOP on {@link HealthComponent} objects -- a map with
     * with no entries will always be returned.
     *
     * @param id The id of some {@link HealthContributor} to search for.
     * @return The {@link Map} of details results.
     */
    Map<String, Object> getDetails(String id);

    /**
     * Calls {@link HealthEndpoint#getHealth(String, boolean)} with a false value and forwards the 'id' {@link String}.
     * @param id  The id/name of the {@link HealthComponent} to check the {@link Health} of.
     * @return The {@link Health} object of the {@link HealthContributor}.
     */
    default Health getHealth(String id) {
        return getHealth(id, false);
    }

    default Health getHealth(boolean includeDetails) {
        return getHealth(getDefaultContributorName(), includeDetails);
    }

    /**
     * Similar to a {@link HealthContributor}, a {@link HealthService} should also provide some way to access the {@link Health}
     * of the service. The difference is a {@link HealthService} is concerned with one or many {@link HealthComponent},
     * where as a {@link HealthContributor} should just be concerned with it's own {@link Health}.
     *
     * @param id  The id/name of the {@link HealthComponent} to check the {@link Health} of.
     * @param includeDetails Whether or not to include detailed information provided by said {@link HealthComponent}.
     * @return The {@link Health} object of the {@link HealthContributor}.
     */
    Health getHealth(String id, boolean includeDetails);

    /**
     * Retrieve the list of {@link HealthContributor} names that are used to determine the {@link Health} result
     * for the root {@link HealthContributor}.
     *
     * @return The list of names (ids) that the root {@link HealthContributor} relies on.
     */
    default List<String> getDependencies() {
        return getDependencies(getDefaultContributorName());
    }

    /**
     * Retrieve the list of {@link HealthContributor} names that are used to determine the {@link Health} result
     * for a particular {@link HealthContributor} mapped by 'id'.
     *
     * @param id The id of the {@link HealthContributor} to request from the {@link ContributorRegistry}.
     * @return The list of names (ids) that the {@link HealthContributor} relies on.
     */
    List<String> getDependencies(String id);
}
