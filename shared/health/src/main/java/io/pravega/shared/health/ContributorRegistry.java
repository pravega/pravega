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

import java.util.Collection;

/**
 * Holds the set of {@link HealthContributor} objects that will be tracked to determine
 * the overall state of some {@link HealthService}.
 */
public interface ContributorRegistry extends Registry<HealthContributor> {

    static final String DEFAULT_CONTRIBUTOR_NAME = "health-service";

    HealthContributor get();

    /**
     * Registers the component to the default {@link HealthContributor} registry.
     *
     * @param component The {@link HealthComponent} object to add to the registry.
     * @return The {@link HealthContributor} registered.
     */
    HealthContributor register(HealthComponent component);

    /**
     * Registers the contributor to the default {@link HealthContributor} registry.
     *
     * @param contributor The {@link HealthContributor} object to add to the registry.
     * @return The {@link HealthContributor} registered.
     */
    @Override
    HealthContributor register(HealthContributor contributor);

    /**
     * Registers the component to the registry.
     *
     * @param component The {@link HealthComponent} object to add to the registry.
     * @param parent      The {@link HealthComponent} the {@link HealthComponent} should map too. This means that the parent's
     *                    health will be predicated on this {@link HealthComponent}'s health.
     * @return The {@link HealthContributor} registered.
     */
    HealthContributor register(HealthComponent component, HealthComponent parent);

    /**
     * Registers the contributor to the registry.
     *
     * @param contributor The {@link HealthContributor} object to add to the registry.
     * @param parent      The {@link HealthComponent} the {@link HealthContributor} should map too. This means that the parent's
     *                    health will be predicated on this {@link HealthContributor}'s health.
     * @return The {@link HealthContributor} registered.
     */
    HealthContributor register(HealthContributor contributor, HealthComponent parent);

    /**
     * Registers the contributor to the registry.
     *
     * @param contributor The {@link HealthContributor} object to add to the registry.
     * @param parent      A {@link String} that maps to some {@link HealthContributor} which the {@link HealthContributor}
     *                    should it self map too. This means that the parent's health will be predicated on
     *                    this {@link HealthContributor}'s health.
     * @return The {@link HealthContributor} registered.
     */
    HealthContributor register(HealthContributor contributor, String parent);

    /**
     * Removes the {@link HealthContributor} from the registry.
     *
     * @param contributor The {@link HealthContributor} object to remove.
     * @return The {@link HealthContributor} removed.
     */
    @Override
    HealthContributor unregister(HealthContributor contributor);

    /**
     * Removes the {@link HealthContributor} associated by some {@link String} from the registry.
     *
     * @param contributor The {@link String} representing some {@link HealthContributor} object to remove.
     * @return The {@link HealthContributor} removed.
     */
    HealthContributor unregister(String contributor);

    /**
     * Provides a {@link Collection} of all the {@link HealthContributor} objects belonging to this registry.
     * @return A {@link Collection} of {@link HealthContributor} that some {@link HealthContributor} with id 'name' depends on.
     */
    Collection<HealthContributor> dependencies();

    /**
     * Provides a {@link Collection} of all the {@link HealthContributor} objects belonging to this registry.
     *
     * @param name The {@link String} used to query which {@link HealthContributor} to gather the {@link HealthContributor}
     *             objects from.
     * @return A {@link Collection} of {@link HealthContributor} that some {@link HealthContributor} with id 'name' depends on.
     */
    Collection<HealthContributor> dependencies(String name);

    /**
     * Provides a {@link Collection} of all the {@link HealthContributor} ids tracked by this {@link ContributorRegistry}.
     *
     * @return The {@link Collection}.
     */
    Collection<String> contributors();

    /**
     * Provides a {@link Collection} of all the {@link HealthComponent} ids tracked by this {@link ContributorRegistry}.
     *
     * @return The {@link Collection}.
     */
    Collection<String> components();
}
