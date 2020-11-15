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

import java.util.Collection;

/**
 * Holds the set of {@link HealthContributor} objects that will be tracked to determine
 * the overall state of some {@link HealthService}.
 */
public interface ContributorRegistry extends Registry<HealthContributor> {

    /**
     * Registers the contributor to the default {@link HealthContributor} registry.
     *
     * @param contributor The {@link HealthContributor} object to add to the registry.
     */
    void register(HealthContributor contributor);

    /**
     * Registers the contributor to the registry.
     *
     * @param contributor The {@link HealthContributor} object to add to the registry.
     * @param parent      The {@link HealthContributor} the {@link HealthContributor} should map too. This means that the parent's
     *                    health will be predicated on this {@link HealthContributor}'s health.
     */
    void register(HealthContributor contributor, HealthContributor parent);

    /**
     * Registers the contributor to the registry.
     *
     * @param contributor The {@link HealthContributor} object to add to the registry.
     * @param parent      A {@link String} that maps to some {@link HealthContributor} which the {@link HealthContributor}
     *                    should it self map too. This means that the parent's health will be predicated on
     *                    this {@link HealthContributor}'s health.
     */
    void register(HealthContributor contributor, String parent);

    /**
     * Removes the {@link HealthContributor} from the registry.
     *
     * @param contributor The {@link HealthContributor} object to remove.
     */
    void unregister(HealthContributor contributor);

    /**
     * Removes the {@link HealthContributor} associated by some {@link String} from the registry.
     *
     * @param contributor The {@link String} representing some {@link HealthContributor} object to remove.
     */
    void unregister(String contributor);

    /**
     * Provides a {@link Collection} of all the {@link HealthContributor} objects belonging to this registry.
     * @return
     */
    Collection<HealthContributor> contributors();

    /**
     * Provides a {@link Collection} of all the {@link HealthContributor} objects belonging to this registry.
     *
     * @param name The {@link String} used to query which {@link HealthContributor} to gather the {@link HealthContributor}
     *             objects from.
     * @return
     */
    Collection<HealthContributor> contributors(String name);
}
