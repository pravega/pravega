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
 * The top level interface used to provide any and all health related information for a particular component
 * of Pravega. It holds the {@link ContributorRegistry} and provides the endpoint used to make health information
 * accessible to clients.
 *
 * A {@link HealthService} should provide four endpoints:
 *  * /health               A route providing the aggregate of the three routes listed below.
 *  * /health/readiness     Exposes the top level 'ready' status.
 *  * /health/liveness      Exposes the top level 'liveness' status.
 *  * /health/details       Exposes the aggregate {@link Health} details.
 */
public interface HealthService extends HealthServer {
    public final static String ROOT_COMPONENT_NAME = "root";

    /**
     * Registers the contributor to the default {@link HealthComponent} registry.
     *
     * @param contributor The {@link HealthContributor} object to add to the registry.
     */
    void register(HealthContributor contributor);

    /**
     * Registers the contributor to the registry.
     *
     * @param contributor The {@link HealthContributor} object to add to the registry.
     * @param parent      The {@link HealthComponent} the {@link HealthContributor} should map too. This means that the parent's
     *                    health will be predicated on this {@link HealthContributor}'s health.
     */
    void register(HealthContributor contributor, HealthComponent parent);

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

    Health health(boolean includeDetails);

    /**
     * Returns a {@link Collection} of all the components the {@link HealthService} is responsible for and observes.
     * @return
     */
    Collection<HealthComponent> components();
}
