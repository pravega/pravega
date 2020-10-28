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

/**
 * Holds the set of {@link HealthContributor} objects that will be tracked to determine
 * the overall state of some {@link HealthService}.
 */
public interface ContributorRegistry {

    /**
     * Registers the contributor to the registry.
     *
     * @param contributor The {@link HealthContributor} object to add to the registry.
     */
    void register(HealthContributor contributor);

    /**
     * Unregisters the contributor from the registry.
     *
     * @param contributor The {@link HealthContributor} object to remove from the registry.
     */
    void unregister(HealthContributor contributor);
}
