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

public interface HealthConfig {

    /**
     * A flag used to determine if the {@link HealthConfig} object is logically 'empty'.
     *
     * @return Whether or not the {@link HealthConfig} is empty.
     */
    boolean isEmpty();

    /**
     * A reconcile method makes it clear that a {@link HealthConfig} should provide a way for the {@link HealthComponent}
     * objects defined to properly registered by the {@link ContributorRegistry}.
     *
     * @param registry The {@link ContributorRegistry} to apply this reconciliation on.
     */
    void reconcile(ContributorRegistry registry);
}
