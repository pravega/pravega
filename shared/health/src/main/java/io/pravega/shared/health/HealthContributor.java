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
import java.util.Collections;
import java.util.Optional;

public interface HealthContributor {

    /**
     * The list of {@link HealthContributor} objects that this {@link HealthContributor} object depends on to determine
     * its own health.
     *
     * @return
     */
    default Collection<HealthContributor> contributors() {
        return Collections.emptyList();
    }

    /**
     * A {@link HealthContributor} may optionally have a parent, which represents a {@link HealthContributor} that
     * depends on the health status of this {@link HealthContributor}.
     *
     * @return The parent of this {@link HealthContributor} if it exists.
     */
    default Optional<HealthContributor> parent() {
        return Optional.empty();
    }

    /**
     * From an abstract view, a {@link HealthContributor} is anything that has an impact on the health of the system.
     * As such it should provide a window into it's current state, I.E some {@link Health} object.
     *
     * @return The {@link Health} object produced by this {@link HealthContributor}.
     */
    default Health health() {
        return health(false);
    }

    /**
     * Logically equivalent to the above, but instead of just returning an encapsulated {@link Health} object,
     * we also include the {@link Details} associated with the request.
     *
     * @param details Flag to control inclusion of {@link Details}.
     * @return The {@link Health} object produced by this {@link HealthContributor}.
     */
    Health health(boolean details);

    /**
     * A human-readable identifier used for logging.
     * @return The name provided.
     */
    String getName();
}
