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

import java.util.Collection;

@FunctionalInterface
public interface StatusAggregator {

    /**
     * Reduces a {@link Collection} of {@link Status} objects into a single {@link Status} object
     * representing the overall health of the given {@link io.pravega.shared.health.impl.CompositeHealthContributor}.
     *
     * @param statuses The {@link Collection} of {@link Status}.
     * @return
     */
    Status aggregate(Collection<Status> statuses);
}