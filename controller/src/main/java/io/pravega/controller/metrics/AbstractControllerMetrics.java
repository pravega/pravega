/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.metrics;

import io.pravega.shared.metrics.DynamicLogger;
import io.pravega.shared.metrics.MetricsProvider;
import io.pravega.shared.metrics.StatsLogger;

/**
 * Contains common metrics resources for child classes implementing metric reporting logic.
 */
public abstract class AbstractControllerMetrics {

    static final StatsLogger STATS_LOGGER = MetricsProvider.createStatsLogger("controller");
    static final DynamicLogger DYNAMIC_LOGGER = MetricsProvider.getDynamicLogger();

}
