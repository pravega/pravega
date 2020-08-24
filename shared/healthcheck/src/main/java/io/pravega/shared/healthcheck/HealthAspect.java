/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.shared.healthcheck;

import com.google.common.base.Preconditions;

import java.util.Collection;
import java.util.Optional;
import java.util.function.Function;

/**
 * HealthAspect is Pravega's notion of health-check on top of individual HealthUnit.
 *
 * For a highly distributed system such as Pravega, the failure of one or more components is completely expected or
 * sometimes even designed, so in addition to the health-check of individual HealthUnit,
 * we have to aggregate all the health-check results from the aspect to determine the healthiness of the aspect.
 *
 * For example, during a scaling-up period, we may see some Segment Containers being shut down while some other
 * being created. We have to collect the HealthInfo from all the Segment Containers (HealthUnit) in order to determine
 * the healthiness of the overall Segment Container aspect.
 */
public enum HealthAspect {

    SYSTEM("System", healthInfos -> {
        return HealthInfoAggregationRules.singleOrNone(healthInfos);
    }),
    CONTROLLER("Controller", healthInfos -> {
        return HealthInfoAggregationRules.majority(healthInfos);
    }),
    SEGMENT_CONTAINER("Segment Container", healthInfos -> {
        return HealthInfoAggregationRules.majority(healthInfos);
    }),
    CACHE("Cache Manager", healthInfos -> {
        return HealthInfoAggregationRules.oneVeto(healthInfos);
    }),
    LONG_TERM_STORAGE("Long Term Storage", healthInfos -> {
        return HealthInfoAggregationRules.singleOrNone(healthInfos);
    }),
    METRICS("Metrics", healthInfos -> {
        return HealthInfoAggregationRules.singleOrNone(healthInfos);
    });

    private final String name;
    private final Function<Collection<HealthInfo>, Optional<HealthInfo>> aspectAggregationRule;

    /**
     *
     * @param name - the name of the aspect
     * @param aspectAggregationRule - the rule to determine aspect level healthiness
     */
    HealthAspect(String name, Function<Collection<HealthInfo>, Optional<HealthInfo>> aspectAggregationRule) {
        Preconditions.checkArgument(aspectAggregationRule != null, "Aspect Aggregation Rule cannot be null");
        this.name = name;
        this.aspectAggregationRule = aspectAggregationRule;
    }

    /**
     * Get the HealthAspect name.
     *
     * @return the HealthAspect name
     */
    public String getName() {
        return this.name;
    }

    /**
     * Get the rule for the aggregation of all the HealthInfo from the HealthAspect.
     *
     * @return the Function to aggregate all HealthInfo from the HealthAspect
     */
    public Function<Collection<HealthInfo>, Optional<HealthInfo>> getAspectAggregationRule() {
        return this.aspectAggregationRule;
    }
}
