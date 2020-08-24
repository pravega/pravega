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

import java.util.Collection;
import java.util.Optional;

/**
 * Utility class holds aggregation rules for health check.
 */
public class HealthInfoAggregationRules {

    /**
     * There must be at least half of the healthy HealthInfos for the overall to be considered healthy.
     *
     * @param healthInfos collection of HealthInfo
     * @return the overall HealthInfo determined by the majority rule
     */
    public static Optional<HealthInfo> majority(Collection<HealthInfo> healthInfos) {
        if (healthInfos.isEmpty()) {
            return Optional.empty();
        }

        int healthCount = 0;
        int unhealthCount = 0;
        StringBuilder details = new StringBuilder();

        for (HealthInfo info: healthInfos) {
            if (info.getStatus() == HealthInfo.Status.HEALTH) {
                healthCount++;
            } else {
                unhealthCount++;
            }
            details.append(info.getDetails());
            details.append("\n");

        }

        return Optional.of(new HealthInfo(unhealthCount > healthCount ? HealthInfo.Status.UNHEALTH : HealthInfo.Status.HEALTH, details.toString()));
    }

    /**
     * None or one HealthInfo is needed for the aggregation.
     *
     * @param healthInfos collection of HealInfo
     * @return the overall HealthInfo determined by the rule
     */
    public static Optional<HealthInfo> singleOrNone(Collection<HealthInfo> healthInfos) {

        if (healthInfos.isEmpty()) {
            return Optional.empty();
        } else {
            return Optional.of(healthInfos.iterator().next());
        }
    }

    /**
     * If there is one not healthy (unhealthy or unknown), then the overall is considered unhealthy.
     *
     * @param healthInfos collection of HealthInfo
     * @return the overall HealthInfo determined by the one veto rule.
     */
    public static Optional<HealthInfo> oneVeto(Collection<HealthInfo> healthInfos) {

        if (healthInfos.isEmpty()) {
            return Optional.empty();
        }

        StringBuilder details = new StringBuilder();
        HealthInfo.Status status = HealthInfo.Status.HEALTH;

        for (HealthInfo info: healthInfos) {
            if (info.getStatus() != HealthInfo.Status.HEALTH) {
                status = HealthInfo.Status.UNHEALTH;
            }
            details.append(info.getDetails());
            details.append("\n");
        }

        return Optional.of(new HealthInfo(status, details.toString()));
    }

}
