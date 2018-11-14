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

import io.pravega.common.cluster.Host;
import io.pravega.shared.metrics.DynamicLogger;
import io.pravega.shared.metrics.MetricsProvider;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static io.pravega.shared.MetricsNames.CONTAINER_FAILURES;
import static io.pravega.shared.MetricsNames.CONTAINER_LOCATION;
import static io.pravega.shared.MetricsNames.SEGMENT_STORE_HOST_CONTAINER_COUNT;
import static io.pravega.shared.MetricsNames.SEGMENT_STORE_HOST_NUMBER;
import static io.pravega.shared.MetricsNames.SEGMENT_STORE_HOST_FAILURES;
import static io.pravega.shared.MetricsNames.nameFromContainer;
import static io.pravega.shared.MetricsNames.nameFromHost;

/**
 * Class to encapsulate the logic to report Controller metrics for Segment Store hosts and Container lifecycle.
 */
public class HostContainerMetrics {

    private static final DynamicLogger DYNAMIC_LOGGER = MetricsProvider.getDynamicLogger();

    /**
     * This method reports the number available Segment Store hosts managing Containers, as well as the number of
     * Containers assigned to each host. It also reports the relationship between Segment Store hosts and Container ids,
     * to make it easier for administrators to infer in which node a certain Container is running. Moreover, this method
     * also reports failures for hosts and Containers; we consider a failure the situation in which a host in present
     * in the oldMapping, but not present in newMapping.
     *
     * @param oldMapping    Previous host to Container relationships.
     * @param newMapping    Updated host to Container relationships.
     */
    public void updateHostContainerMetrics(Map<Host, Set<Integer>> oldMapping, Map<Host, Set<Integer>> newMapping) {
        // Report current number of Segment Store hosts with containers.
        DYNAMIC_LOGGER.reportGaugeValue(SEGMENT_STORE_HOST_NUMBER, newMapping.keySet().size());

        // Report the host/container relationships and the number of containers per host.
        newMapping.keySet().forEach(host -> reportContainerLocationAndCount(host, newMapping.get(host)));
        if (oldMapping == null) {
            return;
        }

        // Report Segment Store node and container failures related to node failures. We consider a failure the event
        // that the old host/container map contains hosts not existing in the new map.
        Set<Host> workingNodes = new HashSet<>(oldMapping.keySet());
        if (workingNodes.retainAll(newMapping.keySet())) {
            oldMapping.keySet().stream()
                               .filter(host -> !workingNodes.contains(host))
                               .forEach(failedHost -> {
                                   reportHostFailures(failedHost);
                                   reportContainerFailovers(oldMapping.get(failedHost));
                               });
        }
    }

    private void reportContainerLocationAndCount(Host host, Set<Integer> containerIds) {
        for (Integer containerId: containerIds) {
            DYNAMIC_LOGGER.reportGaugeValue(nameFromHost(CONTAINER_LOCATION, host.getHostId()), containerId);
        }

        DYNAMIC_LOGGER.reportGaugeValue(nameFromHost(SEGMENT_STORE_HOST_CONTAINER_COUNT, host.getHostId()), containerIds.size());
    }

    private void reportHostFailures(Host failedHost) {
        DYNAMIC_LOGGER.incCounterValue(SEGMENT_STORE_HOST_FAILURES, 1);
        DYNAMIC_LOGGER.incCounterValue(nameFromHost(SEGMENT_STORE_HOST_FAILURES, failedHost.getHostId()), 1);
    }

    private void reportContainerFailovers(Set<Integer> failedContainers) {
        DYNAMIC_LOGGER.incCounterValue(CONTAINER_FAILURES, failedContainers.size());
        for (Integer containerId: failedContainers) {
            DYNAMIC_LOGGER.incCounterValue(nameFromContainer(CONTAINER_FAILURES, containerId), 1);
        }
    }
}
