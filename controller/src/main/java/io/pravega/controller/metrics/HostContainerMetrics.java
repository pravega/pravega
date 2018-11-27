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
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static io.pravega.shared.MetricsNames.CONTAINER_FAILOVERS;
import static io.pravega.shared.MetricsNames.SEGMENT_STORE_HOST_CONTAINER_COUNT;
import static io.pravega.shared.MetricsNames.SEGMENT_STORE_HOST_FAILURES;
import static io.pravega.shared.MetricsNames.SEGMENT_STORE_HOST_NUMBER;
import static io.pravega.shared.MetricsNames.globalMetricName;
import static io.pravega.shared.MetricsNames.nameFromContainer;
import static io.pravega.shared.MetricsNames.nameFromHost;

/**
 * Class to encapsulate the logic to report Controller metrics for Segment Store hosts and Container lifecycle.
 */
public final class HostContainerMetrics extends AbstractControllerMetrics {

    /**
     * This method reports the number of available Segment Store hosts managing Containers, as well as the number of
     * Containers assigned to each host. Moreover, this method also reports failures for hosts and Containers; we
     * consider a failure the situation in which a host is present in the oldMapping, but not present in newMapping.
     *
     * @param oldMapping    Previous host to Container relationships.
     * @param newMapping    Updated host to Container relationships.
     */
    public void updateHostContainerMetrics(Map<Host, Set<Integer>> oldMapping, Map<Host, Set<Integer>> newMapping) {
        if (newMapping == null) {
            // Nothing to do if the new mapping is null.
            return;
        }

        // Report current number of Segment Store hosts with containers.
        DYNAMIC_LOGGER.reportGaugeValue(SEGMENT_STORE_HOST_NUMBER, newMapping.keySet().size());

        // Report the host/container relationships and the number of containers per host.
        newMapping.keySet().forEach(host -> reportContainerCountPerHost(host, newMapping.get(host)));
        if (oldMapping == null) {
            // Do not perform comparisons against the oldMapping if it is null.
            return;
        }

        // Report Segment Store failures and container failovers. We consider a host failure the event that the old
        // host/container map contains a host not existing in the new map; in this case, the related containers will
        // need to be relocated across the rest of running hosts (Container failover).
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

    private void reportContainerCountPerHost(Host host, Set<Integer> containerIds) {
        DYNAMIC_LOGGER.reportGaugeValue(nameFromHost(SEGMENT_STORE_HOST_CONTAINER_COUNT, host.toString()), containerIds.size());
    }

    private void reportHostFailures(Host failedHost) {
        DYNAMIC_LOGGER.incCounterValue(globalMetricName(SEGMENT_STORE_HOST_FAILURES), 1);
        DYNAMIC_LOGGER.incCounterValue(nameFromHost(SEGMENT_STORE_HOST_FAILURES, failedHost.toString()), 1);
        // Set to 0 the number of containers for the failed host.
        DYNAMIC_LOGGER.reportGaugeValue(nameFromHost(SEGMENT_STORE_HOST_CONTAINER_COUNT, failedHost.toString()), 0);
    }

    private void reportContainerFailovers(Set<Integer> failedContainers) {
        DYNAMIC_LOGGER.incCounterValue(globalMetricName(CONTAINER_FAILOVERS), failedContainers.size());
        for (Integer containerId: failedContainers) {
            DYNAMIC_LOGGER.incCounterValue(nameFromContainer(CONTAINER_FAILOVERS, containerId), 1);
        }
    }
}
