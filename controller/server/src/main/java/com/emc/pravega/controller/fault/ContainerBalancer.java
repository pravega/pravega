/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.controller.fault;

import java.util.Map;
import java.util.Set;

import com.emc.pravega.shared.common.cluster.Host;

/**
 * Container Balancers are used to fetch the new owners for the segment containers.
 */
public interface ContainerBalancer {
    /**
     * Compute the new owners of the segment containers based on hosts alive in the cluster.
     *
     * @param previousMapping       Existing host to container mapping. If non-empty its assumed to be balanced for the
     *                              older host set.
     * @param currentHosts          The updated list of hosts in the cluster.
     * @return                      The new host to containers mapping after performing a rebalance operation.
     */
    Map<Host, Set<Integer>> rebalance(Map<Host, Set<Integer>> previousMapping, Set<Host> currentHosts);

}
