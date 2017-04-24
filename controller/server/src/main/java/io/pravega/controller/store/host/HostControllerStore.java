/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package io.pravega.controller.store.host;

import io.pravega.common.cluster.Host;

import java.util.Map;
import java.util.Set;

/**
 * Store manager for the host to container mapping.
 */
public interface HostControllerStore {
    /**
     * Get the existing host to container map.
     *
     * @return                      The latest host to container mapping.
     * @throws HostStoreException   On error while fetching the Map.
     */
    Map<Host, Set<Integer>> getHostContainersMap();

    /**
     * Update the existing host to container map with the new one. This operation has to be atomic.
     *
     * @param newMapping            The new host to container mapping which needs to be persisted.
     * @throws HostStoreException   On error while updating the Map.
     */
    void updateHostContainersMap(Map<Host, Set<Integer>> newMapping);
    
    /**
     * Return the total number of segment containers present in the system.
     *
     * @return The total number of segment containers present in the cluster.
     */
    int getContainerCount();

    /**
     * Fetch the Host which owns the specified segment.
     * 
     * @param scope                         The scope of the segment
     * @param stream                        The stream of the segment
     * @param segmentNumber                 The number of the segment
     * @return                              The host which owns the supplied segment.
     * @throws HostStoreException           On error while fetching host info from the ownership Map.
     */
    Host getHostForSegment(String scope, String stream, int segmentNumber);

}
