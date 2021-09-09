/**
 * Copyright Pravega Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
     * @param segmentId                     The number of the segment
     * @return                              The host which owns the supplied segment.
     * @throws HostStoreException           On error while fetching host info from the ownership Map.
     */
    Host getHostForSegment(String scope, String stream, long segmentId);

    /**
     * Fetch the Host which owns the specified segment.
     * 
     * @param tableName                     The table segment name
     * @return                              The host which owns the supplied segment.
     * @throws HostStoreException           On error while fetching host info from the ownership Map.
     */
    Host getHostForTableSegment(String tableName);
}
