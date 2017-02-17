/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.service.storage.impl.distributedlog;

/**
 * Generates DistributedLog Log Names from StreamSegmentContainer Ids.
 */
class ContainerToLogNameConverter {
    /**
     * Generates a DistributedLog Log Name from a Stream Segment Container Id.
     *
     * @param containerId The Id of the Container to get the log name for.
     */
    static String getLogName(int containerId) {
        return String.format("container_%d", containerId);
    }
}
