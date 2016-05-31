package com.emc.logservice.storageimplementation.distributedlog;

/**
 * Generates DistributedLog Log Names from StreamSegmentContainer Ids.
 */
class ContainerToLogNameConverter {
    /**
     * Generates a DistributedLog Log Name from a Stream Segment Container Id.
     * @param containerId
     * @return
     */
    public static String getLogName(String containerId) {
        return String.format("container_%s", containerId);
    }
}
