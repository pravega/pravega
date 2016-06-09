package com.emc.logservice.server;

import com.emc.logservice.common.Exceptions;

import static com.emc.logservice.server.StreamSegmentNameUtils.getParentStreamSegmentName;

/**
 * Defines a Mapper from StreamSegment Name to Container Id.
 */
public final class SegmentToContainerMapper {
    private final int containerCount;

    /**
     * Creates a new instance of the SegmentToContainerMapper class.
     *
     * @param containerCount The number of containers that are available.
     */
    public SegmentToContainerMapper(int containerCount) {
        Exceptions.throwIfIllegalArgument(containerCount > 0, "containerCount", "containerCount must be a positive integer.");
        this.containerCount = containerCount;
    }

    /**
     * Gets a value representing the total number of available SegmentContainers available within the cluster.
     *
     * @return
     */
    public int getTotalContainerCount() {
        return this.containerCount;
    }

    /**
     * Determines the name of the container to use for the given StreamSegment.
     * This value is dependent on the following factors:
     * <ul>
     * <li>The StreamSegment Name itself.
     * <li>The Number of Containers - getTotalContainerCount()
     * <li>The mapping strategy implemented by instances of this interface.
     * </ul>
     *
     * @param streamSegmentName
     * @return
     */
    public String getContainerId(String streamSegmentName) {
        String parentStreamSegmentName = getParentStreamSegmentName(streamSegmentName);
        if (parentStreamSegmentName != null) {
            // This is a batch. Map it to the parent's Container.
            return mapStreamSegmentNameToContainerId(parentStreamSegmentName);
        }
        else {
            // Standalone StreamSegment.
            return mapStreamSegmentNameToContainerId(streamSegmentName);
        }
    }

    /**
     * Gets the container Id based on its numeric valu.
     *
     * @param numericContainerId
     * @return
     */
    public String getContainerId(int numericContainerId) {
        return Integer.toString(numericContainerId);
    }

    private String mapStreamSegmentNameToContainerId(String streamSegmentName) {
        return Integer.toString(Math.abs(streamSegmentName.hashCode()) % this.containerCount);
    }
}
