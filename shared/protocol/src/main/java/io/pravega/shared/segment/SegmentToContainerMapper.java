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
package io.pravega.shared.segment;

import io.pravega.common.Exceptions;
import io.pravega.common.hash.HashHelper;
import io.pravega.shared.NameUtils;
import lombok.extern.slf4j.Slf4j;

/**
 * Defines a Mapper from StreamSegment Name to Container Id.
 */
@Slf4j
public final class SegmentToContainerMapper {
    
    private final HashHelper hasher = HashHelper.seededWith("SegmentToContainerMapper");
    private final int containerCount;
    private final boolean enableAdmin;

    /**
     * Creates a new instance of the SegmentToContainerMapper class.
     *
     * @param containerCount The number of containers that are available.
     * @param enableAdmin    Whether we allow to resolve internal metadata segments (i.e., when Admin Gateway is enabled).
     */
    public SegmentToContainerMapper(int containerCount, boolean enableAdmin) {
        Exceptions.checkArgument(containerCount > 0, "containerCount", "containerCount must be a positive integer.");
        this.containerCount = containerCount;
        this.enableAdmin = enableAdmin;
    }

    /**
     * Gets a value representing the total number of available SegmentContainers available within the cluster.
     *
     * @return Integer indicating the total number of available SegmentContainers available within the cluster.
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
     * @param streamSegmentName The name of the StreamSegment.
     * @return Integer indicating the container id for the given StreamSegment.
     */
    public int getContainerId(String streamSegmentName) {
        // If not in admin mode, return the hash for a Segment as regular. Enabling admin mode gives access to internal
        // metadata segments, but is also more CPU expensive (i.e., String manipulation). As this operation is in the
        // hot path, we suggest to only allow admin mode when necessary for debug/repair purposes.
        if (!enableAdmin) {
            return getSegmentContainerId(streamSegmentName);
        }

        // Only if the admin gateway is enabled, explore if we can locate internal Segments.
        int containerId = Integer.MIN_VALUE;
        if (!isAddressableSegment(streamSegmentName)) {
            containerId = tryGetInternalMetadataSegmentContainerId(streamSegmentName);
        }
        return (containerId >= 0) ? containerId : getSegmentContainerId(streamSegmentName);
    }

    /**
     * This method returns whether the input Segment name can be addressed to the right container via regular hashing
     * scheme. Note that only Container and Storage metadata Segments do not follow the regular hashing scheme.
     *
     * @param streamSegmentName  Name of the Segment to check.
     * @return                   Returns true whether this Segment can be addressed to the right container via the
     *                           regular hashing scheme or not.
     */
    private boolean isAddressableSegment(String streamSegmentName) {
        return !(NameUtils.isMetadataSegmentName(streamSegmentName) || NameUtils.isStorageMetadataSegmentName(streamSegmentName));
    }

    private int getSegmentContainerId(String streamSegmentName) {
        String primaryStreamSegmentName = NameUtils.extractPrimaryStreamSegmentName(streamSegmentName);
        if (primaryStreamSegmentName != null) {
            // This is a Transaction. Map it to the parent's Container.
            return mapStreamSegmentNameToContainerId(primaryStreamSegmentName);
        } else {
            // Standalone StreamSegment.
            return mapStreamSegmentNameToContainerId(streamSegmentName);
        }
    }

    /**
     * Internal Container and Storage metadata Segments should be addressed to the container identified by the number
     * used as suffix in their name (instead of using hashing).
     *
     * @param streamSegmentName  Name of the Segment.
     * @return                   Container id for this internal metadata Segment.
     */
    private int tryGetInternalMetadataSegmentContainerId(String streamSegmentName) {
        try {
            return Integer.parseInt(streamSegmentName.substring(streamSegmentName.lastIndexOf(NameUtils.INTERNAL_NAME_PREFIX) + 1));
        } catch (NumberFormatException e) {
            log.warn("Metadata segment name has a not supported naming ({}), falling back to default assignment.", streamSegmentName);
        }
        return Integer.MIN_VALUE;
    }

    private int mapStreamSegmentNameToContainerId(String streamSegmentName) {
        return hasher.hashToBucket(streamSegmentName, containerCount);
    }
}
