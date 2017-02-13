/**
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *          http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package com.emc.pravega.common.segment;

import com.emc.pravega.common.Exceptions;
import com.emc.pravega.common.hash.HashHelper;

/**
 * Defines a Mapper from StreamSegment Name to Container Id.
 */
public final class SegmentToContainerMapper {
    
    private final HashHelper hasher = HashHelper.seededWith("SegmentToContainerMapper");
    private final int containerCount;

    /**
     * Creates a new instance of the SegmentToContainerMapper class.
     *
     * @param containerCount The number of containers that are available.
     */
    public SegmentToContainerMapper(int containerCount) {
        Exceptions.checkArgument(containerCount > 0, "containerCount", "containerCount must be a positive integer.");
        this.containerCount = containerCount;
    }

    /**
     * Gets a value representing the total number of available SegmentContainers available within the cluster.
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
     */
    public int getContainerId(String streamSegmentName) {
        String parentStreamSegmentName = StreamSegmentNameUtils.getParentStreamSegmentName(streamSegmentName);
        if (parentStreamSegmentName != null) {
            // This is a Transaction. Map it to the parent's Container.
            return mapStreamSegmentNameToContainerId(parentStreamSegmentName);
        } else {
            // Standalone StreamSegment.
            return mapStreamSegmentNameToContainerId(streamSegmentName);
        }
    }

    private int mapStreamSegmentNameToContainerId(String streamSegmentName) {
        return hasher.hashToBucket(streamSegmentName, containerCount);
    }
}
