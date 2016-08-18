/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.emc.pravega.service.server;

import com.emc.pravega.common.Exceptions;

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
        Exceptions.checkArgument(containerCount > 0, "containerCount", "containerCount must be a positive integer.");
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
    public int getContainerId(String streamSegmentName) {
        String parentStreamSegmentName = StreamSegmentNameUtils.getParentStreamSegmentName(streamSegmentName);
        if (parentStreamSegmentName != null) {
            // This is a batch. Map it to the parent's Container.
            return mapStreamSegmentNameToContainerId(parentStreamSegmentName);
        } else {
            // Standalone StreamSegment.
            return mapStreamSegmentNameToContainerId(streamSegmentName);
        }
    }

    /**
     * Gets the container Id based on its numeric value.
     *
     * @param numericContainerId
     * @return
     */
    public String getContainerId(int numericContainerId) {
        assert numericContainerId >= 0 : "numericContainerId must be a non-negative number. Given " + numericContainerId;
        return Integer.toString(numericContainerId);
    }

    private int mapStreamSegmentNameToContainerId(String streamSegmentName) {
        return Math.abs(streamSegmentName.hashCode()) % this.containerCount;
    }
}
