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
package com.emc.pravega.service.server.logs.operations;

/**
 * Defines a mapping between a StreamSegment Name and its Id.
 */
public interface StreamSegmentMapping {
    /**
     * Gets a value indicating the Id of the StreamSegment.
     */
    long getStreamSegmentId();

    /**
     * Gets a value indicating the Name of the StreamSegment.
     */
    String getStreamSegmentName();

    /**
     * Gets a value indicating the Length of the StreamSegment at the time of the mapping.
     */
    long getLength();

    /**
     * Gets a value indicating whether the StreamSegment is currently sealed at the time of the mapping.
     */
    boolean isSealed();
}
