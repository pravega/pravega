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

package com.emc.pravega.service.storage;

import com.emc.pravega.service.contracts.StreamSegmentInformation;
import lombok.Getter;

import java.util.Date;

/**
 * Segment Information as extracted from Storage.
 */
public class StorageSegmentInformation extends StreamSegmentInformation {
    @Getter
    private final SegmentHandle handle;

    /**
     * Creates a new instance of the StreamSegmentInformation class.
     *
     * @param handle       The SegmentHandle associated with the StreamSegment.
     * @param length       The length of the StreamSegment.
     * @param isSealed     Whether the StreamSegment is sealed (for modifications).
     * @param isDeleted    Whether the StreamSegment is deleted (does not exist).
     * @param lastModified The last time the StreamSegment was modified.
     */
    public StorageSegmentInformation(SegmentHandle handle, long length, boolean isSealed, boolean isDeleted, Date lastModified) {
        super(handle.getSegmentName(), length, isSealed, isDeleted, lastModified);
        this.handle = handle;
    }
}
