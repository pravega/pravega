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

package com.emc.pravega.service.storage.mocks;

import com.emc.pravega.service.storage.SegmentHandle;

/**
 * A handle that is used by InMemoryStorage.
 */
public class InMemorySegmentHandle implements SegmentHandle {
    private final String segmentName;

    /**
     * Creates a new instance of the InMemorySegmentHandle.
     *
     * @param segmentName The name of the Segment to create the handle for.
     */
    public InMemorySegmentHandle(String segmentName) {
        this.segmentName = segmentName;
    }

    @Override
    public String getSegmentName() {
        return this.segmentName;
    }

    @Override
    public String toString() {
        return this.segmentName;
    }
}
