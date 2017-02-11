/**
 *  Copyright (c) 2016 Dell Inc. or its subsidiaries. All Rights Reserved
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
package com.emc.pravega.service.server.mocks;

import java.util.HashMap;

import com.emc.pravega.service.server.MetadataRepository;
import com.emc.pravega.service.server.UpdateableContainerMetadata;
import com.emc.pravega.service.server.containers.StreamSegmentContainerMetadata;

/**
 * In-Memory mock for Metadata Repository. Contents is lost once object is garbage collected.
 */
public class InMemoryMetadataRepository implements MetadataRepository {
    private final HashMap<Integer, UpdateableContainerMetadata> metadatas = new HashMap<>();

    @Override
    public UpdateableContainerMetadata getMetadata(int streamSegmentContainerId) {
        synchronized (this.metadatas) {
            UpdateableContainerMetadata result = this.metadatas.getOrDefault(streamSegmentContainerId, null);
            if (result == null) {
                result = new StreamSegmentContainerMetadata(streamSegmentContainerId);
                this.metadatas.put(streamSegmentContainerId, result);
            }

            return result;
        }
    }
}
