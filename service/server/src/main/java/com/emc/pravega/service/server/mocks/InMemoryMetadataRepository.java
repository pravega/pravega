/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
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
