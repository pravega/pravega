package com.emc.logservice.server.mocks;

import com.emc.logservice.server.MetadataRepository;
import com.emc.logservice.server.UpdateableContainerMetadata;
import com.emc.logservice.server.containers.StreamSegmentContainerMetadata;

import java.util.HashMap;

/**
 * In-Memory mock for Metadata Repository. Contents is lost once object is garbage collected.
 */
public class InMemoryMetadataRepository implements MetadataRepository {
    private final HashMap<String, UpdateableContainerMetadata> metadatas = new HashMap<>();

    @Override
    public UpdateableContainerMetadata getMetadata(String streamSegmentContainerId) {
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
