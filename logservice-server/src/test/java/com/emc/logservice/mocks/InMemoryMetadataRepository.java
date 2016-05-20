package com.emc.logservice.mocks;

import com.emc.logservice.MetadataRepository;
import com.emc.logservice.UpdateableContainerMetadata;
import com.emc.logservice.containers.StreamSegmentContainerMetadata;

import java.util.HashMap;

/**
 * Created by padura on 5/20/16.
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
