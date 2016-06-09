package com.emc.logservice.server.mocks;

import com.emc.logservice.server.MetadataRepository;
import com.emc.logservice.server.SegmentContainerManager;
import com.emc.logservice.server.service.ServiceBuilder;
import com.emc.logservice.storageabstraction.DurableDataLogFactory;
import com.emc.logservice.storageabstraction.StorageFactory;
import com.emc.logservice.storageabstraction.mocks.InMemoryDurableDataLogFactory;
import com.emc.logservice.storageabstraction.mocks.InMemoryStorageFactory;

/**
 * ServiceBuilder that uses all in-memory components. Upon closing this object, all data would be lost.
 */
public class InMemoryServiceBuilder extends ServiceBuilder {
    public InMemoryServiceBuilder(int containerCount) {
        super(containerCount);
    }

    @Override
    protected DurableDataLogFactory createDataLogFactory() {
        return new InMemoryDurableDataLogFactory();
    }

    @Override
    protected StorageFactory createStorageFactory() {
        return new InMemoryStorageFactory();
    }

    @Override
    protected MetadataRepository createMetadataRepository() {
        return new InMemoryMetadataRepository();
    }

    @Override
    protected SegmentContainerManager createSegmentContainerManager() {
        return new LocalSegmentContainerManager(getSegmentContainerRegistry(), this.segmentToContainerMapper);
    }
}
