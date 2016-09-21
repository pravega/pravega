package com.emc.pravega.service.server.host;

import com.emc.pravega.service.server.store.ServiceBuilderConfig;
import com.emc.pravega.service.storage.DurableDataLogFactory;
import com.emc.pravega.service.storage.HDFSStorageFactory;
import com.emc.pravega.service.storage.StorageFactory;
import com.emc.pravega.service.storage.mocks.InMemoryDurableDataLogFactory;

public class HDFSServicebuilder extends DistributedLogServiceBuilder {
    public HDFSServicebuilder(ServiceBuilderConfig config) {
        super(config);
    }

    @Override
    public StorageFactory createStorageFactory() {
        return new HDFSStorageFactory();
    }


    @Override
    protected DurableDataLogFactory createDataLogFactory() {
        return new InMemoryDurableDataLogFactory();
    }

}
