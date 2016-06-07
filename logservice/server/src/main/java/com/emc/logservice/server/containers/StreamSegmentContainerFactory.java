package com.emc.logservice.server.containers;

import com.emc.logservice.server.*;
import com.emc.logservice.storageabstraction.StorageFactory;

/**
 * Represents a SegmentContainerFactory that builds instances of the StreamSegmentContainer class.
 */
public class StreamSegmentContainerFactory implements SegmentContainerFactory {
    private final MetadataRepository metadataRepository;
    private final OperationLogFactory operationLogFactory;
    private final CacheFactory cacheFactory;
    private final StorageFactory storageFactory;

    /**
     * Creates a new instance of the StreamSegmentContainerFactory.
     *
     * @param metadataRepository  The Metadata Repository to use for every container creation.
     * @param operationLogFactory The OperationLogFactory to use for every container creation.
     * @param cacheFactory        The Cache Factory to use for every container creation.
     * @param storageFactory      The Storage Factory to use for every container creation.
     */
    public StreamSegmentContainerFactory(MetadataRepository metadataRepository, OperationLogFactory operationLogFactory, CacheFactory cacheFactory, StorageFactory storageFactory) {
        this.metadataRepository = metadataRepository;
        this.operationLogFactory = operationLogFactory;
        this.cacheFactory = cacheFactory;
        this.storageFactory = storageFactory;
    }

    @Override
    public SegmentContainer createStreamSegmentContainer(String containerId) {
        return new StreamSegmentContainer(containerId, this.metadataRepository, this.operationLogFactory, this.cacheFactory, this.storageFactory);
    }
}
