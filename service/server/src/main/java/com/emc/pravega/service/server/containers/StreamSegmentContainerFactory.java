/**
 *  Copyright (c) 2017 Dell Inc. or its subsidiaries. All Rights Reserved
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
package com.emc.pravega.service.server.containers;

import com.emc.pravega.service.server.MetadataRepository;
import com.emc.pravega.service.server.OperationLogFactory;
import com.emc.pravega.service.server.ReadIndexFactory;
import com.emc.pravega.service.server.SegmentContainer;
import com.emc.pravega.service.server.SegmentContainerFactory;
import com.emc.pravega.service.server.WriterFactory;
import com.emc.pravega.service.storage.CacheFactory;
import com.emc.pravega.service.storage.StorageFactory;
import com.google.common.base.Preconditions;

import java.util.concurrent.Executor;

/**
 * Represents a SegmentContainerFactory that builds instances of the StreamSegmentContainer class.
 */
public class StreamSegmentContainerFactory implements SegmentContainerFactory {
    private final MetadataRepository metadataRepository;
    private final OperationLogFactory operationLogFactory;
    private final ReadIndexFactory readIndexFactory;
    private final WriterFactory writerFactory;
    private final StorageFactory storageFactory;
    private final CacheFactory cacheFactory;
    private final Executor executor;

    /**
     * Creates a new instance of the StreamSegmentContainerFactory.
     *
     * @param metadataRepository  The Metadata Repository to use for every container creation.
     * @param operationLogFactory The OperationLogFactory to use for every container creation.
     * @param readIndexFactory    The ReadIndexFactory to use for every container creation.
     * @param writerFactory       The Writer Factory to use for every container creation.
     * @param storageFactory      The Storage Factory to use for every container creation.
     * @param cacheFactory        The Cache Factory to use for every container creation.
     * @param executor            The Executor to use for running async tasks.
     * @throws NullPointerException If any of the arguments are null.
     */
    public StreamSegmentContainerFactory(MetadataRepository metadataRepository, OperationLogFactory operationLogFactory, ReadIndexFactory readIndexFactory, WriterFactory writerFactory, StorageFactory storageFactory, CacheFactory cacheFactory, Executor executor) {
        Preconditions.checkNotNull(metadataRepository, "metadataRepository");
        Preconditions.checkNotNull(operationLogFactory, "operationLogFactory");
        Preconditions.checkNotNull(readIndexFactory, "readIndexFactory");
        Preconditions.checkNotNull(writerFactory, "writerFactory");
        Preconditions.checkNotNull(storageFactory, "storageFactory");
        Preconditions.checkNotNull(cacheFactory, "cacheFactory");
        Preconditions.checkNotNull(executor, "executor");

        this.metadataRepository = metadataRepository;
        this.operationLogFactory = operationLogFactory;
        this.readIndexFactory = readIndexFactory;
        this.writerFactory = writerFactory;
        this.storageFactory = storageFactory;
        this.cacheFactory = cacheFactory;
        this.executor = executor;
    }

    @Override
    public SegmentContainer createStreamSegmentContainer(int containerId) {
        return new StreamSegmentContainer(containerId, this.metadataRepository, this.operationLogFactory, this.readIndexFactory, this.writerFactory, this.storageFactory, this.cacheFactory, this.executor);
    }
}
