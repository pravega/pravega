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

package com.emc.logservice.server.containers;

import com.emc.logservice.server.CacheFactory;
import com.emc.logservice.server.MetadataRepository;
import com.emc.logservice.server.OperationLogFactory;
import com.emc.logservice.server.SegmentContainer;
import com.emc.logservice.server.SegmentContainerFactory;
import com.emc.logservice.storageabstraction.StorageFactory;
import com.google.common.base.Preconditions;

import java.util.concurrent.Executor;

/**
 * Represents a SegmentContainerFactory that builds instances of the StreamSegmentContainer class.
 */
public class StreamSegmentContainerFactory implements SegmentContainerFactory {
    private final MetadataRepository metadataRepository;
    private final OperationLogFactory operationLogFactory;
    private final CacheFactory cacheFactory;
    private final StorageFactory storageFactory;
    private final Executor executor;

    /**
     * Creates a new instance of the StreamSegmentContainerFactory.
     *
     * @param metadataRepository  The Metadata Repository to use for every container creation.
     * @param operationLogFactory The OperationLogFactory to use for every container creation.
     * @param cacheFactory        The Cache Factory to use for every container creation.
     * @param storageFactory      The Storage Factory to use for every container creation.
     * @param executor            The Executor to use for running async tasks.
     * @throws NullPointerException If any of the arguments are null.
     */
    public StreamSegmentContainerFactory(MetadataRepository metadataRepository, OperationLogFactory operationLogFactory, CacheFactory cacheFactory, StorageFactory storageFactory, Executor executor) {
        Preconditions.checkNotNull(metadataRepository, "metadataRepository");
        Preconditions.checkNotNull(operationLogFactory, "operationLogFactory");
        Preconditions.checkNotNull(cacheFactory, "cacheFactory");
        Preconditions.checkNotNull(storageFactory, "storageFactory");
        Preconditions.checkNotNull(executor, "executor");

        this.metadataRepository = metadataRepository;
        this.operationLogFactory = operationLogFactory;
        this.cacheFactory = cacheFactory;
        this.storageFactory = storageFactory;
        this.executor = executor;
    }

    @Override
    public SegmentContainer createStreamSegmentContainer(String containerId) {
        return new StreamSegmentContainer(containerId, this.metadataRepository, this.operationLogFactory, this.cacheFactory, this.storageFactory, this.executor);
    }
}
